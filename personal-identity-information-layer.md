# Information Layer — Personal Identity (Data Vault 2.0)

Definición de las tablas de la **Information Layer** del padrón de identidades personales,
materializadas como **tablas Iceberg** en la capa Consume del DataLakehouse.

## Propósito

Estas tablas sirven a la aplicación para **consultar la existencia de números de documento
en el padrón** (lookup de existencia). Solo se conserva el registro vigente/reciente de cada
identidad; el historial completo reside en el Raw Vault (satellites).

## Arquitectura de referencia

```
Raw Vault (Hubs + Satellites)
    → Business Vault (reglas de negocio)
        → Information Layer (Iceberg en Consume)   ← ESTE DOCUMENTO
            → Publicación opcional → Aurora PostgreSQL (microservicios)
```

## Ubicación en S3

```
s3://{consume-bucket}/datalake/personal_identity/
├── dim_natural_person/     ← Personas naturales (RENIEC)
├── dim_legal_entity/       ← Personas jurídicas (SUNAT)
└── dim_other_identity/     ← Otros tipos de documento (AFMCPT10)
```

---

## 1. DDL — Definición de tablas Iceberg

### 1.1 dim_natural_person

```sql
-- ============================================================================
-- Tabla 1: Personas Naturales (fuente: RENIEC)
-- Propósito: Lookup de existencia de DNI en el padrón
-- ============================================================================
CREATE TABLE consume_db.dim_natural_person (
    identity_hash_key     STRING        COMMENT 'SHA-256 de D#dni — clave única del Hub',
    dni                   STRING        COMMENT 'DNI 8 dígitos',
    full_name             STRING        COMMENT 'PATERNO MATERNO NOMBRES',
    paternal_last_name    STRING        COMMENT 'Apellido paterno (SN si vacío)',
    maternal_last_name    STRING        COMMENT 'Apellido materno (SN si vacío)',
    status                STRING        COMMENT 'ACTIVE | INACTIVE',
    birth_year            INT           COMMENT 'Año nacimiento (regulatorio: solo YYYY)',
    load_timestamp        TIMESTAMP     COMMENT 'Fecha/hora de última actualización del registro'
)
USING iceberg
PARTITIONED BY (bucket(16, identity_hash_key))
LOCATION 's3://{consume-bucket}/datalake/personal_identity/dim_natural_person/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);
```

### 1.2 dim_legal_entity

```sql
-- ============================================================================
-- Tabla 2: Personas Jurídicas (fuente: SUNAT 10-60 consolidado)
-- Propósito: Lookup de existencia de RUC en el padrón
-- ============================================================================
CREATE TABLE consume_db.dim_legal_entity (
    identity_hash_key     STRING        COMMENT 'SHA-256 de R#ruc — clave única del Hub',
    ruc                   STRING        COMMENT 'RUC 11 dígitos',
    business_name         STRING        COMMENT 'Razón social (fallback: SIN RAZON SOCIAL + RUC)',
    status                STRING        COMMENT 'ACTIVE | INACTIVE',
    load_timestamp        TIMESTAMP     COMMENT 'Fecha/hora de última actualización del registro'
)
USING iceberg
PARTITIONED BY (bucket(16, identity_hash_key))
LOCATION 's3://{consume-bucket}/datalake/personal_identity/dim_legal_entity/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);
```

### 1.3 dim_other_identity

```sql
-- ============================================================================
-- Tabla 3: Otros tipos de identidad (fuente: AFMCPT10)
-- Propósito: Lookup de existencia de otros documentos (CE, PAS, etc.)
-- ============================================================================
CREATE TABLE consume_db.dim_other_identity (
    identity_hash_key     STRING        COMMENT 'SHA-256 de doc_type#doc_number — clave única del Hub',
    doc_type_code         STRING        COMMENT 'Código tipo documento (CE, PAS, CTI, etc.)',
    doc_type_description  STRING        COMMENT 'Descripción legible del tipo de documento',
    doc_number            STRING        COMMENT 'Número de documento',
    full_name             STRING        COMMENT 'Nombre o razón social',
    status                STRING        COMMENT 'ACTIVE',
    load_timestamp        TIMESTAMP     COMMENT 'Fecha/hora de última actualización del registro'
)
USING iceberg
PARTITIONED BY (doc_type_code, bucket(16, identity_hash_key))
LOCATION 's3://{consume-bucket}/datalake/personal_identity/dim_other_identity/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);
```

---

## 2. Decisiones de diseño

### 2.1 Alcance de columnas

Se descartaron los campos de trazabilidad y temporalidad (`source_system`, `source_table`,
`source_priority`, `effective_from`, `effective_to`, `is_current`) porque:

- El caso de uso es **lookup de existencia**, solo requiere el registro vigente.
- El historial ya está en el Raw Vault (satellites).
- Iceberg ofrece **time-travel** nativo, cubriendo consultas históricas sin SCD-2 en la tabla.
- Cada tabla tiene una única fuente fija, por lo que `source_system` sería redundante.

Se conserva `load_timestamp` por ser barato y útil para operación/debugging.

### 2.2 merge-on-read

Configurado por patrón de upsert frecuente con deltas pequeños: los updates/deletes se
escriben como delete-files livianos en lugar de reescribir data-files completos. Requiere
compactación periódica (ver sección 5). Si las lecturas fueran muy sensibles a latencia y
las cargas poco frecuentes, considerar `copy-on-write`.

### 2.3 Particionamiento

- `dim_natural_person`, `dim_legal_entity`: bucketing por `identity_hash_key` (16 buckets)
  para distribución uniforme y lookup puntual eficiente.
- `dim_other_identity`: además particiona por `doc_type_code` (pocos tipos distintos, filtro común).

---

## 3. Reglas de transformación (Business Vault → Information Layer)

| #  | Regla                     | Tabla          | Detalle |
|----|---------------------------|----------------|---------|
| 1  | Validación DNI            | natural        | Solo `dni` matching `^[0-9]{8}$` |
| 2  | Validación RUC            | legal          | Solo `ruc` matching `^[0-9]{11}$` |
| 3  | Exclusión cruzada         | other          | Excluir `doc_type IN ('D','R')` |
| 4  | Normalización estado      | natural, legal | ACTIVE/INACTIVE canónico; default ACTIVE |
| 5  | Fecha nacimiento → año    | natural        | `YEAR(birth_date)` como INT (regulatorio) |
| 6  | Apellido fallback         | natural        | NULL → `'SN'` |
| 7  | Razón social fallback     | legal          | NULL/vacío → `'SIN RAZON SOCIAL ' \|\| ruc` |
| 8  | Prioridad SUNAT           | legal          | `ROW_NUMBER() PARTITION BY ruc ORDER BY priority` = 1 (SUNAT10=1 ... SUNAT60=6) |
| 9  | Hash key                  | todas          | `SHA-256(UPPER(TRIM(doc_type)) \|\| '#' \|\| TRIM(doc_number))` |
| 10 | Enriquecimiento tipo doc  | other          | Lookup catálogo: CE→Carné Extranjería, PAS→Pasaporte, etc. |
| 11 | Deduplicación             | todas          | Un registro por `identity_hash_key` (el más reciente) |

### Normalización de estado (detalle)

- **ACTIVE**: `{A, ACTIVO, ACTIVE, 1, VIGENTE, 00, 01, HABIDO, SI, S}`
- **INACTIVE**: `{I, INACTIVO, INACTIVE, 0, BAJA, 02, 03, 04, 05, NOHABIDO, NO HABIDO, SUSPENDIDO, B, N}`
- **Default**: `ACTIVE` si el valor no se reconoce.

---

## 4. Estrategia de alimentación (MERGE INTO / Upsert)

Patrón de carga: **upsert** con `MERGE INTO`, usando `identity_hash_key` como match key.
Solo se actualiza cuando algún campo de negocio cambió (evita writes innecesarios).

### 4.1 dim_natural_person

```sql
MERGE INTO consume_db.dim_natural_person AS t
USING (
    SELECT
        identity_hash_key, dni, full_name,
        paternal_last_name, maternal_last_name,
        status, birth_year,
        current_timestamp() AS load_timestamp
    FROM business_vault.v_natural_person_current
) AS s
ON t.identity_hash_key = s.identity_hash_key
WHEN MATCHED AND (
       t.full_name          <> s.full_name
    OR t.status             <> s.status
    OR t.paternal_last_name <> s.paternal_last_name
    OR t.maternal_last_name <> s.maternal_last_name
    OR t.birth_year         <> s.birth_year
) THEN UPDATE SET
    full_name          = s.full_name,
    paternal_last_name = s.paternal_last_name,
    maternal_last_name = s.maternal_last_name,
    status             = s.status,
    birth_year         = s.birth_year,
    load_timestamp     = s.load_timestamp
WHEN NOT MATCHED THEN INSERT *;
```

### 4.2 dim_legal_entity

```sql
MERGE INTO consume_db.dim_legal_entity AS t
USING (
    SELECT
        identity_hash_key, ruc, business_name, status,
        current_timestamp() AS load_timestamp
    FROM business_vault.v_legal_entity_current
) AS s
ON t.identity_hash_key = s.identity_hash_key
WHEN MATCHED AND (
       t.business_name <> s.business_name
    OR t.status        <> s.status
) THEN UPDATE SET
    business_name  = s.business_name,
    status         = s.status,
    load_timestamp = s.load_timestamp
WHEN NOT MATCHED THEN INSERT *;
```

### 4.3 dim_other_identity

```sql
MERGE INTO consume_db.dim_other_identity AS t
USING (
    SELECT
        identity_hash_key, doc_type_code, doc_type_description,
        doc_number, full_name, status,
        current_timestamp() AS load_timestamp
    FROM business_vault.v_other_identity_current
) AS s
ON t.identity_hash_key = s.identity_hash_key
WHEN MATCHED AND (
       t.full_name            <> s.full_name
    OR t.status               <> s.status
    OR t.doc_type_description  <> s.doc_type_description
) THEN UPDATE SET
    doc_type_description = s.doc_type_description,
    full_name            = s.full_name,
    status               = s.status,
    load_timestamp       = s.load_timestamp
WHEN NOT MATCHED THEN INSERT *;
```

### 4.4 Manejo de deletes

Como el padrón responde "¿existe este documento?", se recomienda **soft-delete**: si un
documento sale de la fuente, actualizar `status = 'INACTIVE'` en lugar de eliminarlo. Así la
aplicación distingue "nunca existió" de "existió pero fue dado de baja".

Si se requiere hard-delete, agregar `WHEN NOT MATCHED BY SOURCE THEN DELETE`, lo que exige
que la fuente contenga el conjunto completo de identidades válidas (full load desde el vault).

---

## 5. Mantenimiento Iceberg

Programar periódicamente (post-carga o en ventana nocturna) por cada tabla:

```sql
-- Compactar data-files y delete-files (necesario con merge-on-read)
CALL consume_db.system.rewrite_data_files('dim_natural_person');

-- Expirar snapshots antiguos (evita crecimiento indefinido de storage)
CALL consume_db.system.expire_snapshots('dim_natural_person', TIMESTAMP '...');

-- Limpiar archivos huérfanos
CALL consume_db.system.remove_orphan_files(table => 'dim_natural_person');
```

---

## 6. Resumen

| Aspecto            | Decisión |
|--------------------|----------|
| Tablas             | 3 (natural, legal, other) |
| Formato            | Iceberg v2, Parquet + zstd |
| Registro           | Solo vigente (sin SCD-2 en tabla) |
| Carga              | `MERGE INTO` (upsert) por `identity_hash_key` |
| Update condition   | Solo si cambió algún campo de negocio |
| Deletes            | Soft-delete (`status = 'INACTIVE'`) |
| Modo escritura     | merge-on-read + compactación periódica |
| Historial          | En Raw Vault + time-travel Iceberg |
