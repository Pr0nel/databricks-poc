# Databricks notebook source
"""
NOTEBOOK 01: Auto Loader Setup
================================
Propósito: Configurar Auto Loader para ingestar datos de S3
Conceptos clave:
- Auto Loader: Ingesta incremental de archivos desde cloud storage
- cloudFiles: Abstracción de Auto Loader para S3
- schemaLocation: Carpeta donde guarda versiones de schema
Resultado: Infraestructura lista para ingestar datos de S3
"""

# COMMAND ----------

# 1. IMPORTS Y CONFIGURACIÓN
print("AUTO LOADER SETUP - Configurando ingesta desde S3")

# COMMAND ----------

# 2. VARIABLES DE CONFIGURACIÓN
S3_BUCKET = "lakehouse-poc-pabloratache"
AWS_REGION = "us-east-2"
S3_BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/"
SCHEMA_LOCATION = f"s3a://{S3_BUCKET}/_schemas/"

print(f"\nConfigurando Auto Loader:")
print(f"  Bucket: {S3_BUCKET}")
print(f"  Region: {AWS_REGION}")
print(f"  Bronze Path: {S3_BRONZE_PATH}")
print(f"  Schema Location: {SCHEMA_LOCATION}")

# COMMAND ----------

# 3. CREAR CATÁLOGO Y ESQUEMAS (Unity Catalog)
print("PASO 1: CREAR UNITY CATALOG STRUCTURE")

# Crear Catalog
spark.sql("""
    CREATE CATALOG IF NOT EXISTS lakehouse_poc
    COMMENT 'Catalog para Lakehouse POC'
""")
print("Catalog 'lakehouse_poc' creado")

# COMMAND ----------

# Crear Schemas
for schema_name in ["bronze", "silver", "gold"]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS lakehouse_poc.{schema_name}
        COMMENT 'Schema {schema_name} - Medallion Architecture'
    """)
    print(f"Schema 'lakehouse_poc.{schema_name}' creado")

# COMMAND ----------

# 4. VERIFICAR ESTRUCTURA
print("PASO 2: VERIFICAR ESTRUCTURA CREADA")

# Listar catálogos
catalogs = spark.sql("SHOW CATALOGS").collect()
print(f"\nCatálogos disponibles:")
for catalog in catalogs:
    print(f"  - {catalog[0]}")

# COMMAND ----------

# Listar schemas en nuestro catalog
schemas = spark.sql("SHOW SCHEMAS IN lakehouse_poc").collect()
print(f"\nSchemas en 'lakehouse_poc':")
for schema in schemas:
    print(f"  - {schema[0]}")

# COMMAND ----------

# 5. CONFIGURAR S3 MOUNT (si es necesario)
print("PASO 3: VERIFICAR ACCESO A S3")
try:
    files = dbutils.fs.ls(S3_BRONZE_PATH)
    print(f"\nAcceso a S3 OK")
    print(f"  Archivos en bronze/: {len(files)}")
    for file in files[:5]:  # Mostrar primeros 5
        print(f"    - {file.name}")
except Exception as e:
    print(f"Error accediendo a S3: {e}")
    print("  Verifica credenciales AWS en Databricks")

# COMMAND ----------

# 6. CONFIGURAR AUTO LOADER PARAMETERS
print("PASO 4: CONFIGURACIÓN AUTO LOADER")
auto_loader_config = {
    "cloudFiles.format": "parquet",                    # Formato de archivos
    "cloudFiles.schemaLocation": SCHEMA_LOCATION,      # Dónde guardar evolución de schema
    "pathGlobFilter": "*.parquet",                     # Solo archivos parquet
    "ignoreLeadingWhiteSpace": "true",
    "ignoreTrailingWhiteSpace": "true",
    "multiLine": "false"
}
print("\nConfiguraciones Auto Loader:")
for key, value in auto_loader_config.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# 7. CREAR TABLA EXTERNA (sin Auto Loader por ahora)
print("PASO 5: CREAR TABLA BRONZE PRELIMINAR")
# Crear tabla externa que apunta a S3
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lakehouse_poc.bronze.events_raw (
        id INT,
        value INT,
        type STRING,
        timestamp LONG,
        ingested_at TIMESTAMP,
        kafka_partition INT,
        kafka_offset LONG,
        ingestion_date DATE
    )
    USING PARQUET
    LOCATION '{S3_BRONZE_PATH}'
    PARTITIONED BY (ingestion_date)
""")
print("Tabla 'events_raw' creada")

# COMMAND ----------

# 8. VERIFICAR TABLA Y DATOS
print("PASO 6: VERIFICAR DATOS EN TABLA")
try:
    df = spark.read.table("lakehouse_poc.bronze.events_raw")
    print(f"\nTabla leída exitosamente")
    print(f"  Schema:")
    df.printSchema()
    print(f"\n  Conteo de filas: {df.count()}")
    print(f"\n  Primeras filas:")
    df.show(5, truncate=False)
except Exception as e:
    print(f"Error leyendo tabla: {e}")

# COMMAND ----------

# 9. CREAR TABLA SILVER (vacía, para próximos pasos)
print("PASO 7: CREAR TABLA SILVER")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse_poc.silver.events_cleaned (
        id INT,
        value INT,
        type STRING,
        timestamp LONG,
        ingested_at TIMESTAMP,
        kafka_partition INT,
        kafka_offset LONG,
        processed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Datos limpios y deduplicados de Bronze'
""")
print("Tabla 'events_cleaned' creada en Silver")

# COMMAND ----------

# 10. CREAR TABLA GOLD (vacía, para próximos pasos)
print("PASO 8: CREAR TABLA GOLD")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse_poc.gold.events_metrics (
        event_type STRING,
        total_events LONG,
        avg_value DOUBLE,
        min_value INT,
        max_value INT,
        calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Métricas y agregaciones para análisis'
""")
print("Tabla 'events_metrics' creada en Gold")

# COMMAND ----------

# 11. REPORTE FINAL
print("REPORTE FINAL - AUTO LOADER SETUP")
tables = spark.sql("""
    SELECT 
        table_name,
        table_schema as schema_name,
        table_type,
        created as created_at
    FROM system.information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    ORDER BY table_schema, table_name
""").collect()
print(f"\nTablas creadas: {len(tables)}")
for table in tables:
    print(f"  {table['schema_name']}.{table['table_name']} ({table['table_type']})")
print("AUTO LOADER SETUP - COMPLETADO")