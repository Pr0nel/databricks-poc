# Lakehouse POC: Hybrid Data Architecture

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark 3.5.0](https://img.shields.io/badge/apache%20spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Delta Lake 3.1.0](https://img.shields.io/badge/delta%20lake-3.1.0-red.svg)](https://delta.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 📋 Tabla de Contenidos

- [Descripción](#descripción)
- [Arquitectura](#arquitectura)
- [Stack Tecnológico](#stack-tecnológico)
- [Requisitos Previos](#requisitos-previos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso](#uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Etapas de Implementación](#etapas-de-implementación)
- [Troubleshooting](#troubleshooting)
- [Métricas y Monitoreo](#métricas-y-monitoreo)
- [Contribuciones](#contribuciones)
- [Recursos Adicionales](#recursos-adicionales)
- [Licencia](#licencia)
- [Autor](#autor)
- [Próximos Pasos](#próximos-pasos)
- [Changelog](#changelog)

---

## 📖 Descripción

**Lakehouse POC** es una prueba de concepto que demuestra una arquitectura **data lakehouse híbrida** local + cloud, combinando:

- **Streaming en tiempo real** con Apache Kafka
- **Delta Lake** para ACID transactions y time travel
- **Arquitectura Medallion** (Bronze -> Silver -> Gold)
- **Cloud Storage** (AWS S3)
- **Databricks Integration** con Unity Catalog y Auto Loader
- **Gobernanza de datos** y esquemas evolucionables

**Propósito:** Demostrar patrones enterprise reales de ingesta, transformación y gobernanza de datos.

---

## 🏗️ Arquitectura

### Diagrama General

```
┌───────────────────────────────────────────────────────────────┐
│                       LAKEHOUSE HÍBRIDO                       │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  CAPA 1: INGESTA (Docker Local - Streaming)                   │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ Kafka (localhost:9092) -> Spark Consumer -> Delta LOCAL │  │
│  │ └─ Formato: JSON                                        │  │
│  │ └─ Frecuencia: Micro-batches (5s)                       │  │
│  └─────────────────────────────────────────────────────────┘  │
│                              |                                │
│                              v                                │
│  CAPA 2: ALMACENAMIENTO (Batch - Persistencia)                │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ Delta LOCAL -> S3 Parquet (particionado por fecha)      │  │
│  │ └─ Formato: Parquet                                     │  │
│  │ └─ Particionado: ingestion_date                         │  │
│  └─────────────────────────────────────────────────────────┘  │
│                              |                                │
│                              v                                │
│  CAPA 3: TRANSFORMACIÓN (Databricks Cloud)                    │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ Auto Loader S3 -> Bronze -> Silver -> Gold              │  │
│  │ └─ Schema Evolution automática                          │  │
│  │ └─ Unity Catalog (gobernanza)                           │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Stack Tecnológico

| Componente | Versión | Propósito |
|-----------|---------|----------|
| **Kafka** | 7.4.0 | Streaming de eventos |
| **Spark** | 3.5.0 | Procesamiento distribuido |
| **Delta Lake** | 3.1.0 | Data lakehouse (ACID, time travel) |
| **Python** | 3.9+ | Orquestación y lógica |
| **Docker** | 24.0+ | Contenedorización |
| **AWS S3** | - | Cloud storage |
| **Databricks** | Serverless | Data platform cloud |

---

## 📦 Requisitos Previos

### Local

- Python 3.9+
- Docker y Docker Compose 24.0+
- Java 11+ (para Spark)
- pip (gestor de paquetes Python)

### AWS

- Cuenta AWS con permisos S3
- IAM user con credenciales (Access Key + Secret Key)

### Databricks

- Workspace Databricks (trial gratuito disponible)
- PAT token (Personal Access Token)

---

## 🚀 Instalación

### 1. Clonar Repositorio

```bash
git clone https://github.com/Pr0nel/databricks-poc.git
cd databricks-poc
```

### 2. Crear Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

### 3. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar Variables de Entorno

```bash
# .env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_S3_BUCKET=your-lakehouse-poc-bucket
KAFKA_BOOTSTRAP_SERVERS=localhost:
KAFKA_TOPIC=test-events
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_TOKEN=your_pat_token
```

---

## ⚙️ Configuración

### Validar Infraestructura

```bash
# Verificar Python y dependencias
python3 config/settings.py

# Verificar logging
python3 config/logging_config.py

# Verificar Docker
scripts/docker-helpers.sh status
```

### Levantar Docker (Kafka + Zookeeper)

```bash
# Desarrollo (sin persistencia)
scripts/docker-helpers.sh dev-up

# Verificar que está listo
scripts/docker-helpers.sh status

# Testear conexión
scripts/docker-helpers.sh test-kafka
```

---

## 💻 Uso

### Ejecución Completa del Pipeline

```bash
# Asegúrate de que Docker está activo
scripts/docker-helpers.sh dev-up

# Ejecutar todo
python3 run_pipeline.py
```

**Resultado esperado:**
```
===================================================================
    LAKEHOUSE POC - PIPELINE COMPLETO
===================================================================

[PASO 1/5] Setup S3 Structure
    Setup S3 structure (crear carpetas bronze/silver/gold) - EXITOSO

[PASO 2/5] Kafka Producer
    Kafka Producer (generar 50 eventos) - EXITOSO
    Producción completada
    Exitosos: 50/50
    Fallidos: 0/50

[PASO 3/5] Spark Streaming Consumer
    Streaming: Kafka -> Delta LOCAL (120 segundos) - EXITOSO
    Filas en Delta: 50

[PASO 4/5] Spark Batch Writer
    Batch: Delta LOCAL -> S3 Parquet - EXITOSO
    Datos escritos a S3 exitosamente

[PASO 5/5] Spark S3 Validator
    Validation: S3 Quality Checks - EXITOSO
    Total de filas: 50
    Data Quality Checks completados

===================================================================
    PIPELINE COMPLETADO EXITOSAMENTE
===================================================================
```

### Ejecución de Componentes Individuales

```bash
# Solo Producer (generar eventos)
python3 scripts/kafka_producer.py

# Solo Consumer (ingestar a Delta)
python3 pyspark-jobs/01_spark_kafka_consumer.py

# Solo Batch (persistir a S3)
python3 pyspark-jobs/02_spark_delta_to_s3.py

# Solo Validación
python3 pyspark-jobs/03_spark_s3_validator.py
```

### Docker Commands

```bash
# Levantar
scripts/docker-helpers.sh dev-up

# Ver estado
scripts/docker-helpers.sh status

# Ver logs de Kafka
scripts/docker-helpers.sh logs-kafka

# Acceder a shell de Kafka
scripts/docker-helpers.sh shell-kafka

# Reset completo (limpia todo)
scripts/docker-helpers.sh reset-dev

# Detener
scripts/docker-helpers.sh stop

# Ver todos los comandos
scripts/docker-helpers.sh help
```

---

## 📁 Estructura del Proyecto

```
databricks-poc/
│
├─ config/                          <- Configuración centralizada
│  ├─ config.yaml                   (vars de entorno)
│  ├─ settings.py                   (parser de config)
│  └─ logging_config.py             (setup de logging)
│
├─ utils/                           <- Módulos reutilizables
│  ├─ __init__.py
│  ├─ data_validators.py            (validaciones comunes)
│  ├─ encoding_utils.py             (MessageEncoder UTF-8)
│  ├─ health_check.py               (health checks)
│  ├─ retry_logic.py                (políticas de retry)
│  ├─ schema_definitions.py         (schemas centralizados)
│  └─ spark_utils.py                (factory SparkSession)
│
├─ scripts/                         <- Herramientas de desarrollo
│  ├─ kafka_producer.py             (genera eventos a Kafka)
│  ├─ setup_s3.py                   (estructura S3)
│  └─ docker-helpers.sh             (CLI Docker)
│
├─ pyspark-jobs/                    <- Jobs de Spark
│  ├─ 01_spark_kafka_consumer.py    (Kafka -> Delta) Streaming
│  ├─ 02_spark_write_delta_s3.py    (Delta -> S3) Batch
│  └─ 03_spark_s3_validator.py      (Validar S3) Quality Checks
│
├─ notebooks/                       <- Notebooks Databricks
│  ├─ 01_auto_loader_setup.py
│  ├─ 02_auto_loader_bronze.py
│  └─ 03_schema_evolution_test.py
│
├─ docker/
│  ├─ docker-compose.yml            (config principal)
│  └─ docker-compose.dev.yml        (override desarrollo)
│
├─ logs/                            <- Logs rotados
│  ├─ kafka_producer.log
│  ├─ orchestrator.log
│  ├─ setup_s3.log
│  ├─ spark_kafka_consumer.log
│  ├─ spark_write_delta_s3.log
│  └─ spark_s3_validator.log
│
├─ spark_checkpoints/               <- Checkpoints de Spark
│  ├─ delta_consumer/
│  └─ s3_consumer/
│
├─ delta_tables/                    <- Delta local (temporal)
│  └─ events_raw/
│
├─ requirements.txt                 <- Dependencias Python
├─ .env                             <- Template de variables
├─ .gitignore                       <- Template de archivos y carpetas no subidos a github
├─ LICENSE                          <- Licencia
├─ README.md                        <- Este archivo
└─ run_pipeline.py                  <- Orquestador principal
```

---

## 📊 Etapas de Implementación

### ETAPA 0: Validación de Infraestructura
Verificar que todos los servicios están disponibles:
- Databricks Serverless
- AWS S3 + IAM
- Docker + Kafka
- Spark local + Delta

### ETAPA 1: Setup Inicial
Preparar código base sin ejecutar:
- Docker Compose
- AWS S3 config
- Databricks setup

### ETAPA 2: Docker Kafka
Levantar Kafka y validar:
- Kafka + Zookeeper cluster
- Producer de eventos
- Creación de Kafka topics
- Health checks

### ETAPA 3: Spark Streaming
Implementar pipeline local:
- Kafka Consumer
- JSON Parsing
- Escritura de Delta (local)
- Escritura parquet en S3
- Validaciones

### ETAPA 4: Databricks Auto Loader
Implementar transformaciones cloud:
- Auto Loader setup
- Schema evolution
- Unity Catalog

### ETAPA 5: Transformaciones Medallion
Implementar capas Silver + Gold:
- Bronze -> Silver (limpieza)
- Silver -> Gold (agregaciones)
- Lineage y gobernanza

### ETAPA 6: Documentación
Notebooks y docs finales

---

## 🐛 Troubleshooting

### Kafka no responde

```bash
# Reset completo de Docker
scripts/docker-helpers.sh reset-dev

# Esperar 20 segundos

# Verificar
scripts/docker-helpers.sh test-kafka
```

### Error de credenciales AWS

```bash
# Verificar que .env está configurado
cat .env

# Validar acceso S3
python3 scripts/setup_s3.py
```

### Spark falla con timeout

Aumentar timeouts en config.yaml
    - spark_consumer: 180 -> 300 (segundos)

---

## 📈 Métricas y Monitoreo

Todos los jobs generan logs detallados en `logs/`:

```bash
# Ver logs en tiempo real
tail -f logs/spark_kafka_consumer.log

# Ver último 50 eventos
tail -50 logs/orchestrator.log

# Buscar errores
grep ERROR logs/*.log
```

---

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Para cambios principales:

1. Fork el proyecto
2. Crea una rama (`git checkout -b feature/mejora`)
3. Commit cambios (`git commit -m 'Agregar mejora'`)
4. Push a la rama (`git push origin feature/mejora`)
5. Open Pull Request

---

## 📚 Recursos Adicionales

- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [AWS S3 Guide](https://docs.aws.amazon.com/s3/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

---

## 📝 Licencia

Este proyecto está bajo la Licencia MIT - ver archivo [LICENSE](LICENSE) para detalles. Sino, en <https://opensource.org/license/mit>.

---

## ✍️ Autor

**[Pablo Ratache Rojas]** - Data Engineer

- GitHub: [@Pr0nel](https://github.com/Pr0nel)
- LinkedIn: [Pablo Ratache](www.linkedin.com/in/pablo-ratache-rojas-9a9602140)
- Portfolio: [Pablo's Portfolio](https://pr0nel.github.io/cv_pablo_ratache/)

---

## 🎯 Próximos Pasos

- [ ] Agregar tests unitarios
- [ ] Documentar transformaciones de negocios
- [ ] Configurar CI/CD con GitHub Actions

---

## Changelog

### v1.0.0

**Última actualización:** Noviembre 2025