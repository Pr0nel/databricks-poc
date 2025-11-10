# Data Engineering Pipeline con Databricks

Este proyecto demuestra un pipeline de ingesta, transformación y análisis de datos utilizando **Databricks**, **PySpark** y herramientas modernas de ingeniería de datos. Implementa la **arquitectura Medallion** (Bronze → Silver → Gold) en **Databricks**.

## 📋 Contenido

- **Ingesta de Datos**: Carga datos crudos desde archivos locales.
- **Transformaciones**: Transforma datos crudos en datos curados.
- **Análisis**: Genera vistas analíticas para insights empresariales.
- **Orquestación**: Automatización completa mediante jobs en Databricks.

## 🛠️ Tecnologías Utilizadas

- **Databricks**: Plataforma de procesamiento distribuido.
- **PySpark**: Framework para procesamiento de datos a gran escala.
- **Python**: Lenguaje principal para el desarrollo del pipeline.
- **API REST de Databricks**: Para la creación y ejecución de jobs.
- **GitHub**: Control de versiones y documentación.

## 🚀 Instalación

1. Clona este repositorio:
    ```bash
    git clone https://github.com/Pr0nel/databricks-poc.git
    cd databricks-poc
    ```

2. Crea un archivo .env con las siguientes variables:
    ```
    ENV=development
    DATABRICKS_HOST=<tu-host-de-databricks>
    DATABRICKS_TOKEN=<tu-token-de-databricks>
    SPARK_VERSION=14.3.x-scala2.12
    NODE_TYPE_ID=Standard_DS3_v2
    NUM_WORKERS=2
    ```

3. Instalar dependencias:
    ```
    pip install -r requirements.txt
    ```

4. Ejecutar instalación:
    ```
    python scripts/main.py
    ```

## 📂 Estructura del Proyecto

scripts/: Contiene los scripts principales del pipeline.
notebooks/: Notebooks de Databricks para cada etapa del pipeline.
data/: Datos iniciales utilizados en el proyecto.

## 📊 Resultados

Datos RAW: Almacenados en la capa RAW.
Datos CURATED: Transformados y almacenados en la capa CURATED.
Vistas BUSINESS: Disponibles para análisis empresarial.

## 📈 Arquitectura

La arquitectura sigue el patrón Medallion (Bronze → Silver → Gold):

Bronze (RAW): Datos crudos sin procesar.
Silver (CURATED): Datos limpios y transformados.
Gold (BUSINESS): Vistas analíticas optimizadas para consultas empresariales.

## 📄 Licencia

Este proyecto está bajo la licencia MIT. Ver el archivo LICENSE para más detalles. Sino, en <https://opensource.org/license/mit>.