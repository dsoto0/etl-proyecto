# Proyecto ETL en Python

Este repositorio contiene un **pipeline ETL (Extract, Transform, Load)** desarrollado en Python. El objetivo del proyecto es **procesar archivos de Clientes y Tarjetas**, aplicar **validaciones, limpieza y normalización de datos**, registrar errores y finalmente **cargar la información válida en una base de datos**.

El proyecto está pensado con una **arquitectura modular**, separando responsabilidades (lectura, validación, limpieza, carga, logging), lo que facilita el mantenimiento, la escalabilidad y las pruebas.

---

## 🎯 Objetivo del proyecto

* Automatizar la ingesta de datos desde archivos CSV
* Detectar y registrar errores de calidad de datos
* Generar datasets limpios y consistentes
* Cargar la información procesada en una base de datos relacional
* Contar con trazabilidad completa del proceso mediante logs

Este proyecto puede utilizarse como **ejercicio académico**, **prueba técnica** o **pieza de portafolio para perfiles Data Engineer / Data Analyst**.

---

## 🧱 Arquitectura ETL

El pipeline sigue el flujo clásico:

1. **Extract**
   Lectura de archivos CSV desde el directorio `data/raw` mediante descubrimiento automático.

2. **Transform**

   * Validación de reglas de negocio
   * Limpieza de campos
   * Normalización de formatos
   * Separación de registros válidos e inválidos

3. **Load**

   * Escritura de archivos limpios en `data/output`
   * Registro de errores en `errors/`
   * Inserción en base de datos usando SQL

---

## 📂 Estructura del proyecto (detalle completo)

```
etl-proyecto/
├── config.yaml
├── requirements.txt
├── README.md
├── data/
│   ├── raw/
│   └── output/
├── errors/
├── etl/
│   ├── reader.py
│   ├── file_discovery.py
│   ├── validate_clientes.py
│   ├── validate_tarjetas.py
│   ├── clean_clientes.py
│   ├── clean_tarjetas.py
│   ├── db_loader.py
│   ├── errors.py
│   └── logger.py
├── logs/
│   └── etl.log
├── scripts/
│   └── run_pipeline.py
└── sql/
    └── schema.sql
```

---

## 🗂️ Descripción de archivos y módulos

### 🔧 Configuración

#### `config.yaml`

Archivo central de configuración del pipeline. Permite definir:

* Rutas de entrada y salida de datos
* Parámetros de validación
* Configuración de conexión a base de datos

Separar la configuración del código permite modificar el comportamiento del ETL sin tocar la lógica.

---

#### `requirements.txt`

Lista de dependencias necesarias para ejecutar el proyecto. Facilita la instalación del entorno y la reproducibilidad.

---

## 📁 Directorio `data/`

### `data/raw/`

Contiene los **archivos CSV originales** que ingresan al pipeline. Estos archivos no se modifican.

### `data/output/`

Almacena los **archivos procesados y limpios**, listos para análisis o carga en base de datos.

---

## ❌ Directorio `errors/`

Aquí se guardan los registros que **no cumplen las reglas de validación**. Cada archivo contiene:

* Fila original
* Motivo del rechazo

Esto permite auditar errores y mejorar la calidad de los datos de origen.

---

## 🧠 Directorio `etl/` (lógica del pipeline)

### `file_discovery.py`

Se encarga de **detectar automáticamente los archivos de entrada** en `data/raw`, evitando hardcodear nombres de archivos.

---

### `reader.py`

Responsable de la **lectura de archivos CSV** y su conversión a estructuras manejables (por ejemplo, DataFrames).

---

### `validate_clientes.py`

Contiene las **reglas de validación para datos de clientes**, como:

* Campos obligatorios
* Formatos válidos
* Consistencia de datos

---

### `validate_tarjetas.py`

Define las **validaciones específicas para tarjetas**, como:

* Longitud y formato de número de tarjeta
* Fechas válidas
* Asociación con clientes existentes

---

### `clean_clientes.py`

Aplica procesos de **limpieza y normalización** a los datos de clientes:

* Estandarización de texto
* Manejo de valores nulos
* Corrección de formatos

---

### `clean_tarjetas.py`

Limpieza y transformación de los datos de tarjetas para dejarlos listos para persistencia.

---

### `errors.py`

Centraliza la **gestión de errores y registros rechazados**, asegurando consistencia en los archivos generados.

---

### `db_loader.py`

Encargado de la **carga de datos en la base de datos**, utilizando scripts SQL y controlando errores de inserción.

---

### `logger.py`

Configura el **sistema de logging**, permitiendo registrar eventos, advertencias y errores durante la ejecución del ETL.

---

## 📜 Directorio `scripts/`

### `run_pipeline.py`

Script principal del proyecto. Orquesta todo el flujo ETL:

1. Descubrimiento de archivos
2. Lectura
3. Validación
4. Limpieza
5. Escritura de resultados
6. Carga a base de datos
7. Logging del proceso completo

---

## 🗄️ Directorio `sql/`

### `schema.sql`

Define el **esquema de la base de datos**, incluyendo tablas, tipos de datos y relaciones necesarias para almacenar clientes y tarjetas.

---

## ⚙️ Requisitos del sistema

* Python 3.10 o superior
* pip

Instalación de dependencias:

```bash
pip install -r requirements.txt
```

---

## 🚀 Ejecución del pipeline

Desde la raíz del proyecto:

```bash
python scripts/run_pipeline.py
```

Durante la ejecución:

* Se generan logs en tiempo real
* Los errores se almacenan en archivos separados
* Los datos válidos se escriben en `data/output`

---

## 📊 Logs

El archivo `logs/etl.log` contiene:

* Inicio y fin del proceso
* Archivos procesados
* Cantidad de registros válidos e inválidos
* Detalles de errores

---

## 🔮 Posibles mejoras futuras

* Tests automatizados
* Validaciones con esquemas (Great Expectations / Pandera)
* Orquestación con Airflow
* Dockerización del proyecto
* Integración con APIs externas

---

## 📄 Licencia

Proyecto de uso educativo y demostrativo. Puede adaptarse y reutilizarse libremente.

---

✍️ Desarrollado en Python como proyecto ETL modular y escalable.
