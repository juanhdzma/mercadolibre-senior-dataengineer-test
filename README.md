# Prueba Técnica – Data Engineer

En este repositorio se implementa un **proceso ETL** ejecutable tanto **localmente en Python** como mediante **contenedorización en Airflow**.  
El objetivo es mostrar una solución de **ETL robusta y estructurada**, con capacidad de validación, trazabilidad y fácil adaptación a entornos productivos.

---

## 🎯 Objetivos del proyecto

El propósito central del proyecto es **tomar tres fuentes de datos distintas** (`pays`, `prints`, `taps`) y realizar un **proceso de transformación** que produzca un dataset final listo para ser utilizado por otros modelos dentro del negocio.  

El dataset debía cumplir con los siguientes requerimientos:  

1. **Obtener los prints de la última semana**.
2. Para cada print se construyen los siguientes campos:  
   - **Indicador de click** → si el usuario hizo o no click en el print.  
   - **Cantidad de vistas** → número de veces que el usuario vio cada *value prop* en las **3 semanas previas** a ese print.  
   - **Cantidad de clicks** → número de veces que el usuario clickeó cada *value prop* en las **3 semanas previas**.  
   - **Cantidad de pagos** → número de pagos realizados por el usuario para cada *value prop* en las **3 semanas previas**.  
   - **Importe acumulado** → total del gasto del usuario en cada *value prop* durante las **3 semanas previas**.  

---

## 📌 Supuestos y reglas de negocio

Dado que en el enunciado del problema no existen definiciones formales sobre ciertos conceptos, fue necesario establecer **supuestos** para poder construir un pipeline consistente.  
En un escenario real, para lograr un resultado más alineado a las expectativas del usuario final, se debería realizar un proceso de **investigación e indagación directa con el área de negocio** con el fin de resolver ambigüedades y aclarar dudas.  

Los supuestos definidos para este proyecto son los siguientes:

- **Definición de “semana”**:  
  Se considera una semana completa (lunes a domingo). Esto elimina la ambigüedad de tomar “últimos 7 días” y asegura que los reportes sean **absolutos y repetibles**.  

- **Ventana temporal de análisis**:  
  No se reciben fechas como parámetro, por lo que los reportes siempre se calculan tomando la **última fecha disponible en los datos** como referencia.  
  - Última semana disponible.  
  - Tres semanas anteriores para consolidar tendencias históricas.  
  Si se quisiera consultar cualquier otro periodo arbitrario, el pipeline debería extenderse para permitir un **parámetro de fecha de corte**, que filtre desde qué punto temporal realizar los cálculos.  
  En la actualidad, para conseguir este comportamiento sería necesario **filtrar previamente las bases de datos antes de cargarlas**, lo cual representa un **sobreproceso** que podría eliminarse si se implementa dicho parámetro en etapas posteriores.  

- **Naturaleza de los datos de `prints`**:  
  Cada registro de `prints` corresponde a una **fecha, usuario y `value_prop`** diferente.  
  Por definición, esto puede dar lugar a **repeticiones en la información**: por ejemplo, si un mismo usuario tiene interacciones con el mismo `value_prop` el lunes y el miércoles, el conteo acumulado de las semanas pasadas será idéntico en ambos registros.  

- **Resultados finales**:  
  - `final.csv`: formato universal, portable a cualquier sistema.  
  - `final.parquet`: optimizado para análisis en data warehouses y lagos de datos.  

👉 Este diseño garantiza que los datos sean claros, consistentes y fácilmente reutilizables por equipos de BI, ML o analítica, evitando reprocesamientos adicionales.

---

## 🏗️ Arquitectura técnica

El proyecto combina varias tecnologías clave:

### 1. Orquestación con **Airflow**
- Airflow permite definir los pipelines como **DAGs** (Directed Acyclic Graphs).  
- Ventajas:  
  - **Escalabilidad**: fácil de extender a múltiples tareas distribuidas.  
  - **Observabilidad**: interfaz gráfica para visualizar dependencias, logs y ejecuciones.  
  - **Flexibilidad**: soporte para ejecución programada (ej. semanal) o bajo demanda.  
- En este proyecto:  
  - Airflow corre en contenedores Docker.  
  - Incluye Scheduler + Webserver.  
  - Inicialización automática con Postgres como metastore.  
  - DAG configurado para correr cada 5 minutos (modo pruebas). En producción, puede cambiarse fácilmente a una frecuencia semanal.

### 2. Almacenamiento con **MinIO (S3-like)**
- MinIO simula un bucket S3 de AWS en local.  
- Ventajas:  
  - Compatible 100% con APIs S3 → migración inmediata a cloud.  
  - Permite validar flujo **end-to-end** de ingestión y exportación.  
  - Facilita pruebas locales sin depender de infraestructura externa.  

### 3. **Polars** como motor de transformación
- Polars se eligió por su rendimiento superior a Pandas en escenarios de ETL.  
- Beneficios:  
  - **Ejecución en paralelo** con motor basado en Apache Arrow.  
  - **API declarativa** que facilita escritura limpia y mantenible.  
  - **Escalabilidad futura** hacia motores distribuidos.  
- Justificación técnica:  
  - Permite procesar datasets más grandes en memoria.  
  - Evita cuellos de botella comunes en Pandas.
  - Es **estricto con el tipado**, lo cual es deseable en proyectos de **data engineering**, ya que reduce errores silenciosos y asegura mayor consistencia en la manipulación de datos.  

### 4. Validación de datos con **Great Expectations**
- Cada dataset es sometido a validaciones tanto de **esquema** como de **valores**.  
- Los reportes en formato **JSON** permiten una **auditoría visual** de cada ejecución, registrando información clave como:  
  - Presencia o ausencia de columnas obligatorias.  
  - Aparición de columnas inesperadas.  
  - Detección de valores fuera de rango o inconsistentes.  
- Posible extensión:  
  - Almacenar los reportes con un **timestamp** para mantener un histórico de calidad de datos.  
  - Dicho histórico facilitaría el análisis de la **evolución de las fuentes** en el tiempo y permitiría anticipar fallas o rupturas en la integración.  

### 5. Backend de metadatos con **PostgreSQL**
- Airflow utiliza Postgres como **base de metadatos**.  
- Permite persistir DAGs, estados de ejecución y logs de scheduler.  
- Facilita auditoría completa del pipeline.

### 6. **Infraestructura contenidizada con Docker Compose**
- Todos los servicios corren en contenedores:  
  - Airflow Webserver.  
  - Airflow Scheduler.  
  - Postgres.  
  - MinIO.  
  - Servicios de inicialización (`init-airflow`, `init-minio`).  
- Ventajas:  
  - Portabilidad → el mismo pipeline corre en cualquier máquina.  
  - Aislamiento → cada servicio es independiente.  
  - Facilidad de despliegue → un solo comando levanta toda la infraestructura.

---

## 📂 Estructura del repositorio

```bash
.
├── .github/workflows/   # CI: tipado, linting, seguridad, tests
├── apps/
│   ├── runner.py        # Ejecución del ETL en local
│   └── dags/            # DAGs de Airflow
├── data/
│   ├── raw/             # Datos crudos de entrada (local)
│   └── out/             # Resultados finales (CSV + Parquet) (local)
├── docs/                # Requerimientos (EN y ES)
├── envis/               # Variables de entorno (local + docker)
├── expectations/        # Reportes de Great Expectations
├── infra/               # Docker Compose + servicios Airflow y MinIO
├── metadata/            # Documentación de esquemas esperados en los datasets
├── notebooks/           # Exploración y prototipado del ETL
├── src/
│   ├── adapters/        # Utilidades (logs, lectores, helpers)
│   ├── application/     # Lógica central del ETL
│   ├── config/          # Configuración de entornos
│   └── domain/          # Definiciones de modelos/esquemas
├── tests/
│   └── unit/            # Unit tests de cada módulo
│       └── data/        # Fixtures y datos de apoyo
├── Makefile             # Comandos automatizados
├── requirements.txt     # Dependencias de producción
└── requirements-dev.txt # Dependencias de desarrollo
```

---

## 📊 Logs y Auditabilidad

### Logging
- Implementación de logs con distintos niveles (`INFO`, `DEBUG`, `ERROR`) utilizando la librería estándar de Python: **`logging`**.  
- Logs centralizados en adaptadores (`src/adapters/logging`).  
- Cada transformación y validación queda registrada en tiempo real.  
- Ventajas:  
  - Permite **auditoría paso a paso** del pipeline.  
  - Facilita el **troubleshooting en producción** con trazas claras.  
  - Aporta **robustez ante fallas**, ya que cada error queda registrado con contexto técnico.  
  - Mejora la **observabilidad** y la **mantenibilidad** del proceso ETL.  

### Auditabilidad
- Great Expectations + logs garantizan trazabilidad total.  
- Cambios en fuentes se detectan automáticamente.  
- Posibilidad de mantener un **histórico de reportes de calidad**, solo cambiando el nombre de salida a formato `YYYY-MM-DD_report.html`.

---

## 🔄 CI/CD y calidad

La política es clara: **si algún paso falla, el pipeline completo falla** y la ejecución queda marcada como **fallida** en GitHub Actions.  
Esto garantiza visibilidad inmediata de errores y evita que se avance con código en mal estado hacia producción. 

### 1. Tipado estático – `mypy`
- Garantiza que el código cumple con anotaciones de tipos estrictas.  
- En data engineering esto es clave: evita errores silenciosos al manejar estructuras de datos complejas (ej. `DataFrame` → `Series`).  
- La verificación de tipos aporta **robustez y mantenibilidad**, minimizando bugs en producción.

### 2. Linting – `flake8`
- Asegura que el código cumple con convenciones **PEP8**.  
- Detecta imports sin uso, variables mal definidas y problemas de estilo que a la larga generan deuda técnica.  
- Contribuye a la **legibilidad** y a la colaboración en equipo.

### 3. Seguridad – `bandit` + `pip-audit`
- `bandit`: analiza patrones de código en busca de vulnerabilidades (ej. uso inseguro de `eval`, problemas de criptografía, hardcodeo de credenciales).  
- `pip-audit`: revisa dependencias contra bases de datos de vulnerabilidades conocidas (CVE).  
  - Gracias a esta verificación se detectó que las librerías usadas por defecto en GitHub Actions incluían una versión vulnerable de **`setuptools`**.  
  - El problema se solucionó actualizando manualmente la librería al inicio del workflow, asegurando que las dependencias del proyecto se instalen siempre en versiones seguras.
- Esta combinación garantiza que el pipeline sea **seguro por diseño**.

### 4. Testing – `pytest` con cobertura
- La cobertura global del proyecto es de **95.49%**, superando ampliamente el umbral mínimo definido en el CI.  
- El pipeline aplica una política estricta e **inmutable**:  
  - **Cobertura <80% → ejecución fallida automáticamente** (no se permite continuar el flujo).  
- La mayoría de los módulos críticos mantienen **100% de cobertura**, lo que respalda la confiabilidad de las transformaciones y reglas de negocio centrales.  
- Se implementaron **53 tests unitarios**, cubriendo tanto escenarios comunes como casos límite y validaciones de errores, lo que garantiza un conjunto robusto de pruebas.  
- La estrategia no busca el 100% absoluto, sino asegurar la **estabilidad y calidad real en componentes clave**.  
- Los tests están diseñados de forma **modular y escalable**, permitiendo que la suite crezca sin fricción a medida que se incorporen nuevas funcionalidades al pipeline.  

---

### Estrategia de branching
- El pipeline de CI actualmente está configurado para ejecutarse en **push directo a `master`**, garantizando que el estado de esa rama siempre sea estable.  
- En un entorno productivo, lo más recomendable es habilitar CI en **Pull Requests hacia `master` o `testing`**, de forma que la validación se ejecute antes de la integración.  
- Esto asegura que **ningún cambio defectuoso llegue a producción**, y que el código se valide siempre en el mismo entorno que será desplegado.  

### Extensión a CD
- La implementación de CD es inmediata gracias a la arquitectura basada en contenedores.  
- Bastaría con:
  1. Asociar el servidor productivo con **runners de GitHub Actions**.  
  2. En el pipeline, agregar un job que haga **build y push** de la imagen Docker generada a un registry.  
  3. El runner en el servidor ejecuta `docker-compose pull && docker-compose up -d` para levantar la nueva versión automáticamente.  

Este diseño asegura un flujo **reproducible y confiable** de código → build → test → despliegue, sin intervención manual.  

---

## 🛠️ Ejecución local (Python)

Para correr el proyecto en un entorno local se requiere:  
1. Descargar el repositorio.  
2. Tener instalado `make` en el sistema.  
3. Crear un entorno virtual de Python (`venv`) e instalar las dependencias de desarrollo listadas en `requirements_dev.txt`.  

Una vez configurado el entorno y activado, los comandos principales son:  

- `make run-local` → ejecuta el ETL completo en local, procesando los datasets y generando las salidas (`csv` y `parquet`).  
- `make all` → corre de forma automática todas las validaciones de calidad: tipado, linting, seguridad y tests con cobertura.  

👉 Esta capa de automatización estandariza los procesos de desarrollo, evita errores manuales y asegura que todos los desarrolladores trabajen con el **mismo flujo de ejecución y validación**.  

📌 Nota: además de los comandos principales, existen otros útiles y más específicos. Se pueden consultar directamente en el `Makefile` para conocer todas las opciones disponibles.  

---

## 🐳 Ejecución con Docker y Airflow

Para correr el proyecto en contenedores:

1. Entrar a la carpeta de infraestructura:

```bash
cd infra/
```

2. Levantar los servicios con: 

```bash
docker-compose up -d --build
```

Una vez ejecutado el comando, es necesario **esperar unos segundos** hasta que los contenedores terminen de inicializarse y configurarse.  

### Acceso a servicios principales

- **Airflow Web UI** → [http://localhost:8080](http://localhost:8080)  
  - Usuario: `admin`  
  - Password: `admin`  
  - Aquí puedes consultar los **logs detallados**, la **estadística de las ejecuciones** y la **visualización de los DAGs**.  

- **MinIO** → [http://localhost:9091](http://localhost:9091)  
  - Usuario: `minio`  
  - Password: `minio123`  
  - Permite inspeccionar directamente los archivos almacenados, funcionando como una **visual del repositorio de datos**.  

### Comportamiento automático
- Al iniciar los contenedores, las bases de datos de prueba se copian automáticamente en MinIO para mayor comodidad en el desarrollo.  
- Airflow lanza un **primer job de ETL automáticamente** apenas se termina de levantar el contenedor.  
- A partir de ahí, el DAG queda configurado para ejecutarse **cada 5 minutos (en múltiplos de 5)** de manera recurrente.  
- Opcionalmente, en lugar de esperar al scheduler, se puede entrar a la interfaz de Airflow y **ejecutar manualmente el DAG** con un clic en “Trigger DAG”.  

---

## 🚀 Escalabilidad y mejoras

El proyecto fue diseñado con visión productiva y contempla múltiples rutas de evolución hacia entornos empresariales:

- **Migración cloud nativa**  
  - Sustituir MinIO por **AWS S3** o **Google Cloud Storage (GCS)** como data lake.  
  - Reemplazar Airflow local por **MWAA (Managed Workflows for Apache Airflow)** en AWS o **Cloud Composer** en GCP.  
  - Integración con servicios de IAM para **gestión granular de accesos** y compliance.  

- **Procesamiento distribuido**  
  - Migrar los procesos de Polars a **PySpark o Dask**, habilitando procesamiento de datasets en el orden de **terabytes**.  
  - Optimización de **particionamiento y paralelismo** para ETL de alto rendimiento.  
  - Uso de **cluster autoscaling** en cloud para balancear costo y performance.  

- **CD automatizado**  
  - Configurar un **self-hosted runner** de GitHub Actions conectado al servidor de despliegue.  
  - Pipeline extendido para **build + push de imágenes Docker** a un registry (ECR/GCR).  
  - CD real: servidor con `docker-compose pull && docker-compose up -d` → despliegue sin intervención manual.  

- **Observabilidad y alertas**  
  - Integrar Airflow y Great Expectations con **Prometheus + Grafana** para métricas y dashboards centralizados.  
  - Configuración de **alertas proactivas en Slack/Email** para:  
    - Fallos en DAGs.  
    - Validaciones de datos que rompan expectativas.  
    - Degradación de performance en jobs críticos.  

- **Gestión de calidad de datos**  
  - Versionado de reportes de Great Expectations en formato **parquet/JSON** dentro de buckets.  
  - Construcción de un **histórico de calidad** consultable para detectar tendencias en la evolución de las fuentes.  
  - Potencial integración con **Data Catalog** (Glue Data Catalog, BigQuery Data Catalog).  

- **Seguridad avanzada (Hardening)**  
  - Sustituir `.env` locales por **secrets managers** (AWS Secrets Manager, GCP Secret Manager, HashiCorp Vault).  
  - Integración con **escaneo continuo de dependencias y contenedores** (Trivy, Grype).  
  - Políticas de **least privilege** en usuarios y roles de orquestadores y buckets.  

- **Próximos pasos de automatización**  
  - Implementación de **data lineage automático** con herramientas como **OpenLineage/Marquez**.  
  - Orquestación híbrida de DAGs → ejecución condicional de workflows según calidad de inputs.  
  - Evaluación de **feature store** para centralizar transformaciones críticas si se avanza hacia ML.  