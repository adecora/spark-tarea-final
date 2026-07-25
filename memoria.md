# Tarea Spark

Tarea del **Máster en Big Data & Data Engineering 2025-2026**, motor de ingesta de datos de vuelos en formato `JSON`.

## Notas

- El código completo del trabajo se encuentra alojado en el *fork* del repositorio original: [https://github.com/adecora/spark-tarea-final](https://github.com/adecora/spark-tarea-final).
- El proyecto se ha desarrollado en VS Code, la configuración del entorno se encuentra en el directorio `.vscode` en la raíz del repositorio.

  ```text
   .vscode
  ├──  extensions.json  # Extensiones recomendadas
  └──  settings.json    # Opciones de configuración para manejar el entorno de Python
  ```
- Fuentes utilizadas:
  * El material de la asignatura **Spark**.
  * [Doocumentación de Apache Spark (API Reference)](https://spark.apache.org/docs/latest/api/python/reference/index.html).
  * [Documentación de Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/).
  * Modelos de lenguaje (LLMs) consultados:
    * Gemini Pro Latest
    * Gemini Flash Latest


## Actualización de la estructura del proyecto

Se ha modernizado el esqueleto (*scaffolding*) del proyecto para adaptarlo a los estándares actuales de empaquetado en Python:

1. **Migración a `pyproject.toml`:** Sustitución de `setup.py` por `pyproject.toml`.
2. **Reestructuración de directorios:** Se ha adoptado la estructura `src/` para el código fuente:
   - [Guía de empaquetado de Python](https://packaging.python.org/en/latest/tutorials/packaging-projects/#a-simple-project)
   - [Conceptos de módulos en uv](https://docs.astral.sh/uv/concepts/build-backend/#modules)

  ```text
  pyproject.toml
  src
  └── motor_ingesta
      └── __init__.py
  ```

3. **Build Backend:** Configuración de [`uv-build`](https://docs.astral.sh/uv/concepts/build-backend/#using-the-uv-build-backend) como [build backend](https://packaging.python.org/en/latest/tutorials/packaging-projects/#choosing-a-build-backend).
4. **Gestión de Dependencias:**
  - El paquete define como dependencias principales `loguru` y `pandas`. Se ha omitido intencionadamente la declaración de una versión explícita de `pandas` para evitar conflictos, ya que el entorno de ejecución requiere `pyspark`, el cual ya gestiona su propia versión de `pandas`.
  - **Instalación en entorno de Databricks:** Al instalar el paquete directamente en un *notebook* de Databricks, es necesario indicarlo explícitamente para evitar que sobrescriba las dependencias del propio *cluster*:

    ```python
    # Importante usar --no-deps para que no instale dependencias del entorno de Databricks
    !pip install --no-deps --force-reinstall git+https://github.com/adecora/spark-tarea-final.git
    !pip install loguru==0.7.3
    ```

  - Se ha hecho uso de los [dependency-groups](https://packaging.python.org/en/latest/specifications/dependency-groups/#dependency-groups) para separar los distintos entornos de desarrollo:
    * `dev`: Dependencias comunes de desarrollo. Incluye `ipykernel` para permitir la [ejecución de notebooks de Jupyter directamente desde VS Code](https://docs.astral.sh/uv/guides/integration/jupyter/#using-jupyter-from-vs-code).
    * `connect`: Utiliza `databricks-connect` para el desarrollo remoto, conectándose al clúster de Databricks configurado en el archivo local `~/.databrickscfg`.
    * `pyspark`: Dependencias necesarias para ejecutar los tests y/o desarrollar utilizando **PySpark** en modo local.

  **Nota sobre compatibilidad:** Las dependencias de los grupos `connect` y `pyspark` son incompatibles entre sí. Esto se ha indicado explícitamente mediante la directiva `conflicts` en la configuración [^1].

  Para sincronizar los entornos de forma independiente, se utilizan los siguientes comandos:

  ```bash
  # Entorno para Databricks Connect
  $ UV_PROJECT_ENVIRONMENT=.venv-connect uv sync --no-default-groups --group dev --group connect

  # Entorno para PySpark Local
  $ UV_PROJECT_ENVIRONMENT=.venv-pyspark uv sync --no-default-groups --group dev --group pyspark
  ```

1. **Gestión de Recursos Estáticos:** Se ha implementado el uso de la librería estándar `importlib.resources` para acceder de forma segura y empaquetable al archivo de zonas horarias ubicado en [`src/motor_ingesta/resources/timezones.csv`](src/motor_ingesta/resources/timezones.csv) [^2].
2. **Ejecución de Pruebas:**
  Los tests unitarios se ejecutan sobre el entorno local de PySpark utilizando `pytest`:

  ```bash
  $ UV_PROJECT_ENVIRONMENT=.venv-pyspark uv run pytest
  ```
7. **Ejecución como módulo:**
  Es posible ejecutar el pipeline tratando el paquete como un módulo de Python:

  ```bash
   UV_PROJECT_ENVIRONMENT=.venv-pyspark uv run python -m motor_ingesta.flujo_diario <config_file> <data_file>
  ```

[^1]: Declaración de [conflictos de dependencias en `uv`](https://docs.astral.sh/uv/concepts/projects/config/#conflicting-dependencies).
[^2]: [Acceso a archivos de datos en tiempo de ejecución](https://setuptools.pypa.io/en/latest/userguide/datafiles.html#accessing-data-files-at-runtime).


## Opciones de diseño

### 1. Clase `MotorIngesta`

- Se hace uso explicito de la opción `EXECUTION_ENVIRONMENT` definida en fichero de configuración:
  * Si es `"databricks"`, inicializa el entorno con `databricks-connect`.
  * En cualquier otro caso, inicializa el entorno local con `pyspark`.

  **NOTA:** Otra opción que se consideró fue utilizar un bloque `try-except` que intentara usar `databricks-connect` por defecto como mecanismo de *fallback*. [^3]

- **`ingesta_fichero`:** Este método itera sobre las columnas definidas en la opción `data_columns`, aplicando las transformaciones y formatos definidos dinámicamente.


### 2. Función `aniade_hora_utc`

- El cuaderno evaluación [notebooks/actividad_spark.ipynb](./notebooks/actividad_spark.ipynb) evalua que **266** valores de `FlightTime` nulos.
  * **264** casos ocurren porque el dataset original no tiene informado el campo `DepTime` (`df.filter(F.col("DepTime").isNull()).count()`) en este caso no tiene sentido utilizar un valor de relleno ya que la información de la hora de salida no sería real.
  * **2** Se deben  a que `DepTime` trae como valor **2400**, (`df.filter(condition=F.col("DepTime") == 2400).count()`). Este valor genera un *timestamp* fuera del rango de horas válido aceptado por los formatos de Spark: [^4]
    * **H** acepta horas en el rango (0-23).
    * **k** acepta horas en el rango (1-24).

  Aunque esto se podría corregir forzando `DepTime` a **0000** y sumando un día a `FlightDate`, he optado por no alterar el dato. Hacerlo rompería la lógica del paquete, `flujo_diario` recupera la información del día anterior filtrando por `FlightDate` por tanto los registros corregidos se perderian al procesar un fichero del día posterior.


### 3. Función `aniade_intervalos_por_aeropuerto`

- La ventana (*Window*) utilizada particiona los datos por `Origin` y ordena por `F.asc_nulls_last(F.col("FlightTime")`. Con esto se garantiza de forma explícita que los valores nulos de la columna `FlightTime` queden al final de la partición, evitando así que alteren el cálculo de las nuevas columnas.
- La información relativa a los vuelos necesarios para el cálculo se encapsula en una columna temporal llamada `flight_info`, utilizando una estructura de tipo `struct {time, airline}`.
- Para acceder al siguiente registro dentro de la ventana, se utiliza la función `F.lead(col, 1)`. Aunque es funcionalmente equivalente a `F.lag(col, -1)`, `lead` más informativo para este propósito.

### 4. Clase `FlujoDiario`

- En el proceso de recuperación de los datos del día anterior, se utiliza el siguiente bloque:
  ```python
  try:
    # Recuperar datos del día previo que no tengan FlightTime_next, información del vuelo siguiente
    # pero si tenga FlightTime, información de la hora de salida del vuelo.
    flights_previo = self.spark.read.table(output_table).where(
        F.col("FlightDate") == dia_previo
    )

    if flights_previo.isEmpty():
        flag_previo = False
        logger.info(
            f"No se han podido leer datos del día {dia_previo}: no existen datos"
        )
    else:
        flag_previo = True
        logger.info(f"Leída partición del día {dia_previo} con éxito")

  except Exception as e:
      logger.info(
          f"No se han podido leer datos del día {dia_previo}: {str(e)}"
      )
      flag_previo = False
  ```

  **Justificación:** Debido a la evaluación perezosa (*lazy evaluation*) de Spark, es estrictamente necesario invocar una acción (en este caso `isEmpty()`) dentro del bloque `try`. Si no se materializa la acción, Spark no intentará leer la tabla en ese momento, por lo que una posible excepción (como que la tabla no exista) no sería capturada por el `except`, provocando un fallo posterior en el pipeline.

- Si se recuperan datos del día anterior, se unen (`union`) a los actuales omitiendo las columnas `FlightTime_next`, `Airline_next`, y `diff_next`. Estas columnas se recalculan en el paso posterior. Aunque computacionalmente no es la fórmula más óptima, permite completar los campos `_next` de los registros del día anterior que quedaron nulos por ser el último vuelo de su ventana.
- A la hora de guardar la tabla final:
  * Si la tabla no existía en el catálogo, se crea definiendo su clave de partición.
  * Si la tabla ya existía, se sobrescribe utilizando el método `insertInto`. Se evitó el uso de `saveAsTable` porque en determinadas configuraciones este método elimina la metadata de las particiones preexistentes.
- Si el proceso se invoca directamente desde la terminal, se instancia una `SparkSession` de forma explícita habilitando el **soporte para Hive** (`enableHiveSupport()`). Esto garantiza que, al ejecutarse en modo local, la metadata del catálogo se persista en disco (por defecto, la configuración local `spark.sql.catalogImplementation` es `in-memory`).

  <!-- Ver: https://stackoverflow.com/a/77905860/32697703 -->
  ![Ejecución como módulo](https://lh3.googleusercontent.com/d/1N4xt_rzStojh8zMzhEfno6s1O8wxDPiN)

## Tests

Las variables de entorno no se definen de forma explicita en [tests/conftest.py](./tests/conftest.py) se importan del fichero **.env** con `load_dotenv()`.

### Ejecución de test de forma local

[Ejecución de tests](https://drive.google.com/file/d/1SnP9chfu239mTt6F3Wy3NfSMR6tiDhw4/view)

### Flujo de github actions

- Existe un flujo de [github actions](https://github.com/adecora/spark-tarea-final/actions) que lanza los tests con cada **push** al repo y publica los resultados de **coverage** en [codecov](https://app.codecov.io/github/adecora/spark-tarea-final/tree/master?search=&displayType=list)

  ![Coverage publicado en codecov con tests implementados](https://lh3.googleusercontent.com/d/1NRWvUFh1CNHNfqYRlf7fLw19gaXot7DM)


## Ejecución en Databricks

[Ejecución del paquete en Databricks](https://drive.google.com/file/d/10e20RU9jiEXMCrT2lBAdMw2fX9gXhDRr/view)


[^3]: [Recomendación de Azure para escribir código portable con Databricks Connect](https://docs.azure.cn/en-us/databricks/dev-tools/databricks-connect/python/examples#example-use-databrickssesssion-or-sparksession).
[^4]: [Formatos de fecha y hora en spark](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html).


---
[@title]: #
[Source - https://stackoverflow.com/a/35760941]: #
[Posted by Harmon, modified by community. See post 'Timeline' for change history]: #
[Retrieved 2026-02-26, License - CC BY-SA 4.0]: #

<footer style="width:100%; display:flex; justify-content:center; margin:3rem 0;">
    <p align="center">
      <a href="https://alejandrodecora.es/til" style="width:100%; display:flex; justify-content:center; text-decoration: none;">
          Hecho con 💜 por
          <!-- prettier-ignore -->
          <svg viewBox="0 0 600 530" version="1.1" xmlns="http://www.w3.org/2000/svg" style="position: relative; top: 4px; height: 1.25em;">
          <path
            d="m135.72 44.03c66.496 49.921 138.02 151.14 164.28 205.46 26.262-54.316 97.782-155.54 164.28-205.46 47.98-36.021 125.72-63.892 125.72 24.795 0 17.712-10.155 148.79-16.111 170.07-20.703 73.984-96.144 92.854-163.25 81.433 117.3 19.964 147.14 86.092 82.697 152.22-122.39 125.59-175.91-31.511-189.63-71.766-2.514-7.3797-3.6904-10.832-3.7077-7.8964-0.0174-2.9357-1.1937 0.51669-3.7077 7.8964-13.714 40.255-67.233 197.36-189.63 71.766-64.444-66.128-34.605-132.26 82.697-152.22-67.108 11.421-142.55-7.4491-163.25-81.433-5.9562-21.282-16.111-152.36-16.111-170.07 0-88.687 77.742-60.816 125.72-24.795z"
            fill="#1185fe" />
          </svg>
          <code>@vichelocrego</code>
      </a>
    </p>
</footer>
