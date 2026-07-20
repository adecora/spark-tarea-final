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
  * [Doocumentación de Apache Spark (API Reference)](https://spark.apache.org/docs/4.1.0/api/python/reference/index.html).
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
4. **Gestión de Dependencias:** Se ha hecho uso de los [dependency-groups](https://packaging.python.org/en/latest/specifications/dependency-groups/#dependency-groups) para separar los distintos entornos de desarrollo:
  * `dev`: Dependencias comunes de desarrollo. Incluye `ipykernel` para permitir la [ejecución de notebooks de Jupyter directamente desde VS Code](https://docs.astral.sh/uv/guides/integration/jupyter/#using-jupyter-from-vs-code).
  * `connect`: Utiliza `databricks-connect` para el desarrollo remoto, conectándose al clúster de Databricks configurado en el archivo local `~/.databrickscfg`.
  * `pyspark`: Dependencias necesarias para ejecutar los tests y/o desarrollar utilizando **PySpark** en modo local.

  **Nota sobre compatibilidad:** Las dependencias de los grupos `connect` y `pyspark` son incompatibles entre sí. Esto se ha indicado explícitamente mediante la directiva `conflicts` en la configuración [^1].

  Para sincronizar los entornos de forma independiente, se utilizan los siguientes comandos:

  ```bash
  # Entorno para Databricks Connect
  $ UV_PROJECT_ENVIRONMENT=.venv-connect uv sync --only-group dev --only-group connect

  # Entorno para PySpark Local
  $ UV_PROJECT_ENVIRONMENT=.venv-pyspark uv sync --only-group dev --only-group pyspark
  ```

5. **Gestión de Recursos Estáticos:** Se ha implementado el uso de la librería estándar `importlib.resources` para acceder de forma segura y empaquetable al archivo de zonas horarias ubicado en [`src/motor_ingesta/resources/timezones.csv`](src/motor_ingesta/resources/timezones.csv) [^3].
6. **Ejecución de Pruebas:**
  Los tests unitarios se ejecutan sobre el entorno local de PySpark utilizando `pytest`:

  ```bash
  $ UV_PROJECT_ENVIRONMENT=.venv-pyspark pytest
  ```


[^1]: Declaración de [conflictos de dependencias en `uv`](https://docs.astral.sh/uv/concepts/projects/config/#conflicting-dependencies).
[^2]: [Acceso a archivos de datos en tiempo de ejecución](https://setuptools.pypa.io/en/latest/userguide/datafiles.html#accessing-data-files-at-runtime).