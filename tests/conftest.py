# import os
# import sys

import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession


@pytest.fixture(scope="session", autouse=True)
def spark():
    # Carga de variables de entorno para ejecutar PySpark en local
    load_dotenv()

    # os.environ["PYSPARK_PYTHON"] = sys.executable
    # os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.appName("Testing PySpark Example")
        .master("local[2]")
        .getOrCreate()
    )

    yield spark

    # Cierra la sesión de Spark al finalizar los tests
    spark.stop()
