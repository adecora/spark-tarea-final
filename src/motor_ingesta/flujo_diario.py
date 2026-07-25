import json
from datetime import timedelta
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto
from .motor_ingesta import MotorIngesta


class FlujoDiario:
    def __init__(self, config_file: str):
        """
        Inicializa un flujo diario de ingesta de datos, leyendo la configuración desde un fichero JSON.
        :param config_file: Ruta al fichero JSON que contiene la configuración del flujo diario.
        """
        # Leer como diccionario el fichero json indicado en la ruta config_file, usando json.load(f) del paquete json
        # y almacenarlo en self.config. Además, crear la SparkSession si no existiese usando
        # SparkSession.builder.getOrCreate() que devolverá la sesión existente, o creará una nueva si no existe ninguna
        with Path(config_file).open("r") as f:
            self.config = json.load(f)

        if self.config.get("EXECUTION_ENVIRONMENT", "local") == "databricks":
            from databricks.connect import DatabricksSession

            self.spark: SparkSession = DatabricksSession.builder.profile(
                self.config.get("DATABRICKS_CONFIG_PROFILE")
            ).getOrCreate()
            self.spark.conf.set(
                "spark.app.name", self.config.get("SPARK_APP_NAME", "Motor Ingesta")
            )
        else:
            self.spark = SparkSession.builder.appName(
                self.config.get("SPARK_APP_NAME", "Motor Ingesta")
            ).getOrCreate()

    def procesa_diario(self, data_file: str):
        """
        Procesa un fichero JSON de vuelos del día, aplicando el motor de ingesta y las agregaciones necesarias,
        y guardando el resultado en la tabla indicada en la configuración.

        :param data_file: Ruta al fichero JSON que contiene los datos de vuelos del día a procesar.
        """

        try:
            # Procesamiento diario: crea un nuevo objeto motor de ingesta con self.config, invoca a ingesta_fichero,
            # después a las funciones que añaden columnas adicionales, y finalmente guarda el DF en la tabla indicada en
            # self.config["output_table"], que debe crearse como tabla manejada (gestionada), sin usar ningún path,
            # siempre particionando por FlightDate. Tendrás que usar .write.option("path", ...).saveAsTable(...) para
            # indicar que queremos crear una tabla externa en el momento de guardar.
            # Conviene cachear el DF flights_df así como utilizar el número de particiones indicado en
            # config["output_partitions"]

            motor_ingesta = MotorIngesta(self.config)
            flights_df = motor_ingesta.ingesta_fichero(data_file)

            # Cachear el DF para evitar que se vuelva a leer el fichero JSON en cada transformación
            flights_df.cache()

            # Paso 1. Invocamos al método para añadir la hora de salida UTC
            flights_with_utc = aniade_hora_utc(self.spark, flights_df)

            # -----------------------------
            #  CÓDIGO PARA EL EJERCICIO 4
            # -----------------------------
            # Paso 2. Para resolver el ejercicio 4 que arregla el intervalo faltante entre días,
            # hay que leer de la tabla self.config["output_table"] la partición del día previo si existiera. Podemos
            # obviar este código hasta llegar al ejercicio 4 del notebook
            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            output_table = self.config["output_table"]

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

            if flag_previo:
                # añadir columnas a F.lit(None) haciendo cast al tipo adecuado de cada una, y unirlo con flights_previo.
                # OJO: hacer select(flights_previo.columns) para tenerlas en el mismo orden antes de
                # la unión, ya que la columna de partición se había ido al final al escribir
                df_unido = flights_with_utc.unionByName(
                    flights_previo.select(flights_with_utc.columns),
                    allowMissingColumns=False,
                )
                # Spark no permite escribir en la misma tabla de la que estamos leyendo. Por eso salvamos
                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")

            else:
                df_unido = flights_with_utc  # lo dejamos como está

            # Paso 3. Invocamos al método para añadir información del vuelo siguiente
            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)

            # Paso 4. Escribimos el DF en la tabla externa config["output_table"] con ubicación config["output_path"], con
            # el número de particiones indicado en config["output_partitions"]
            if self.spark.catalog.tableExists(output_table):
                columns = self.spark.table(output_table).columns

                (
                    # Answer: Overwrite only some partitions in a partitioned spark Dataset
                    # https://stackoverflow.com/a/50006527/32697703?stw=2
                    # La tabla ya existe, la sobreescribimos indicando el modo de sobreescritura dinámico
                    # sobreescribe las particiones que se encuentran en el DF y deja intactas las demás particiones de la tabla
                    df_with_next_flight.select(columns)
                    .coalesce(self.config["output_partitions"])
                    .write.mode("overwrite")
                    .option("partitionOverwriteMode", "dynamic")
                    .insertInto(output_table)
                )
                logger.info(f"Tabla {output_table} sobreescrita con éxito.")
            else:
                (
                    df_with_next_flight.coalesce(self.config["output_partitions"])
                    .write.mode("overwrite")
                    # La tabla no existe, la creamos indicando la clave de partición
                    .partitionBy("FlightDate")
                    .saveAsTable(output_table)
                )
                logger.info(f"Tabla {output_table} creada con éxito.")

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            raise e

        finally:
            # Borrar la tabla provisional si la hubiéramos creado
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

            # Liberar la caché del DF
            flights_df.unpersist()


if __name__ == "__main__":
    import sys

    from dotenv import load_dotenv

    # Cargar variables de entorno desde el archivo .env
    load_dotenv()

    # Habilitar el soporte de Hive para persistir localmente los metadatos de las tablas entre sesiones.
    spark = (
        SparkSession.builder.enableHiveSupport().getOrCreate()
    )  # sólo si lo ejecutas localmente

    if len(sys.argv) != 3:
        raise SystemExit("Uso: python flujo_diario.py <config_file> <data_file>")

    flujo = FlujoDiario(sys.argv[1])
    flujo.procesa_diario(sys.argv[2])
