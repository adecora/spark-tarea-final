from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class MotorIngesta:
    """
    Clase que representa un motor de ingesta para procesar ficheros de tipo JSON, aplanando las columnas de tipo array y de tipo struct,
    y seleccionando únicamente las columnas indicadas en el fichero de configuración, convirtiéndolas al tipo indicado en el mismo.
    """

    def __init__(self, config: dict):
        """
        Inicializa un motor de ingesta para procesar ficheros de tipo JSON.
        :param config: Diccionario que contiene los parámetros de configuración.
        """
        self.config = config

        # Ver: https://docs.azure.cn/en-us/databricks/dev-tools/databricks-connect/python/examples#example-use-databrickssesssion-or-sparksession
        try:
            from databricks.connect import DatabricksSession

            self.spark = DatabricksSession.builder.profile(
                config.get("DATABRICKS_CONFIG_PROFILE")
            ).getOrCreate()
            self.spark.conf.set(
                "spark.app.name", config.get("SPARK_APP_NAME", "Motor Ingesta")
            )
        except ImportError:
            self.spark = SparkSession.builder.appName(
                config.get("SPARK_APP_NAME", "Motor Ingesta")
            ).getOrCreate()

    def ingesta_fichero(self, json_path: str) -> DF:
        """
        Lee un fichero JSON, lo aplana y selecciona únicamente las columnas indicadas en el fichero de configuración, convirtiéndolas al tipo indicado en el mismo.
        :param json_path: Ruta al fichero JSON que se desea procesar. Debe estar en DBFS si ejecutamos
                          desde un notebook de Databricks, o en una ruta de nuestro portátil si usamos dbconnect
        :return: DataFrame de Spark resultante tras aplanar y seleccionar las columnas indicadas.
        """
        # Leemos el JSON como DF, tratando de inferir el esquema, y luego lo aplanamos.
        # Por último nos quedamos con las columnas indicadas en el fichero de configuración,
        # en la propiedad self.config["data_columns"], que es una lista de diccionarios. Debemos recorrer
        # esa lista, seleccionando la columna y convirtiendo cada columna al tipo indicado en el fichero.

        # PISTA: crear en lista_obj_column una lista de objetos Column como lista por comprensión a partir
        # de self.config["data_columns"], y luego usar dicha lista como argumento de select(...). El DF resultante
        # debe ser devuelto como resultado de la función.

        # Para incluir también el campo "comment" como metadatos de la columna, podemos hacer:
        # F.col(...).cast(...).alias(..., metadata={"comment": ...})

        flights_day_df = self.spark.read.json(json_path)

        aplanado_df = MotorIngesta.aplana_df(flights_day_df)

        lista_obj_column = []
        for columna in self.config["data_columns"]:
            name = columna.get("name")
            type = columna.get("type")
            comment = columna.get("comment", "")

            if type == "date":
                lista_obj_column.append(
                    F.to_date(F.col(name), columna.get("format", None)).alias(
                        name, metadata={"comment": comment}
                    )
                )
            else:
                lista_obj_column.append(
                    F.col(name).cast(type).alias(name, metadata={"comment": comment})
                )

        resultado_df = aplanado_df.select(*lista_obj_column)
        return resultado_df

    @staticmethod
    def aplana_df(df: DF) -> DF:
        """
        Aplana un DataFrame de Spark que tenga columnas de tipo array y de tipo estructura.

        :param df: DataFrame de Spark que contiene columnas de tipo array o columnas de tipo estructura, incluyendo
                   cualquier nivel de anidamiento y también arrays de estructuras. Asumimos que los nombres de los
                   campos anidados son todos distintos entre sí, y no van a coincidir cuando sean aplanados.
        :return: DataFrame de Spark donde todas las columnas de tipo array han sido explotadas y las estructuras
                 han sido aplanadas recursivamente.
        """
        to_select = []
        schema = df.schema.jsonValue()
        fields = schema["fields"]
        recurse = False

        for f in fields:
            if not isinstance(f["type"], dict):
                to_select.append(f["name"])
            else:
                if f["type"]["type"] == "array":
                    to_select.append(F.explode(f["name"]).alias(f["name"]))
                    recurse = True
                elif f["type"]["type"] == "struct":
                    # OJO!!! Asumimos que los nombres de los campos anidados son todos
                    # distintos entre sí, y no van a coincidir cuando sean aplanados.
                    to_select.append(f"{f['name']}.*")
                    recurse = True

        new_df = df.select(*to_select)
        return MotorIngesta.aplana_df(new_df) if recurse else new_df
