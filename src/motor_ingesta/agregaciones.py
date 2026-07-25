from importlib.resources import files

import pandas as pd
from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Función que añade la hora de salida del vuelo en UTC a partir de las columnas FlightDate y DepTime,
    teniendo en cuenta la zona horaria del aeropuerto de salida del vuelo.

    :param spark: Objeto SparkSession
    :param df: DataFrame de vuelos con columnas FlightDate y DepTime
    :param fichero_timezones: Ruta del fichero CSV que contiene los timezones de los aeropuertos
    :return: DataFrame de vuelos con una nueva columna FlightTime de tipo timestamp, que contiene la hora de salida del vuelo en UTC
    """

    # Antes de empezar el ejercicio 2, debemos unir a los vuelos la zona horaria del aeropuerto de salida del vuelo,
    # utilizando el CSV de timezones.csv y uniéndolo por código IATA (columna Origin de los datos con columna iata_code
    # del CSV), dejando a null los timezones de los aeropuertos que no aparezcan en dicho fichero CSV si los hubiera.
    # Primero deberemos leer dicho CSV infiriendo el esquema e indicando que las columnas contienen encabezados.

    path_timezones = files("motor_ingesta.resources").joinpath("timezones.csv")
    timezones_pd = pd.read_csv(str(path_timezones))
    timezones_df = spark.createDataFrame(timezones_pd)

    df_with_tz = df.join(
        timezones_df, df["Origin"] == timezones_df["iata_code"], how="left"
    )

    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 2 (2 puntos)
    # ----------------------------------------

    # Añadir por la derecha una columna llamada FlightTime de tipo timestamp, a partir de las columnas
    # FlightDate y DepTime. Para ello:
    # (a) añade una columna llamada castedHour (que borraremos más adelante) como resultado de convertir la columna
    # DepTime a string, y aplicarle a la columna de string la función F.lpad para obtener una nueva columna en la
    # que se ha añadido el carácter "0" por la izquierda tantas veces como sea necesario. De ese modo nos
    # aseguramos de que tendrá siempre 4 caracteres.
    # (b) añade la columna FlightTime, de la forma "2023-12-25 20:04:00", concatenando lo siguiente (F.concat(...)):
    #    i. la columna resultante de convertir FlightDate a string. Esto nos dará la parte "2023-12-15"
    #    ii. un objeto columna constante, igual a " " (carácter espacio)
    #    iii. la columna resultante de tomar el substring que empieza en la posición 1 y tiene longitud 2. Revisa
    #         la documentación del método substr de la clase Column, y aplica (F.col(...).substr(...))
    #     iv. un objeto columna constante igual a ":"
    #     v. la columna resultante de tomar el substring que empieza en la posición 3 y tiene longitud 2. Los puntos
    #        iii, iv y v nos darán la parte "20:04:00" como string
    #     vi. Por último, aplica la función cast("timestamp") al objeto columna devuelto por concat:
    #         F.concat(...).cast("timestamp"). Los pasos i a v deben hacerse **en una única transformación**
    # (c) Finalmente, en una nueva transformación, reemplaza la columna FlightTime por el resultado de aplicar la
    #     función F.to_utc_timestamp("columna", "time zone") siendo "columna" la columna FlightTime y siendo
    #     "iana_tz" la columna que contiene la zona horaria en base a la cuál debe interpretarse el timestamp
    #     que ya teníamos en FlightTime
    # (d) Antes de devolver el DF resultante, borra las columnas que estaban en timezones_df, así como la columna
    #     castedHour

    # Ojo!!! DepTime tiene 264 valores nulos
    #  > df.filter(F.col("DepTime").isNull()).count()

    # Para evitar problemas al convertir a timestamp, los llenamos con "0000" al crear la columna castedHour

    # Ver:https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    # Para el formato de fecha al usar F.try_to_timestamp(..., formato)
    # - H clock-hour-of-day (0-23)

    # El dataset cuenta con un par de filas con valor DepTime = 2400 que están fuera del rango de horas válido
    #  > df.filter(condition=F.col("DepTime") == 2400).count()

    df_with_flight_time = (
        df_with_tz
        # 1. Generar Timesptamp de la hora de salida del vuelo, usar try_to_timestamp para evitar los casos en que
        # DepTime es Null o 2400, que no se pueden convertir a timestamp. En esos casos, FlightTime será Null
        .withColumn("castedHour", F.lpad(F.col("DepTime").cast("string"), 4, "0"))
        .withColumn(
            "FlightTime",
            F.try_to_timestamp(
                F.concat_ws(" ", F.col("FlightDate"), F.col("castedHour")),
                F.lit("yyyy-MM-dd HHmm"),
            ),
        )
        # 2. Convertir el timestamp a UTC
        .withColumn(
            "FlightTime", F.to_utc_timestamp(F.col("FlightTime"), F.col("iana_tz"))
        )
        # 2. Limpiar columnas auxiliares
        .drop(*timezones_df.columns, F.col("castedHour"))
        # 4. Devolver columnas en el orden original, con FlightTime al final
        .select(*df.columns, F.col("FlightTime"))
    )

    return df_with_flight_time


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Función que añade a cada vuelo la información del siguiente vuelo que despega del mismo aeropuerto de origen,
    así como la diferencia en segundos entre ambos vuelos.

    :param df: DataFrame de vuelos con columna FlightTime de tipo timestamp
    :return: DataFrame con las columnas originales más FlightTime_next, Airline_next y diff_next
    """
    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 3 (2 puntos)
    # ----------------------------------------

    # Queremos pegarle a cada vuelo la información del vuelo que despega justo después de su **mismo
    # aeropuerto de origen**. En concreto queremos saber la hora de despegue del siguiente vuelo y la compañía aérea.
    # Para ello, primero crea una columna de pares (FlightTime, Reporting_Airline), y después crea otra columna
    # adicional utilizando la función F.lag(..., -1) con dicha columna, dentro de una ventana que
    # debe estar particionada adecuadamente y ordenada adecuadamente. No debes utilizar la transformación sort()
    # de los DF. Después, extrae los dos campos internos de la tupla como columnas llamadas "FlightTime_next" y "Airline_next",
    # y calcula una nueva columna diff_next con la diferencia en segundos entre la hora de salida de un vuelo y la
    # del siguiente, como la diferencia de ambas columnas (next menos actual) tras haberlas convertido al tipo "long".
    # El DF resultante de esta función debe ser idéntico al de entrada pero con 3 columnas nuevas añadidas por la
    # derecha, llamadas FlightTime_next, Airline_next y diff_next. Cualquier columna auxiliar debe borrarse.

    w = Window.partitionBy("Origin").orderBy(F.asc_nulls_last(F.col("FlightTime")))
    df_with_next_flight = (
        df
        # 1. Crear la columna de pares como struct {time, airline}
        .withColumn(
            "flight_info",
            F.struct(
                F.col("FlightTime").alias("time"),
                F.col("Reporting_Airline").alias("airline"),
            ),
        )
        # 2. Acceder al siguiente vuelo de la ventana
        .withColumn("next_flight", F.lead("flight_info", 1).over(w))
        # 3. Extraer los campos internos de la tupla como columnas y calcular la diferencia en segundos
        .withColumns(
            {
                "FlightTime_next": F.col("next_flight.time"),
                "Airline_next": F.col("next_flight.airline"),
                "diff_next": (F.col("next_flight.time") - F.col("FlightTime")).cast(
                    "long"
                ),
            }
        )
        # 4. Devolver el DF con las columnas originales y las 3 nuevas columnas añadidas por la derecha
        .select(
            *df.columns,
            "FlightTime_next",
            "Airline_next",
            "diff_next",
        )
    )

    return df_with_next_flight
