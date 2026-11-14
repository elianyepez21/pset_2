"""Construye la One Big Table (OBT) de reservas a partir de las tablas Gold (star schema) y la guarda en Snowflake.

GRANO de la OBT: una fila por reserva única (BOOKING_KEY), igual que GOLD.FCT_RESERVAS.
Antes de escribir se validan los joins: si alguno falla, el job termina con error y NO escribe la OBT.
Credenciales: solo por variables de entorno (ver .env.example).
"""
import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SNOWFLAKE_PACKAGE = "net.snowflake:spark-snowflake_2.12:3.1.1"   # conector para Spark 3.5 / Scala 2.12
FECHA_CORTE_PRUEBA = os.getenv("FECHA_CORTE_PRUEBA", "2017-01-01")  # llegadas desde esta fecha = prueba

# Variables que se conocen DESPUÉS de reservar y pueden filtrar el resultado: no entran a la OBT.
COLUMNAS_POSTERIORES = ["CAMBIOS_RESERVA", "PLAZAS_PARKING", "SOLICITUDES_ESPECIALES"]

# dimensión -> (columna de la tabla de hechos, llave de la dimensión, atributos que se traen)
DIMENSIONES = {
    "dim_hotel":        ("HOTEL_KEY",         "HOTEL_KEY",        ["HOTEL_NOMBRE"]),
    "dim_fecha":        ("FECHA_LLEGADA_KEY", "FECHA_KEY",        ["FECHA", "ANIO", "TRIMESTRE", "MES", "DIA", "SEMANA_ISO",
                                                                  "DIA_SEMANA", "ES_FIN_DE_SEMANA", "ES_TEMPORADA_ALTA"]),
    "dim_canal":        ("CANAL_KEY",         "CANAL_KEY",        ["SEGMENTO_MERCADO", "CANAL_DISTRIBUCION"]),
    "dim_pais":         ("PAIS_KEY",          "PAIS_KEY",         ["PAIS_CODIGO"]),
    "dim_tipo_cliente": ("TIPO_CLIENTE_KEY",  "TIPO_CLIENTE_KEY", ["TIPO_CLIENTE"]),
    "dim_deposito":     ("DEPOSITO_KEY",      "DEPOSITO_KEY",     ["TIPO_DEPOSITO"]),
    "dim_habitacion":   ("HABITACION_KEY",    "HABITACION_KEY",   ["TIPO_HABITACION_RESERVADA"]),
    "dim_plan_comida":  ("PLAN_COMIDA_KEY",   "PLAN_COMIDA_KEY",  ["PLAN_COMIDA"]),
}


def construir_obt(fct: DataFrame, dims: dict, fecha_corte: str = FECHA_CORTE_PRUEBA) -> DataFrame:
    """Une la tabla de hechos con todas sus dimensiones (LEFT JOIN muchos-a-uno) y deja una tabla ancha."""
    obt = fct
    for nombre, (fk, pk, atributos) in DIMENSIONES.items():
        dim = dims[nombre].select(F.col(pk).alias("_pk"), *atributos)
        obt = obt.join(F.broadcast(dim), obt[fk] == dim["_pk"], "left").drop("_pk")
    llaves = [fk for fk, _, _ in DIMENSIONES.values()]
    obt = obt.drop(*llaves, *COLUMNAS_POSTERIORES)
    obt = obt.withColumn("CONJUNTO", F.when(F.col("FECHA") < F.lit(fecha_corte).cast("date"), "train").otherwise("test"))
    # el objetivo queda como última columna
    columnas = [c for c in obt.columns if c != "IS_CANCELED"] + ["IS_CANCELED"]
    return obt.select(*columnas)


def validar(fct: DataFrame, dims: dict, obt: DataFrame) -> list:
    """Chequeos de que los joins no duplicaron ni perdieron observaciones. Devuelve una lista de resultados."""
    r = []

    def check(nombre, esperado, obtenido):
        r.append({"chequeo": nombre, "esperado": str(esperado), "obtenido": str(obtenido), "ok": str(esperado) == str(obtenido)})

    n_fct = fct.count()
    for nombre, (_, pk, _) in DIMENSIONES.items():
        d = dims[nombre]
        check(f"llave única en {nombre}", d.count(), d.select(pk).distinct().count())   # si no, el join multiplicaría filas
    check("filas OBT = filas FCT_RESERVAS", n_fct, obt.count())
    check("BOOKING_KEY distintas = filas FCT", n_fct, obt.select("BOOKING_KEY").distinct().count())
    attrs = [a for _, _, atributos in DIMENSIONES.values() for a in atributos]
    cond = None
    for a in attrs:
        c = F.col(a).isNull()
        cond = c if cond is None else (cond | c)
    check("filas sin match en alguna dimensión", 0, obt.filter(cond).count())
    check("suma de IS_CANCELED conservada", fct.agg(F.sum("IS_CANCELED")).first()[0], obt.agg(F.sum("IS_CANCELED")).first()[0])
    check("suma de INGRESO_ESTIMADO conservada",
          fct.agg(F.round(F.sum("INGRESO_ESTIMADO"), 2)).first()[0], obt.agg(F.round(F.sum("INGRESO_ESTIMADO"), 2)).first()[0])
    check("columnas posteriores a la reserva excluidas", 0, len([c for c in COLUMNAS_POSTERIORES if c in obt.columns]))
    n_train = obt.filter(F.col("CONJUNTO") == "train").count()
    n_test = obt.filter(F.col("CONJUNTO") == "test").count()
    check("train + test = total", n_fct, n_train + n_test)
    return r


def opciones_snowflake() -> dict:
    faltan = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD") if not os.getenv(k)]
    if faltan:
        sys.exit(f"Faltan variables de entorno: {faltan} (ver .env.example)")
    return {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfRole": os.getenv("SNOWFLAKE_ROLE", "PSET2_ROLE"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "WH_PSET2"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "HOTELS_DB"),
        "sfSchema": "GOLD",
    }


def leer(spark, opts, tabla):
    return spark.read.format("snowflake").options(**opts).option("dbtable", f"GOLD.{tabla.upper()}").load()


def escribir(df, opts, tabla, modo):
    (df.write.format("snowflake").options(**{**opts, "sfSchema": "OBT"})
       .option("dbtable", f"OBT.{tabla}").mode(modo).save())


def main():
    opts = opciones_snowflake()
    spark = (SparkSession.builder.appName("pset2-obt").master(os.getenv("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages", SNOWFLAKE_PACKAGE).config("spark.sql.shuffle.partitions", "8").getOrCreate())
    fct = leer(spark, opts, "fct_reservas")
    dims = {n: leer(spark, opts, n) for n in DIMENSIONES}

    obt = construir_obt(fct, dims).cache()
    resultados = validar(fct, dims, obt)
    for x in resultados:
        print(("OK   " if x["ok"] else "FALLA"), x["chequeo"], "| esperado:", x["esperado"], "| obtenido:", x["obtenido"])

    log = spark.createDataFrame([{**x, "ejecutado_en": datetime.utcnow().isoformat(timespec="seconds")} for x in resultados])
    escribir(log, opts, "OBT_VALIDACIONES", "append")           # evidencia de cada corrida

    if not all(x["ok"] for x in resultados):
        sys.exit("Validaciones fallidas: la OBT NO se escribió.")
    escribir(obt, opts, "OBT_RESERVAS", "overwrite")
    print(f"OBT escrita en OBT.OBT_RESERVAS: {obt.count()} filas, {len(obt.columns)} columnas.")
    spark.stop()


if __name__ == "__main__":
    main()
