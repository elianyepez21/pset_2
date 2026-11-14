"""Prueba local (sin Snowflake) de la lógica de la OBT.
Replica con pandas las reglas de Silver/Gold sobre el CSV original, construye la OBT con la misma función que usa
el job real y verifica (1) que las validaciones pasan y (2) que FALLAN si se rompe una dimensión.
Uso:  python test_obt_local.py hotels.csv
"""
import hashlib
import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from build_obt import DIMENSIONES, construir_obt, validar

MESES = {m: i for i, m in enumerate(["January", "February", "March", "April", "May", "June", "July", "August",
                                     "September", "October", "November", "December"], 1)}
sk = lambda *v: hashlib.md5("-".join(str(x) for x in v).encode()).hexdigest()


def silver_pandas(path):
    df = pd.read_csv(path, keep_default_na=False, na_values=["NULL", "NA"])
    n_raw = len(df)
    s = df.drop_duplicates().copy()                                   # regla 1: duplicados exactos
    n_dedup = len(s)
    s = s[(s.adults + s.children.fillna(0) + s.babies) > 0].copy()     # regla 2: sin huéspedes
    s["children"] = s.children.fillna(0)
    s["country"] = s.country.fillna("UNK")
    s["meal"] = s.meal.replace("Undefined", "SC")
    s.loc[(s.adr < 0) | (s.adr > 1000), "adr"] = None                  # regla 4
    s["noches_total"] = s.stays_in_weekend_nights + s.stays_in_week_nights
    s["fecha"] = pd.to_datetime(dict(year=s.arrival_date_year, month=s.arrival_date_month.map(MESES), day=s.arrival_date_day_of_month))
    s["booking_key"] = [sk(*r) for r in s.drop(columns=["fecha", "noches_total"]).itertuples(index=False)]
    return n_raw, n_dedup, s.reset_index(drop=True)


def gold_pandas(s):
    f = pd.DataFrame({
        "BOOKING_KEY": s.booking_key,
        "HOTEL_KEY": s.hotel.map(sk), "FECHA_LLEGADA_KEY": s.fecha.dt.strftime("%Y%m%d").astype(int),
        "CANAL_KEY": [sk(a, b) for a, b in zip(s.market_segment, s.distribution_channel)],
        "PAIS_KEY": s.country.map(sk), "TIPO_CLIENTE_KEY": s.customer_type.map(sk), "DEPOSITO_KEY": s.deposit_type.map(sk),
        "HABITACION_KEY": s.reserved_room_type.map(sk), "PLAN_COMIDA_KEY": s.meal.map(sk),
        "LEAD_TIME": s.lead_time, "NOCHES_TOTAL": s.noches_total, "ADR": s.adr,
        "INGRESO_ESTIMADO": (s.adr * s.noches_total).round(2),
        "CAMBIOS_RESERVA": s.booking_changes, "PLAZAS_PARKING": s.required_car_parking_spaces,
        "SOLICITUDES_ESPECIALES": s.total_of_special_requests, "IS_CANCELED": s.is_canceled})
    d = {}
    d["dim_hotel"] = pd.DataFrame({"HOTEL_KEY": s.hotel.map(sk), "HOTEL_NOMBRE": s.hotel}).drop_duplicates()
    cal = pd.DataFrame({"FECHA": pd.date_range("2015-07-01", "2017-08-31")})
    cal["FECHA_KEY"] = cal.FECHA.dt.strftime("%Y%m%d").astype(int)
    cal["ANIO"] = cal.FECHA.dt.year; cal["TRIMESTRE"] = cal.FECHA.dt.quarter; cal["MES"] = cal.FECHA.dt.month
    cal["DIA"] = cal.FECHA.dt.day; cal["SEMANA_ISO"] = cal.FECHA.dt.isocalendar().week.astype(int)
    cal["DIA_SEMANA"] = cal.FECHA.dt.day_of_week + 1; cal["ES_FIN_DE_SEMANA"] = cal.DIA_SEMANA >= 6
    cal["ES_TEMPORADA_ALTA"] = cal.MES.between(6, 9)
    d["dim_fecha"] = cal
    c = s[["market_segment", "distribution_channel"]].drop_duplicates()
    d["dim_canal"] = pd.DataFrame({"CANAL_KEY": [sk(a, b) for a, b in zip(c.market_segment, c.distribution_channel)],
                                   "SEGMENTO_MERCADO": c.market_segment.values, "CANAL_DISTRIBUCION": c.distribution_channel.values})
    for nombre, col, key, attr in [("dim_pais", "country", "PAIS_KEY", "PAIS_CODIGO"), ("dim_tipo_cliente", "customer_type", "TIPO_CLIENTE_KEY", "TIPO_CLIENTE"),
                                   ("dim_deposito", "deposit_type", "DEPOSITO_KEY", "TIPO_DEPOSITO"),
                                   ("dim_habitacion", "reserved_room_type", "HABITACION_KEY", "TIPO_HABITACION_RESERVADA"),
                                   ("dim_plan_comida", "meal", "PLAN_COMIDA_KEY", "PLAN_COMIDA")]:
        v = s[col].drop_duplicates()
        d[nombre] = pd.DataFrame({key: v.map(sk).values, attr: v.values})
    return f, d


def main(path):
    n_raw, n_dedup, s = silver_pandas(path)
    print(f"RAW={n_raw}  tras quitar duplicados={n_dedup}  SILVER={len(s)}")
    f, d = gold_pandas(s)
    spark = SparkSession.builder.master("local[2]").appName("test-obt").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    fct = spark.createDataFrame(f)
    for c in ("ADR", "INGRESO_ESTIMADO"):                         # en Snowflake las tarifas anuladas son NULL, no NaN
        fct = fct.withColumn(c, F.when(F.isnan(c), None).otherwise(F.col(c)))
    dims = {k: spark.createDataFrame(v) for k, v in d.items()}
    obt = construir_obt(fct, dims)
    res = validar(fct, dims, obt)
    for x in res:
        print(("OK   " if x["ok"] else "FALLA"), x["chequeo"], "| esperado:", x["esperado"], "| obtenido:", x["obtenido"])
    print("columnas OBT:", len(obt.columns), obt.columns)
    assert all(x["ok"] for x in res), "las validaciones deberían pasar con datos correctos"

    # Prueba negativa: una dimensión con llave repetida multiplica filas -> el chequeo debe detectarlo
    roto = dict(dims)
    roto["dim_hotel"] = dims["dim_hotel"].union(dims["dim_hotel"].limit(1))
    res2 = validar(fct, roto, construir_obt(fct, roto))
    fallos = [x["chequeo"] for x in res2 if not x["ok"]]
    print("\nPrueba negativa (llave duplicada en dim_hotel) -> chequeos que fallan:", fallos)
    assert fallos, "la validación debía detectar el join que duplica filas"
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
