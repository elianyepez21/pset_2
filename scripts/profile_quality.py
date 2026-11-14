"""Perfilado de calidad de datos sobre el CSV original (completitud, precisión, consistencia, validez).
Reproduce las métricas citadas en docs/calidad_datos.md.   Uso: python scripts/profile_quality.py hotels.csv
"""
import json
import sys

import pandas as pd

path = sys.argv[1]
df = pd.read_csv(path, keep_default_na=False, na_values=["NULL", "NA"])
n = len(df)
pct = lambda x: round(100 * x / n, 2)
m = {"filas_raw": n}

# Completitud
nulos = df.isna().sum()
m["completitud_nulos"] = {c: {"n": int(v), "pct": pct(v)} for c, v in nulos[nulos > 0].items()}

# Consistencia: duplicados exactos (la fuente no trae ID)
dup = df.duplicated(keep="first")
u = df[~dup]
m["duplicados_exactos"] = {"filas_extra": int(dup.sum()), "pct": pct(dup.sum()), "filas_unicas": len(u),
                           "tasa_cancelacion_antes": round(df.is_canceled.mean(), 4), "tasa_cancelacion_despues": round(u.is_canceled.mean(), 4)}
nr = df.deposit_type == "Non Refund"
m["non_refund"] = {"filas_raw": int(nr.sum()), "tasa_cancelacion_raw": round(df[nr].is_canceled.mean(), 4),
                   "filas_unicas": int((u.deposit_type == "Non Refund").sum()), "tasa_cancelacion_unicas": round(u[u.deposit_type == "Non Refund"].is_canceled.mean(), 4)}
sin_huesped = (df.adults + df.children.fillna(0) + df.babies) == 0
m["sin_huespedes"] = {"raw": int(sin_huesped.sum()), "tras_dedup": int(((u.adults + u.children.fillna(0) + u.babies) == 0).sum())}
m["silver_esperado"] = int((((u.adults + u.children.fillna(0) + u.babies) > 0)).sum())
m["meal_undefined"] = int((df.meal == "Undefined").sum())
m["sin_noches"] = int(((df.stays_in_weekend_nights + df.stays_in_week_nights) == 0).sum())
m["cliente_repetido_sin_historial"] = int(((df.is_repeated_guest == 1) & (df.previous_bookings_not_canceled + df.previous_cancellations == 0)).sum())
m["reservation_status_vs_target"] = pd.crosstab(df.is_canceled, df.reservation_status).to_dict()

# Precisión
m["adr"] = {"negativo": int((df.adr < 0).sum()), "cero": int((df.adr == 0).sum()), "mayor_1000": int((df.adr > 1000).sum()),
            "max": float(df.adr.max()), "cero_y_complementary": int(((df.adr == 0) & (df.market_segment == "Complementary")).sum())}
m["adultos_mayor_10"] = int((df.adults > 10).sum())

# Validez
meses = {mm: i for i, mm in enumerate(["January", "February", "March", "April", "May", "June", "July", "August", "September",
                                       "October", "November", "December"], 1)}
f = pd.to_datetime(dict(year=df.arrival_date_year, month=df.arrival_date_month.map(meses), day=df.arrival_date_day_of_month), errors="coerce")
m["fechas_llegada_invalidas"] = int(f.isna().sum())
m["rango_llegadas"] = [str(f.min().date()), str(f.max().date())]
m["is_canceled_valores"] = sorted(df.is_canceled.unique().tolist())
m["categorias_undefined"] = {"market_segment": int((df.market_segment == "Undefined").sum()),
                             "distribution_channel": int((df.distribution_channel == "Undefined").sum())}
co = df[df.reservation_status == "Check-Out"]
rsd = pd.to_datetime(co.reservation_status_date)
esperado = f[co.index] + pd.to_timedelta(co.stays_in_weekend_nights + co.stays_in_week_nights, unit="D")
m["checkout_no_coincide"] = {"n": int((rsd != esperado).sum()), "de": len(co)}
m["batches_mensuales"] = int(f.dt.to_period("M").nunique())
print(json.dumps(m, indent=2, ensure_ascii=False, default=str))
