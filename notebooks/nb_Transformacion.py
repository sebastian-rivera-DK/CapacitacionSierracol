# %% [markdown]
# # nb_Transformacion · Bronze → Silver
# Limpia los CSV de **Bronze** y los escribe en **Silver** (lakehouse predeterminado).
# Lo que no pasa las reglas va a `calidad.cuarentena` con el motivo.

# %% PARAMETROS
WORKSPACE = ""        # vacío = el workspace actual
LH_BRONZE = "Bronze"

# %%
import re
from functools import reduce
from pyspark.sql import DataFrame, Window, functions as F

WORKSPACE = WORKSPACE or notebookutils.runtime.context["currentWorkspaceName"]
BRONZE = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LH_BRONZE}.Lakehouse/Files/bronze"
cuarentena = []


def num(c):          # '245,5' → 245.5 · '8828 ft' → 8828
    return F.regexp_replace(F.regexp_replace(c, ",", "."), r"[^0-9.\-]", "").cast("double")


def fecha(c):        # yyyy-MM-dd · dd/MM/yyyy · yyyyMMdd
    return (F.when(F.col(c).rlike(r"^\d{4}-"), F.to_date(F.substring(c, 1, 10)))
             .when(F.col(c).rlike("/"), F.to_date(c, "dd/MM/yyyy")).otherwise(F.to_date(c, "yyyyMMdd")))


def pozo(c):         # CO-LLA-MOR-0001 · CO-LLA-MORICHAL-001 · 'MORICHAL 1' · ' mor-1 ' → MOR-001
    s, p = F.regexp_replace(F.upper(F.trim(c)), "^CO-LLA-", ""), r"^([A-Z]{3})[A-Z]*[^A-Z0-9]*0*([0-9]+)$"
    return F.when(s.rlike(p), F.concat(F.regexp_extract(s, p, 1), F.lit("-"), F.lpad(F.regexp_extract(s, p, 2), 3, "0")))


def homologar(c, valores):   # {'TECO': 'Cerrada', ...}
    return F.create_map(*[F.lit(x) for par in valores.items() for x in par])[F.upper(F.trim(c))]


def procesar(archivo, tabla, columnas, reglas, llaves, mas_reciente="_fila", referencia=None):
    """Lee el CSV, limpia, rechaza (reglas y duplicados) y escribe en Silver."""
    df = spark.read.option("header", True).option("sep", ";").csv(f"{BRONZE}/{archivo}")
    df = df.toDF(*[re.sub(r"\W+", "_", c.replace("﻿", "").strip().lower()) for c in df.columns])
    df = df.withColumn("_fila", F.monotonically_increasing_id()).select("_fila", *columnas)
    if referencia:                                   # la llave debe existir en otra tabla de Silver
        ref, llave = referencia
        df = df.join(ref.select(llave).distinct().withColumn("_existe", F.lit(1)), llave, "left")

    df = df.withColumn("motivo", F.coalesce(*[F.when(F.expr(regla), motivo) for motivo, regla in reglas.items()]))
    orden = Window.partitionBy(*llaves, F.col("motivo").isNull()).orderBy(F.desc(mas_reciente))
    df = df.withColumn("motivo", F.when(F.col("motivo").isNull() & (F.row_number().over(orden) > 1), "DUPLICADO")
                                  .otherwise(F.col("motivo")))

    cols = [c for c in df.columns if not c.startswith("_") and c != "motivo"]
    cuarentena.append(df.where("motivo IS NOT NULL")
                        .select(F.lit(tabla).alias("tabla"), "motivo", F.to_json(F.struct(*cols)).alias("registro")))
    limpio = df.where("motivo IS NULL").select(*cols)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tabla.split('.')[0]}")
    limpio.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla)
    print(f"{tabla}: {df.count()} leídas · {limpio.count()} en Silver")
    return limpio

# %%
lat, lon = num("latitud"), num("longitud")
pozos = procesar("openwell/pozos.csv", "openwell.pozos",
    [pozo("id_pozo").alias("clave_pozo"), F.trim("nombre_pozo").alias("nombre_pozo"), F.upper(F.trim("campo")).alias("campo"),
     F.when(lat < -60, lon).otherwise(lat).alias("latitud"), F.when(lat < -60, lat).otherwise(lon).alias("longitud"),
     F.upper("estado").alias("estado"), fecha("fecha_actualizacion").alias("fecha_actualizacion")],
    {"POZO_INVALIDO": "clave_pozo IS NULL",
     "FUERA_DE_COLOMBIA": "NOT (latitud BETWEEN -4.3 AND 13.5 AND longitud BETWEEN -82 AND -66)"},
    ["clave_pozo"], "fecha_actualizacion")

equipos = procesar("sap/equipos_bes.csv", "sap.equipos_bes",
    [F.col("id_equipo"), pozo("ubicacion_tecnica").alias("clave_pozo"), F.col("tipo").alias("tipo_equipo"),
     F.initcap(F.regexp_replace(F.upper(F.trim("fabricante")), r"^FAB\.? ", "FABRICANTE ")).alias("fabricante"),
     num("potencia_hp").alias("potencia_hp"), fecha("fecha_instalacion").alias("fecha_instalacion")],
    {"POZO_NO_EXISTE": "_existe IS NULL", "FECHA_FUTURA": "fecha_instalacion > current_date()"},
    ["id_equipo"], referencia=(pozos, "clave_pozo"))

ordenes = procesar("sap/ordenes_mantenimiento.csv", "sap.ordenes_mantenimiento",
    [F.col("numero_orden"), F.col("id_equipo"),
     homologar("tipo_orden", {"PM01": "Correctivo", "PM02": "Preventivo", "PM03": "Predictivo"}).alias("tipo_orden"),
     homologar("estado", {"CRTD": "Creada", "CREADA": "Creada", "REL": "Liberada", "LIBERADA": "Liberada",
                          "TECO": "Cerrada", "CERRADA": "Cerrada", "CERRADO": "Cerrada"}).alias("estado"),
     F.regexp_replace(F.regexp_replace("costo_cop", r",\d{2}$", ""), r"[^0-9]", "").cast("decimal(18,2)").alias("costo_cop"),
     fecha("fecha_inicio").alias("fecha_inicio"), fecha("fecha_fin").alias("fecha_fin"),
     F.to_timestamp("fecha_modificacion").alias("fecha_modificacion")],
    {"EQUIPO_NO_EXISTE": "_existe IS NULL", "FIN_ANTES_DEL_INICIO": "fecha_fin < fecha_inicio"},
    ["numero_orden"], "fecha_modificacion", referencia=(equipos, "id_equipo"))

pruebas = procesar("openwell/pruebas_pozo.csv", "openwell.pruebas_pozo",
    [F.col("id_prueba"), pozo("pozo").alias("clave_pozo"), fecha("fecha_prueba").alias("fecha_prueba"),
     num("duracion_horas").alias("duracion_horas"), num("aceite_bpd").alias("aceite_bpd")],
    {"POZO_NO_EXISTE": "_existe IS NULL", "SIN_ACEITE": "aceite_bpd IS NULL",
     "DURACION_INVALIDA": "coalesce(duracion_horas, 0) <= 0", "FECHA_FUTURA": "fecha_prueba > current_date()"},
    ["id_prueba"], referencia=(pozos, "clave_pozo"))

produccion = procesar("produccion/produccion_diaria.csv", "produccion.produccion_diaria",
    [pozo("pozo").alias("clave_pozo"), fecha("fecha").alias("fecha"), num("aceite_bbl").alias("aceite_bbl"),
     num("agua_bbl").alias("agua_bbl"), num("gas_kpc").alias("gas_kpc"), num("horas_operacion").alias("horas_operacion")],
    {"POZO_NO_EXISTE": "_existe IS NULL", "FECHA_INVALIDA": "fecha IS NULL",
     "VOLUMEN_NEGATIVO": "aceite_bbl < 0 OR agua_bbl < 0", "HORAS_FUERA_DE_RANGO": "NOT horas_operacion BETWEEN 0 AND 24"},
    ["clave_pozo", "fecha"], referencia=(pozos, "clave_pozo"))

# %%
rechazos = reduce(DataFrame.unionByName, cuarentena)
spark.sql("CREATE SCHEMA IF NOT EXISTS calidad")
rechazos.write.format("delta").mode("overwrite").saveAsTable("calidad.cuarentena")
display(rechazos.groupBy("tabla", "motivo").count().orderBy("tabla", "motivo"))
