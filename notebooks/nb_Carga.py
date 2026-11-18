# %% [markdown]
# # nb_Carga · Silver → Gold
# Construye el modelo en estrella en **Gold** (lakehouse predeterminado) y marca una alerta cuando la producción
# de un pozo se aleja más del umbral de su última prueba de pozo.

# %% PARAMETROS
WORKSPACE = ""                 # vacío = el workspace actual
LH_SILVER = "Silver"
UMBRAL_DESVIACION_PCT = 30

# %%
from pyspark.sql import Window, functions as F

WORKSPACE = WORKSPACE or notebookutils.runtime.context["currentWorkspaceName"]
SILVER = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LH_SILVER}.Lakehouse/Tables"
leer = lambda tabla: spark.read.format("delta").load(f"{SILVER}/{tabla.replace('.', '/')}")
sk_fecha = lambda c: F.date_format(c, "yyyyMMdd").cast("int")

pozos, pruebas, equipos = leer("openwell.pozos"), leer("openwell.pruebas_pozo"), leer("sap.equipos_bes")
ordenes, produccion = leer("sap.ordenes_mantenimiento"), leer("produccion.produccion_diaria")

# Dimensiones
dim_pozo = pozos.drop("fecha_actualizacion").withColumn("sk_pozo", F.dense_rank().over(Window.orderBy("clave_pozo")))
sk_pozo = dim_pozo.select("clave_pozo", "sk_pozo")
dim_equipo = equipos.join(sk_pozo, "clave_pozo").drop("clave_pozo") \
    .withColumn("sk_equipo", F.dense_rank().over(Window.orderBy("id_equipo")))
dim_fecha = spark.sql("SELECT explode(sequence(to_date('2026-01-01'), to_date('2026-12-31'), interval 1 day)) AS fecha") \
    .select(sk_fecha("fecha").alias("sk_fecha"), "fecha", F.year("fecha").alias("anio"), F.month("fecha").alias("mes"))

# Regla de negocio: aceite esperado = aceite de la última prueba × horas operadas / 24
ultima_prueba = produccion.join(pruebas, "clave_pozo").where("fecha_prueba <= fecha") \
    .withColumn("n", F.row_number().over(Window.partitionBy("clave_pozo", "fecha").orderBy(F.desc("fecha_prueba")))) \
    .where("n = 1").select("clave_pozo", "fecha", F.col("aceite_bpd").alias("aceite_prueba"))
esperado = F.col("aceite_prueba") * F.col("horas_operacion") / 24
desviacion = 100 * (F.col("aceite_bbl") - esperado) / esperado

fact_produccion = produccion.join(ultima_prueba, ["clave_pozo", "fecha"], "left").join(sk_pozo, "clave_pozo").select(
    sk_fecha("fecha").alias("sk_fecha"), "sk_pozo", "aceite_bbl", "agua_bbl", "gas_kpc", "horas_operacion",
    F.round(100 * F.col("agua_bbl") / (F.col("agua_bbl") + F.col("aceite_bbl")), 2).alias("bsw_pct"),
    F.round(esperado, 2).alias("aceite_esperado_bbl"), F.round(desviacion, 1).alias("desviacion_pct"),
    F.when(F.abs(desviacion) > UMBRAL_DESVIACION_PCT, 1).otherwise(0).alias("alerta_desviacion"))

fact_mantenimiento = ordenes.join(dim_equipo.select("id_equipo", "sk_equipo", "sk_pozo"), "id_equipo").select(
    sk_fecha("fecha_inicio").alias("sk_fecha"), "sk_equipo", "sk_pozo", "numero_orden", "tipo_orden", "estado",
    "costo_cop", F.datediff("fecha_fin", "fecha_inicio").alias("duracion_dias"))

for tabla, df in {"dim_pozo": dim_pozo, "dim_equipo": dim_equipo, "dim_fecha": dim_fecha,
                  "fact_produccion_diaria": fact_produccion, "fact_mantenimiento": fact_mantenimiento}.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla)
    print(f"{tabla}: {df.count()} filas")

# %%
display(fact_produccion.where("alerta_desviacion = 1").join(sk_pozo, "sk_pozo")
        .select("sk_fecha", "clave_pozo", "aceite_bbl", "aceite_esperado_bbl", "desviacion_pct"))
