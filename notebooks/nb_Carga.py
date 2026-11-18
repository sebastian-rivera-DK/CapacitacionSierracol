# %% [markdown]
# # nb_Carga · Silver → Gold
# Construye un modelo en estrella en **Gold** (lakehouse predeterminado) y marca una alerta cuando la producción
# de un pozo se aleja más del 30 % de su última prueba de pozo.

# %% PARAMETROS
WORKSPACE = ""                 # vacío = el workspace donde corre el notebook
LH_SILVER = "Silver"
UMBRAL_DESVIACION_PCT = 30

# %% [markdown]
# ## 1. Leer Silver

# %%
from pyspark.sql import Window, functions as F

WORKSPACE = WORKSPACE or notebookutils.runtime.context["currentWorkspaceName"]
SILVER = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LH_SILVER}.Lakehouse/Tables"


def leer(tabla):              # 'openwell.pozos' → Tables/openwell/pozos
    return spark.read.format("delta").load(f"{SILVER}/{tabla.replace('.', '/')}")


def guardar(df, tabla):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla)
    print(f"{tabla}: {df.count()} filas")


def sk_fecha(c):              # 2026-11-15 → 20261115
    return F.date_format(c, "yyyyMMdd").cast("int")


pozos, pruebas = leer("openwell.pozos"), leer("openwell.pruebas_pozo")
equipos, ordenes = leer("sap.equipos_bes"), leer("sap.ordenes_mantenimiento")
produccion = leer("produccion.produccion_diaria")

# %% [markdown]
# ## 2. Dimensiones

# %%
dim_pozo = pozos.withColumn("sk_pozo", F.dense_rank().over(Window.orderBy("clave_pozo"))) \
    .select("sk_pozo", "clave_pozo", "uwi", "nombre_pozo", "campo", "latitud", "longitud", "estado", "metodo_levantamiento")
dim_equipo = equipos.join(dim_pozo.select("clave_pozo", "sk_pozo"), "clave_pozo") \
    .withColumn("sk_equipo", F.dense_rank().over(Window.orderBy("id_equipo"))) \
    .select("sk_equipo", "id_equipo", "sk_pozo", "tipo_equipo", "fabricante", "modelo", "potencia_hp")
# Calendario del año de los datos
dim_fecha = spark.sql("SELECT explode(sequence(to_date('2026-01-01'), to_date('2026-12-31'), interval 1 day)) AS fecha") \
    .select(sk_fecha("fecha").alias("sk_fecha"), "fecha", F.year("fecha").alias("anio"), F.month("fecha").alias("mes"),
            F.date_format("fecha", "yyyy-MM").alias("anio_mes"), F.dayofmonth("fecha").alias("dia"))

guardar(dim_pozo, "dim_pozo")
guardar(dim_equipo, "dim_equipo")
guardar(dim_fecha, "dim_fecha")

# %% [markdown]
# ## 3. Hechos
# **Regla de negocio:** aceite esperado = aceite de la última prueba × horas operadas / 24.
# Si el aceite real se aleja más del umbral, `alerta_desviacion = 1`.

# %%
sk = dim_pozo.select("clave_pozo", "sk_pozo")

ultima_prueba = produccion.select("clave_pozo", "fecha") \
    .join(pruebas.select("clave_pozo", "fecha_prueba", F.col("aceite_bpd").alias("aceite_prueba")), "clave_pozo") \
    .where("fecha_prueba <= fecha") \
    .withColumn("n", F.row_number().over(Window.partitionBy("clave_pozo", "fecha").orderBy(F.desc("fecha_prueba")))) \
    .where("n = 1").drop("n")

esperado = F.col("aceite_prueba") * F.col("horas_operacion") / 24
fact_produccion = produccion.join(ultima_prueba, ["clave_pozo", "fecha"], "left").join(sk, "clave_pozo") \
    .select(sk_fecha("fecha").alias("sk_fecha"), "sk_pozo", "aceite_bbl", "agua_bbl", "gas_kpc", "bsw_pct",
            "horas_operacion", F.round(esperado, 2).alias("aceite_esperado_bbl"),
            F.round(100 * (F.col("aceite_bbl") - esperado) / esperado, 1).alias("desviacion_pct")) \
    .withColumn("alerta_desviacion", F.when(F.abs("desviacion_pct") > UMBRAL_DESVIACION_PCT, 1).otherwise(0))

fact_prueba = pruebas.join(sk, "clave_pozo") \
    .select(sk_fecha("fecha_prueba").alias("sk_fecha"), "sk_pozo", "id_prueba", "aceite_bpd", "agua_bpd",
            "bsw_pct", "presion_cabeza_psi")

fact_mantenimiento = ordenes.join(dim_equipo.select("id_equipo", "sk_equipo", "sk_pozo"), "id_equipo") \
    .select(sk_fecha("fecha_inicio").alias("sk_fecha"), "sk_equipo", "sk_pozo", "numero_orden", "tipo_orden", "estado",
            "costo_cop", F.datediff("fecha_fin", "fecha_inicio").alias("duracion_dias"))

guardar(fact_produccion, "fact_produccion_diaria")
guardar(fact_prueba, "fact_prueba_pozo")
guardar(fact_mantenimiento, "fact_mantenimiento")

# %% [markdown]
# ## 4. Pozos con alerta de desviación

# %%
display(spark.table("fact_produccion_diaria").where("alerta_desviacion = 1")
        .join(spark.table("dim_pozo"), "sk_pozo")
        .select("sk_fecha", "clave_pozo", "aceite_bbl", "aceite_esperado_bbl", "desviacion_pct"))
