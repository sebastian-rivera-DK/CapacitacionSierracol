# %% [markdown]
# # nb_Carga · Silver → Gold: modelo dimensional
#
# Lee las tablas limpias de **Silver**, aplica las **reglas de negocio** y deja en **Gold** un modelo en estrella,
# listo para Power BI.
#
# **Antes de ejecutar:** agrega el lakehouse **Gold** como lakehouse predeterminado del notebook
# (panel *Explorer* → *Add data items* → *Existing data sources* → `Gold`).
#
# ```
#                        dim_fecha
#                 ┌──────────┼──────────────┐
#   fact_produccion_diaria  fact_prueba_pozo  fact_mantenimiento ── dim_equipo
#                 └──────────┼──────────────┘          │
#                        dim_pozo ─────────────────────┘
# ```
#
# | Tabla | Una fila por… |
# |---|---|
# | `dim_pozo` | pozo |
# | `dim_equipo` | equipo de bombeo o variador |
# | `dim_fecha` | día |
# | `fact_produccion_diaria` | pozo y día |
# | `fact_prueba_pozo` | prueba de pozo |
# | `fact_mantenimiento` | orden de mantenimiento |
#
# **Regla de negocio principal:** comparar la producción de cada día con la **última prueba de pozo válida**.
# Si el aceite se desvía más de 30 % de lo esperado, se marca una alerta.

# %% [markdown] PARAMETROS
# ### ⚙️ Parámetros

# %% PARAMETROS
WORKSPACE = ""                 # nombre del workspace; vacío = el workspace donde corre el notebook
LH_SILVER = "Silver"
UMBRAL_DESVIACION_PCT = 30     # % de diferencia contra la última prueba de pozo que dispara la alerta

# %% [markdown]
# ## 1. Leer Silver

# %%
from pyspark.sql import Window
from pyspark.sql import functions as F

if not WORKSPACE:
    try:
        WORKSPACE = notebookutils.runtime.context["currentWorkspaceName"]
    except Exception:
        raise ValueError("No se pudo leer el nombre del workspace: escríbelo en el parámetro WORKSPACE")


def ruta_silver(esquema, tabla):
    return f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LH_SILVER}.Lakehouse/Tables/{esquema}/{tabla}"


def leer_silver(esquema, tabla):
    return spark.read.format("delta").load(ruta_silver(esquema, tabla))


def guardar(df, tabla):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla)
    print(f"{tabla}: {df.count()} filas")


def sk_fecha(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


pozo = leer_silver("openwell", "pozos")
prueba = leer_silver("openwell", "pruebas_pozo")
equipo = leer_silver("sap", "equipos_bes")
orden = leer_silver("sap", "ordenes_mantenimiento")
prod = leer_silver("produccion", "produccion_diaria")

# %% [markdown]
# ## 2. Dimensiones
# Llaves sustitutas enteras (`sk_`): Power BI relaciona y comprime mejor con enteros.

# %%
dim_pozo = (pozo.withColumn("sk_pozo", F.dense_rank().over(Window.orderBy("clave_pozo")))
            .select("sk_pozo", "clave_pozo", "uwi", "nombre_pozo", "campo", "latitud", "longitud", "estado",
                    "metodo_levantamiento", "profundidad_ft", "fecha_spud",
                    F.coalesce("advertencias", F.lit("Sin observaciones")).alias("calidad_maestro")))

dim_equipo = (equipo.join(dim_pozo.select("clave_pozo", "sk_pozo"), "clave_pozo", "left")
              .withColumn("sk_equipo", F.dense_rank().over(Window.orderBy("id_equipo")))
              .select("sk_equipo", "id_equipo", "tipo_equipo", "fabricante", "modelo", "potencia_hp",
                      "fecha_instalacion", "estado", "sk_pozo"))

limites = (prod.select(F.min("fecha").alias("a"), F.max("fecha").alias("b"))
           .union(prueba.select(F.min("fecha_prueba"), F.max("fecha_prueba")))
           .union(orden.select(F.min("fecha_inicio"), F.max("fecha_inicio")))
           .agg(F.min("a").alias("ini"), F.max("b").alias("fin")).collect()[0])
dim_fecha = (spark.sql(f"SELECT explode(sequence(to_date('{limites['ini']}'), to_date('{limites['fin']}'), interval 1 day)) AS fecha")
             .select(sk_fecha("fecha").alias("sk_fecha"), "fecha", F.year("fecha").alias("anio"),
                     F.month("fecha").alias("mes"), F.date_format("fecha", "yyyy-MM").alias("anio_mes"),
                     F.dayofmonth("fecha").alias("dia"), F.weekofyear("fecha").alias("semana")))

guardar(dim_pozo, "dim_pozo")
guardar(dim_equipo, "dim_equipo")
guardar(dim_fecha, "dim_fecha")

# %% [markdown]
# ## 3. Hecho de pruebas de pozo

# %%
fact_prueba = (prueba.join(dim_pozo.select("clave_pozo", "sk_pozo"), "clave_pozo")
               .select("id_prueba", sk_fecha("fecha_prueba").alias("sk_fecha"), "sk_pozo", "fecha_prueba",
                       "duracion_horas", "aceite_bpd", "agua_bpd", "gas_kpcd", "bsw_pct", "presion_cabeza_psi"))
guardar(fact_prueba, "fact_prueba_pozo")

# %% [markdown]
# ## 4. Hecho de producción diaria con la regla de negocio
# Para cada pozo y día se busca la **última prueba anterior o igual a esa fecha** y se calcula:
#
# - `aceite_esperado_bbl` = aceite de la prueba × horas operadas / 24
# - `desviacion_pct` = (aceite real − esperado) / esperado × 100
# - `alerta_desviacion` = 1 si la desviación supera el umbral (30 %)

# %%
ultima_prueba = (prod.select("clave_pozo", "fecha")
                 .join(prueba.select("clave_pozo", "fecha_prueba", F.col("aceite_bpd").alias("aceite_prueba_bpd")),
                       "clave_pozo")
                 .where(F.col("fecha_prueba") <= F.col("fecha"))
                 .withColumn("_n", F.row_number().over(Window.partitionBy("clave_pozo", "fecha")
                                                       .orderBy(F.col("fecha_prueba").desc())))
                 .where("_n = 1").drop("_n"))

esperado = F.col("aceite_prueba_bpd") * F.col("horas_operacion") / 24
fact_prod = (prod.join(ultima_prueba, ["clave_pozo", "fecha"], "left")
             .join(dim_pozo.select("clave_pozo", "sk_pozo"), "clave_pozo")
             .withColumn("aceite_esperado_bbl", F.round(esperado, 2))
             .withColumn("desviacion_pct",
                         F.round(F.when(esperado > 0, 100 * (F.col("aceite_bbl") - esperado) / esperado), 1))
             .select(sk_fecha("fecha").alias("sk_fecha"), "sk_pozo",
                     "aceite_bbl", "agua_bbl", "gas_kpc",
                     F.round(F.col("aceite_bbl") + F.col("agua_bbl"), 2).alias("liquido_bbl"),
                     "bsw_pct", "horas_operacion",
                     F.round(F.col("horas_operacion") / 24, 4).alias("eficiencia_operativa"),
                     "fecha_prueba", "aceite_esperado_bbl", "desviacion_pct",
                     F.when(F.abs("desviacion_pct") > UMBRAL_DESVIACION_PCT, 1).otherwise(0).alias("alerta_desviacion"),
                     "observaciones"))
guardar(fact_prod, "fact_produccion_diaria")

# %% [markdown]
# ## 5. Hecho de mantenimiento

# %%
fact_mant = (orden.join(dim_equipo.select("id_equipo", "sk_equipo", "sk_pozo"), "id_equipo")
             .select("numero_orden", sk_fecha("fecha_inicio").alias("sk_fecha"), "sk_equipo", "sk_pozo",
                     "tipo_orden", "estado", "descripcion", "costo_cop",
                     F.datediff("fecha_fin", "fecha_inicio").alias("duracion_dias"),
                     F.when(F.col("estado") != "Cerrada", 1).otherwise(0).alias("es_abierta")))
guardar(fact_mant, "fact_mantenimiento")

# %% [markdown]
# ## 6. Verificación: ¿qué pozos tienen alerta de desviación?

# %%
display(spark.table("fact_produccion_diaria").where("alerta_desviacion = 1")
        .join(spark.table("dim_pozo"), "sk_pozo").join(spark.table("dim_fecha"), "sk_fecha")
        .select("fecha", "clave_pozo", "campo", "aceite_bbl", "aceite_esperado_bbl", "desviacion_pct", "fecha_prueba")
        .orderBy("clave_pozo", "fecha"))

# %%
display(spark.table("fact_produccion_diaria").join(spark.table("dim_pozo"), "sk_pozo").join(spark.table("dim_fecha"), "sk_fecha")
        .groupBy("fecha", "campo")
        .agg(F.round(F.sum("aceite_bbl"), 0).alias("aceite_bbl"),
             F.round(100 * F.sum("agua_bbl") / (F.sum("agua_bbl") + F.sum("aceite_bbl")), 1).alias("bsw_ponderado_pct"),
             F.count("*").alias("pozos_reportando"))
        .orderBy("fecha", "campo"))
