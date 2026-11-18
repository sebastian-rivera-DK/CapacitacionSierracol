# %% [markdown]
# # 🧹 nb_Transformacion · Bronze → Silver
#
# Lee los 5 archivos CSV que el pipeline de extracción dejó en **Bronze**, los **limpia** y los escribe como
# tablas Delta en **Silver**.
#
# | Paso | Qué pasa con el dato |
# |---|---|
# | **Limpiar** | Se corrige el formato: números con coma decimal, fechas en varios formatos, nombres con variantes |
# | **Filtrar** | Se descartan los registros que no sirven (por ejemplo, un pozo que no existe) |
# | **Quitar duplicados** | Si un registro llega más de una vez, se conserva el más reciente |
#
# > **Antes de ejecutar:** agrega el lakehouse **Silver** como lakehouse predeterminado del notebook
# > (panel *Explorer* → *Add data items* → *Existing data sources* → `Silver`).

# %% [markdown]
# ### ⚙️ Parámetros

# %% PARAMETROS
WORKSPACE = ""        # vacío = el workspace donde corre el notebook
LH_BRONZE = "Bronze"  # lakehouse donde el pipeline dejó los archivos

# %% [markdown]
# ## 1. Preparación
#
# Funciones que se reutilizan con todas las fuentes:
#
# | Función | Qué hace | Ejemplo |
# |---|---|---|
# | `leer` | Lee un CSV de Bronze como texto y normaliza los encabezados | `"ID EQUIPO"` → `id_equipo` |
# | `numero` | Convierte texto a número | `"245,5"` → `245.5` · `"8828 ft"` → `8828` |
# | `fecha` | Convierte texto a fecha, en tres formatos | `2026-11-15` · `15/11/2026` · `20261115` |
# | `clave_pozo` | Une las formas de escribir un pozo en una sola llave | `CO-LLA-MORICHAL-001` · `MORICHAL 1` → `MOR-001` |
# | `quitar_duplicados` | Deja un registro por llave, el más reciente | |
# | `guardar` | Escribe la tabla en Silver | |

# %%
import re
from pyspark.sql import Window, functions as F

WORKSPACE = WORKSPACE or notebookutils.runtime.context["currentWorkspaceName"]
BRONZE = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LH_BRONZE}.Lakehouse/Files/bronze"
resumen = []


def leer(ruta):
    df = spark.read.option("header", True).option("sep", ";").csv(f"{BRONZE}/{ruta}")
    nombres = [re.sub(r"[^a-z0-9]+", "_", c.replace("﻿", "").strip().lower()).strip("_") for c in df.columns]
    return df.toDF(*nombres).withColumn("_fila", F.monotonically_increasing_id())


def numero(c):
    return F.regexp_replace(F.regexp_replace(c, ",", "."), r"[^0-9.\-]", "").cast("double")


def fecha(c):
    c = F.col(c)
    return (F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(F.substring(c, 1, 10)))
             .when(c.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(c, "dd/MM/yyyy"))
             .when(c.rlike(r"^\d{8}$"), F.to_date(c, "yyyyMMdd")))


def clave_pozo(c):
    s, p = F.regexp_replace(F.upper(F.trim(c)), "^CO-LLA-", ""), r"^([A-Z]{3})[A-Z]*[^A-Z0-9]*0*([0-9]+)$"
    return F.when(s.rlike(p), F.concat(F.regexp_extract(s, p, 1), F.lit("-"), F.lpad(F.regexp_extract(s, p, 2), 3, "0")))


def quitar_duplicados(df, llaves, mas_reciente):
    ventana = Window.partitionBy(*llaves).orderBy(F.desc(mas_reciente))
    return df.withColumn("_n", F.row_number().over(ventana)).where("_n = 1").drop("_n")


def guardar(df, tabla, filas_bronze):
    esquema = tabla.split(".")[0]
    df = df.drop("_fila")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esquema}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla)
    filas_silver = df.count()
    resumen.append((tabla, filas_bronze, filas_silver, filas_bronze - filas_silver))
    print(f"✅ {tabla}: {filas_bronze} filas en Bronze → {filas_silver} en Silver")

# %% [markdown]
# ## 2. Pozos (OpenWell)
#
# | Limpieza | Filtro |
# |---|---|
# | Llave del pozo, campo en mayúsculas, latitud y longitud invertidas, fechas | Solo pozos con identificador válido · sin duplicados (gana la actualización más reciente) |

# %%
bronze = leer("openwell/pozos.csv")
lat, lon = numero("latitud"), numero("longitud")

pozos = bronze.select(
    clave_pozo("id_pozo").alias("clave_pozo"),
    F.trim("id_pozo").alias("uwi"),
    F.trim("nombre_pozo").alias("nombre_pozo"),
    F.upper(F.trim("campo")).alias("campo"),
    F.when(lat < -60, lon).otherwise(lat).alias("latitud"),     # vienen invertidas en algunos registros
    F.when(lat < -60, lat).otherwise(lon).alias("longitud"),
    F.upper("estado").alias("estado"),
    F.coalesce(F.upper("metodo_levantamiento"), F.lit("SIN DATO")).alias("metodo_levantamiento"),
    fecha("fecha_actualizacion").alias("fecha_actualizacion"),
)
pozos = pozos.where("clave_pozo IS NOT NULL")
pozos = quitar_duplicados(pozos, ["clave_pozo"], "fecha_actualizacion")

guardar(pozos, "openwell.pozos", bronze.count())
display(pozos.limit(5))

# %% [markdown]
# ## 3. Equipos BES (SAP)
#
# | Limpieza | Filtro |
# |---|---|
# | Ubicación técnica SAP → llave del pozo, fabricante homologado (`Fab. A` → `Fabricante A`) | Solo equipos de pozos que existen |

# %%
bronze = leer("sap/equipos_bes.csv")

equipos = bronze.select(
    F.trim("id_equipo").alias("id_equipo"),
    clave_pozo("ubicacion_tecnica").alias("clave_pozo"),
    F.upper("tipo").alias("tipo_equipo"),
    F.initcap(F.regexp_replace(F.upper(F.trim("fabricante")), r"^FAB\.? ", "FABRICANTE ")).alias("fabricante"),
    F.trim("modelo").alias("modelo"),
    numero("potencia_hp").alias("potencia_hp"),
    fecha("fecha_instalacion").alias("fecha_instalacion"),
)
equipos = equipos.join(pozos.select("clave_pozo"), "clave_pozo", "left_semi")

guardar(equipos, "sap.equipos_bes", bronze.count())
display(equipos.groupBy("fabricante").count())

# %% [markdown]
# ## 4. Órdenes de mantenimiento (SAP PM)
#
# | Limpieza | Filtro |
# |---|---|
# | Estados (`TECO`, `Cerrada`, `CERRADO` → `Cerrada`), tipo de orden, costo (`$ 12.500.000` → `12500000`), fechas | Sin duplicados (gana la última modificación) |

# %%
bronze = leer("sap/ordenes_mantenimiento.csv")
estado, tipo = F.upper(F.trim("estado")), F.upper(F.trim("tipo_orden"))

ordenes = bronze.select(
    F.trim("numero_orden").alias("numero_orden"),
    F.trim("id_equipo").alias("id_equipo"),
    F.when(tipo == "PM01", "Correctivo").when(tipo == "PM02", "Preventivo").otherwise("Predictivo").alias("tipo_orden"),
    F.when(estado.isin("CRTD", "CREADA"), "Creada")
     .when(estado.isin("REL", "LIBERADA"), "Liberada")
     .otherwise("Cerrada").alias("estado"),
    F.regexp_replace(F.regexp_replace("costo_cop", r",\d{2}$", ""), r"[^0-9]", "").cast("decimal(18,2)").alias("costo_cop"),
    fecha("fecha_inicio").alias("fecha_inicio"),
    fecha("fecha_fin").alias("fecha_fin"),
    F.to_timestamp("fecha_modificacion").alias("fecha_modificacion"),
)
ordenes = quitar_duplicados(ordenes, ["numero_orden"], "fecha_modificacion")

guardar(ordenes, "sap.ordenes_mantenimiento", bronze.count())
display(ordenes.groupBy("estado").count())

# %% [markdown]
# ## 5. Pruebas de pozo (OpenWell)
#
# | Limpieza | Filtro |
# |---|---|
# | Llave del pozo, números, el BSW se **recalcula** con agua / (agua + aceite) | Solo pruebas con aceite medido |

# %%
bronze = leer("openwell/pruebas_pozo.csv")

pruebas = bronze.select(
    F.trim("id_prueba").alias("id_prueba"),
    clave_pozo("pozo").alias("clave_pozo"),
    fecha("fecha_prueba").alias("fecha_prueba"),
    numero("aceite_bpd").alias("aceite_bpd"),
    numero("agua_bpd").alias("agua_bpd"),
    numero("presion_cabeza_psi").alias("presion_cabeza_psi"),
)
pruebas = pruebas.where("aceite_bpd IS NOT NULL")
pruebas = pruebas.withColumn("bsw_pct", F.round(100 * F.col("agua_bpd") / (F.col("agua_bpd") + F.col("aceite_bpd")), 2))

guardar(pruebas, "openwell.pruebas_pozo", bronze.count())
display(pruebas.limit(5))

# %% [markdown]
# ## 6. Producción diaria
#
# | Limpieza | Filtro |
# |---|---|
# | Llave del pozo (`MORICHAL 1`, ` mor-1 ` → `MOR-001`), coma decimal, BSW | Solo pozos que existen y con fecha · sin volúmenes negativos · sin duplicados (gana la última fila del archivo) |

# %%
bronze = leer("produccion/produccion_diaria.csv")

produccion = bronze.select(
    clave_pozo("pozo").alias("clave_pozo"),
    fecha("fecha").alias("fecha"),
    numero("aceite_bbl").alias("aceite_bbl"),
    numero("agua_bbl").alias("agua_bbl"),
    numero("gas_kpc").alias("gas_kpc"),
    numero("horas_operacion").alias("horas_operacion"),
    "_fila",
)
produccion = produccion.join(pozos.select("clave_pozo"), "clave_pozo", "left_semi")
produccion = produccion.where("fecha IS NOT NULL AND aceite_bbl >= 0 AND agua_bbl >= 0")
produccion = quitar_duplicados(produccion, ["clave_pozo", "fecha"], "_fila")
produccion = produccion.withColumn("bsw_pct", F.round(100 * F.col("agua_bbl") / (F.col("agua_bbl") + F.col("aceite_bbl")), 2))

guardar(produccion, "produccion.produccion_diaria", bronze.count())
display(produccion.groupBy("fecha").agg(F.count("*").alias("pozos"), F.round(F.sum("aceite_bbl")).alias("aceite_bbl"))
        .orderBy("fecha"))

# %% [markdown]
# ## 7. Resumen
# Cuántas filas llegaron de Bronze y cuántas quedaron en Silver después de limpiar.

# %%
display(spark.createDataFrame(resumen, "tabla string, filas_bronze long, filas_silver long, descartadas long"))
