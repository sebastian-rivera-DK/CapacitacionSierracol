# %% [markdown]
# # nb_Transformacion · Bronze → Silver
# Lee los CSV de **Bronze**, los limpia y escribe tablas Delta en **Silver** (lakehouse predeterminado).
# Lo que no pasa las reglas va a `calidad.cuarentena` con el motivo.

# %% PARAMETROS
WORKSPACE = ""        # vacío = el workspace donde corre el notebook
LH_BRONZE = "Bronze"

# %% [markdown]
# ## 1. Funciones de apoyo

# %%
import re
from pyspark.sql import Window, functions as F

WORKSPACE = WORKSPACE or notebookutils.runtime.context["currentWorkspaceName"]
BRONZE = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LH_BRONZE}.Lakehouse/Files/bronze"
cuarentena = []


def leer(ruta):
    """Lee un CSV como texto y normaliza los encabezados ('ID EQUIPO' → 'id_equipo')."""
    df = spark.read.option("header", True).option("sep", ";").csv(f"{BRONZE}/{ruta}")
    nombres = [re.sub(r"[^a-z0-9]+", "_", c.replace("﻿", "").strip().lower()).strip("_") for c in df.columns]
    return df.toDF(*nombres).withColumn("_fila", F.monotonically_increasing_id())


def numero(c):   # '245,5' → 245.5 · '8828 ft' → 8828
    return F.regexp_replace(F.regexp_replace(c, ",", "."), r"[^0-9.\-]", "").cast("double")


def fecha(c):    # yyyy-MM-dd · dd/MM/yyyy · yyyyMMdd
    return (F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(F.substring(c, 1, 10)))
             .when(c.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(c, "dd/MM/yyyy"))
             .when(c.rlike(r"^\d{8}$"), F.to_date(c, "yyyyMMdd")))


def clave_pozo(c):   # CO-LLA-MOR-0001 · CO-LLA-MORICHAL-001 · 'MORICHAL 1' · ' mor-1 ' → MOR-001
    s, p = F.regexp_replace(F.upper(F.trim(c)), "^CO-LLA-", ""), r"^([A-Z]+)[^A-Z0-9]*0*([0-9]+)$"
    return F.when(s.rlike(p), F.concat(F.substring(F.regexp_extract(s, p, 1), 1, 3), F.lit("-"),
                                       F.lpad(F.regexp_extract(s, p, 2), 3, "0")))


def existe(df, llave):
    return df.select(llave, F.lit(1).alias("_existe")).distinct()


def guardar(df, tabla, reglas, llaves, orden):
    """Aplica las reglas de rechazo, deja un registro por llave y escribe en Silver."""
    motivo = F.lit(None).cast("string")
    for nombre, condicion in reversed(list(reglas.items())):   # gana la primera regla que se cumpla
        motivo = F.when(condicion, nombre).otherwise(motivo)
    df = df.withColumn("motivo", motivo)
    df = df.withColumn("motivo", F.when(F.col("motivo").isNull() & (F.row_number().over(
        Window.partitionBy(*llaves, F.col("motivo").isNull()).orderBy(*orden)) > 1), "DUPLICADO").otherwise(F.col("motivo")))

    columnas = [c for c in df.columns if not c.startswith("_") and c != "motivo"]
    cuarentena.append(df.where("motivo IS NOT NULL").select(
        F.lit(tabla).alias("tabla"), "motivo", F.to_json(F.struct(*columnas)).alias("registro")))
    limpio = df.where("motivo IS NULL").select(*columnas)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tabla.split('.')[0]}")
    limpio.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla)
    print(f"{tabla}: {df.count()} leídas · {limpio.count()} en Silver")
    return limpio

# %% [markdown]
# ## 2. Pozos

# %%
df = leer("openwell/pozos.csv")
lat, lon = numero(F.col("latitud")), numero(F.col("longitud"))
pozos = guardar(df.select(
    clave_pozo(F.col("id_pozo")).alias("clave_pozo"), F.trim("id_pozo").alias("uwi"),
    F.regexp_replace(F.trim("nombre_pozo"), r"\s+", " ").alias("nombre_pozo"), F.upper(F.trim("campo")).alias("campo"),
    F.when(lat < -60, lon).otherwise(lat).alias("latitud"),        # coordenadas invertidas
    F.when(lat < -60, lat).otherwise(lon).alias("longitud"),
    F.upper("estado").alias("estado"), F.coalesce(F.upper("metodo_levantamiento"), F.lit("SIN DATO")).alias("metodo_levantamiento"),
    fecha(F.col("fecha_spud")).alias("fecha_spud"), numero(F.col("profundidad_ft")).cast("int").alias("profundidad_ft"),
    fecha(F.col("fecha_actualizacion")).alias("fecha_actualizacion"), "_fila"),
    "openwell.pozos",
    {"POZO_INVALIDO": F.col("clave_pozo").isNull(),
     "COORDENADAS_FUERA_DE_COLOMBIA": ~(F.col("latitud").between(-4.3, 13.5) & F.col("longitud").between(-82, -66))},
    ["clave_pozo"], [F.col("fecha_actualizacion").desc()])

# %% [markdown]
# ## 3. Equipos BES y órdenes de mantenimiento (SAP)

# %%
df = leer("sap/equipos_bes.csv")
equipos = guardar(df.select(
    F.trim("id_equipo").alias("id_equipo"), clave_pozo(F.col("ubicacion_tecnica")).alias("clave_pozo"),
    F.upper("tipo").alias("tipo_equipo"),
    F.initcap(F.regexp_replace(F.upper(F.trim("fabricante")), r"^FAB\.? ", "FABRICANTE ")).alias("fabricante"),
    F.trim("modelo").alias("modelo"), numero(F.col("potencia_hp")).alias("potencia_hp"),
    fecha(F.col("fecha_instalacion")).alias("fecha_instalacion"), F.upper("estado").alias("estado"), "_fila")
    .join(existe(pozos, "clave_pozo"), "clave_pozo", "left"),
    "sap.equipos_bes",
    {"POZO_NO_EXISTE": F.col("_existe").isNull(),
     "FECHA_FUTURA": F.col("fecha_instalacion") > F.current_date()},
    ["id_equipo"], [F.col("_fila").desc()])

df = leer("sap/ordenes_mantenimiento.csv")
estado, tipo = F.upper(F.trim("estado")), F.upper(F.trim("tipo_orden"))
ordenes = guardar(df.select(
    F.trim("numero_orden").alias("numero_orden"), F.trim("id_equipo").alias("id_equipo"),
    F.when(tipo == "PM01", "Correctivo").when(tipo == "PM02", "Preventivo").otherwise("Predictivo").alias("tipo_orden"),
    F.trim("descripcion").alias("descripcion"),
    fecha(F.col("fecha_inicio")).alias("fecha_inicio"), fecha(F.col("fecha_fin")).alias("fecha_fin"),
    F.when(estado.isin("CRTD", "CREADA"), "Creada").when(estado.isin("REL", "LIBERADA"), "Liberada")
     .when(estado.isin("TECO", "CERRADA", "CERRADO"), "Cerrada").alias("estado"),
    F.regexp_replace(F.regexp_replace("costo_cop", r",\d{2}$", ""), r"[^0-9]", "").cast("decimal(18,2)").alias("costo_cop"),
    F.to_timestamp("fecha_modificacion").alias("fecha_modificacion"), "_fila")
    .join(existe(equipos, "id_equipo"), "id_equipo", "left"),
    "sap.ordenes_mantenimiento",
    {"EQUIPO_NO_EXISTE": F.col("_existe").isNull(),
     "FIN_ANTES_DEL_INICIO": F.col("fecha_fin") < F.col("fecha_inicio")},
    ["numero_orden"], [F.col("fecha_modificacion").desc()])

# %% [markdown]
# ## 4. Pruebas de pozo y producción diaria

# %%
df = leer("openwell/pruebas_pozo.csv")
pruebas = guardar(df.select(
    F.trim("id_prueba").alias("id_prueba"), clave_pozo(F.col("pozo")).alias("clave_pozo"),
    fecha(F.col("fecha_prueba")).alias("fecha_prueba"), numero(F.col("duracion_horas")).alias("duracion_horas"),
    numero(F.col("aceite_bpd")).alias("aceite_bpd"), numero(F.col("agua_bpd")).alias("agua_bpd"),
    numero(F.col("gas_kpcd")).alias("gas_kpcd"), numero(F.col("presion_cabeza_psi")).alias("presion_cabeza_psi"), "_fila")
    .withColumn("bsw_pct", F.round(100 * F.col("agua_bpd") / (F.col("agua_bpd") + F.col("aceite_bpd")), 2))  # se recalcula
    .join(existe(pozos, "clave_pozo"), "clave_pozo", "left"),
    "openwell.pruebas_pozo",
    {"POZO_NO_EXISTE": F.col("_existe").isNull(),
     "SIN_ACEITE": F.col("aceite_bpd").isNull(),
     "DURACION_INVALIDA": F.coalesce("duracion_horas", F.lit(0)) <= 0,
     "FECHA_FUTURA": F.col("fecha_prueba") > F.current_date()},
    ["id_prueba"], [F.col("_fila").desc()])

df = leer("produccion/produccion_diaria.csv")
produccion = guardar(df.select(
    clave_pozo(F.col("pozo")).alias("clave_pozo"), fecha(F.col("fecha")).alias("fecha"),
    numero(F.col("aceite_bbl")).alias("aceite_bbl"), numero(F.col("agua_bbl")).alias("agua_bbl"),
    numero(F.col("gas_kpc")).alias("gas_kpc"), numero(F.col("horas_operacion")).alias("horas_operacion"),
    F.trim("reportado_por").alias("reportado_por"), F.trim("observaciones").alias("observaciones"), "_fila")
    .withColumn("bsw_pct", F.round(100 * F.col("agua_bbl") / (F.col("agua_bbl") + F.col("aceite_bbl")), 2))
    .join(existe(pozos, "clave_pozo"), "clave_pozo", "left"),
    "produccion.produccion_diaria",
    {"POZO_NO_EXISTE": F.col("_existe").isNull(),
     "FECHA_INVALIDA": F.col("fecha").isNull(),
     "VOLUMEN_NEGATIVO": (F.col("aceite_bbl") < 0) | (F.col("agua_bbl") < 0),
     "HORAS_FUERA_DE_RANGO": ~F.col("horas_operacion").between(0, 24)},
    ["clave_pozo", "fecha"], [F.col("_fila").desc()])

# %% [markdown]
# ## 5. Cuarentena

# %%
rechazos = cuarentena[0]
for c in cuarentena[1:]:
    rechazos = rechazos.unionByName(c)
spark.sql("CREATE SCHEMA IF NOT EXISTS calidad")
rechazos.write.format("delta").mode("overwrite").saveAsTable("calidad.cuarentena")
display(rechazos.groupBy("tabla", "motivo").count().orderBy("tabla", "motivo"))
