# %% [markdown]
# # nb_Transformacion · Bronze → Silver
#
# Lee los 5 CSV que el pipeline de extracción dejó en **Bronze** (`Files/bronze/...`), los **valida y limpia**
# y escribe las tablas Delta en **Silver**.
#
# **Antes de ejecutar:** agrega el lakehouse **Silver** como lakehouse predeterminado del notebook
# (panel *Explorer* → *Add data items* → *Existing data sources* → `Silver`).
#
# | Tipo de regla | Qué hace | Ejemplo |
# |---|---|---|
# | **Corrección** | arregla el dato y sigue | `"245,5"` → `245.5`; `"MORICHAL 1"` → `MOR-001` |
# | **Rechazo** | el registro no pasa a Silver: va a **cuarentena** con el motivo | aceite negativo |
# | **Advertencia** | el registro pasa, pero queda marcado | pozo sin coordenadas |
# | **Duplicado** | si un registro llega más de una vez, se conserva uno | la misma orden en dos versiones |
#
# Resultado en Silver:
#
# | Esquema | Tablas |
# |---|---|
# | `openwell` | `pozos`, `pruebas_pozo` |
# | `sap` | `equipos_bes`, `ordenes_mantenimiento` |
# | `produccion` | `produccion_diaria` |
# | `calidad` | `cuarentena`, `catalogo_reglas`, `resumen_calidad` |

# %% [markdown] PARAMETROS
# ### ⚙️ Parámetros

# %% PARAMETROS
WORKSPACE = ""        # nombre del workspace; vacío = el workspace donde corre el notebook
LH_BRONZE = "Bronze"  # lakehouse donde el pipeline dejó los archivos

# %% [markdown]
# ## 1. Rutas y lectura de los archivos de Bronze
# Todo se lee como **texto**: interpretar los datos es trabajo de las reglas de esta capa.
# Los encabezados se normalizan (`"ID EQUIPO"` → `id_equipo`).

# %%
import re
import unicodedata
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

if not WORKSPACE:
    try:
        WORKSPACE = notebookutils.runtime.context["currentWorkspaceName"]
    except Exception:
        raise ValueError("No se pudo leer el nombre del workspace: escríbelo en el parámetro WORKSPACE")

# Misma información que ControlCargas.TablaControlCargas (RutaDestino, NombreArchivo, Esquema, Tabla)
FUENTES = {
    "pozos":                 ("bronze/openwell",   "pozos.csv",                 "openwell",   "pozos"),
    "equipos_bes":           ("bronze/sap",        "equipos_bes.csv",           "sap",        "equipos_bes"),
    "ordenes_mantenimiento": ("bronze/sap",        "ordenes_mantenimiento.csv", "sap",        "ordenes_mantenimiento"),
    "pruebas_pozo":          ("bronze/openwell",   "pruebas_pozo.csv",          "openwell",   "pruebas_pozo"),
    "produccion_diaria":     ("bronze/produccion", "produccion_diaria.csv",     "produccion", "produccion_diaria"),
}


def ruta_bronze(carpeta, archivo):
    return f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LH_BRONZE}.Lakehouse/Files/{carpeta}/{archivo}"


def normalizar_nombre(col):
    col = col.replace("﻿", "").strip().lower()
    col = unicodedata.normalize("NFKD", col).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", col).strip("_")


def leer_bronze(objeto):
    carpeta, archivo, _, _ = FUENTES[objeto]
    df = (spark.read.option("header", True).option("sep", ";").option("inferSchema", False)
          .csv(ruta_bronze(carpeta, archivo)))
    df = df.toDF(*[normalizar_nombre(c) for c in df.columns])
    return (df.withColumn("_archivo_origen", F.lit(f"{carpeta}/{archivo}"))
              .withColumn("_orden", F.monotonically_increasing_id()))   # orden de llegada dentro del archivo


def guardar(df, esquema, tabla, modo="overwrite"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esquema}")
    (df.write.format("delta").mode(modo).option("overwriteSchema", "true").option("mergeSchema", "true")
       .saveAsTable(f"{esquema}.{tabla}"))

# %% [markdown]
# ## 2. Funciones de limpieza

# %%
def vacio_a_nulo(c):
    return F.when(c != "", c)


def texto(col):
    """Quita espacios sobrantes y tildes, y pasa a mayúsculas."""
    s = F.upper(F.regexp_replace(F.trim(col), r"\s+", " "))
    return vacio_a_nulo(F.translate(s, "ÁÉÍÓÚÑ", "AEIOUN"))


def numero(col):
    """'245,5' → 245.5 · '8828 ft' → 8828 · '' → null."""
    s = F.regexp_replace(F.regexp_replace(F.trim(col), ",", "."), r"[^0-9.\-]", "")
    return vacio_a_nulo(s).cast("double")


def pesos(col):
    """'$ 12.500.000' · '12.500.000,00' · '12500000' → 12500000."""
    s = F.regexp_replace(F.trim(col), r",\d{1,2}$", "")
    return vacio_a_nulo(F.regexp_replace(s, r"[^0-9]", "")).cast("decimal(18,2)")


def fecha(col):
    """Acepta yyyy-MM-dd, dd/MM/yyyy y yyyyMMdd (formato SAP)."""
    c = F.trim(col)
    return (F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(F.substring(c, 1, 10), "yyyy-MM-dd"))
             .when(c.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(c, "dd/MM/yyyy"))
             .when(c.rlike(r"^\d{8}$"), F.to_date(c, "yyyyMMdd")))


PATRON_POZO = r"^([A-Z]+)[^A-Z0-9]*0*([0-9]+)$"


def clave_pozo(col):
    """Homologa las formas de escribir un pozo a una sola llave: LLL-NNN.
    CO-LLA-MOR-0001 (OpenWell) · CO-LLA-MORICHAL-001 (SAP) · MORICHAL 1 · ' mor-1 '  →  MOR-001"""
    s = F.regexp_replace(texto(col), r"^CO-LLA-", "")
    letras, num = F.regexp_extract(s, PATRON_POZO, 1), F.regexp_extract(s, PATRON_POZO, 2)
    return F.when((letras != "") & (num != ""), F.concat(F.substring(letras, 1, 3), F.lit("-"), F.lpad(num, 3, "0")))


display(spark.createDataFrame([("CO-LLA-MOR-0001",), ("CO-LLA-MORICHAL-001",), ("MORICHAL 1",), (" mor-1 ",),
                               ("MOR-099",), (None,)], "original string")
        .withColumn("clave_pozo", clave_pozo(F.col("original"))))

# %% [markdown]
# ## 3. Motor de reglas
# Cada regla es una fila: `(id, descripción, severidad, condición que indica el problema)`.
# Así las reglas se leen como una tabla y se pueden documentar o mover a configuración más adelante.

# %%
CATALOGO, CUARENTENA, HALLAZGOS, RESULTADOS = [], [], [], []


def corregir(objeto, regla_id, descripcion):
    """Documenta una regla de corrección (la corrección está en el código de cada objeto)."""
    CATALOGO.append((objeto, regla_id, descripcion, "CORRECCION"))


def validar(objeto, df, reglas, id_col):
    for rid, desc, sev, _ in reglas:
        CATALOGO.append((objeto, rid, desc, sev))
    rech = [F.when(cond, F.lit(rid)) for rid, _, sev, cond in reglas if sev == "RECHAZO"]
    avis = [F.when(cond, F.lit(rid)) for rid, _, sev, cond in reglas if sev == "ADVERTENCIA"]
    vacio = F.array().cast("array<string>")
    df = (df.withColumn("_rechazos", F.filter(F.array(*rech), lambda x: x.isNotNull()) if rech else vacio)
            .withColumn("_avisos", F.filter(F.array(*avis), lambda x: x.isNotNull()) if avis else vacio))
    a_cuarentena(objeto, df.where(F.size("_rechazos") > 0), id_col)
    HALLAZGOS.append(df.select(F.lit(objeto).alias("objeto"), F.explode(F.concat("_rechazos", "_avisos")).alias("regla")))
    return (df.where(F.size("_rechazos") == 0)
              .withColumn("advertencias", vacio_a_nulo(F.concat_ws(", ", "_avisos")))
              .drop("_rechazos", "_avisos"))


def deduplicar(objeto, df, llaves, orden, id_col, regla_id):
    CATALOGO.append((objeto, regla_id, f"Registro repetido por {', '.join(llaves)}: se conserva el más reciente", "RECHAZO"))
    df = df.withColumn("_n", F.row_number().over(Window.partitionBy(*llaves).orderBy(*orden)))
    repetidos = df.where("_n > 1").withColumn("_rechazos", F.array(F.lit(regla_id)))
    a_cuarentena(objeto, repetidos, id_col)
    HALLAZGOS.append(repetidos.select(F.lit(objeto).alias("objeto"), F.lit(regla_id).alias("regla")))
    return df.where("_n = 1").drop("_n")


def a_cuarentena(objeto, df, id_col):
    datos = [c for c in df.columns if not c.startswith("_") and c != "advertencias"]
    CUARENTENA.append(df.select(
        F.lit(objeto).alias("objeto"), F.col(id_col).cast("string").alias("id_registro"),
        F.concat_ws(", ", "_rechazos").alias("reglas"), F.to_json(F.struct(*datos)).alias("registro"),
        "_archivo_origen"))


def escribir_silver(objeto, df_bronze, df_silver):
    _, _, esquema, tabla = FUENTES[objeto]
    df_silver = df_silver.drop("_orden").withColumn("_fecha_carga", F.current_timestamp())
    guardar(df_silver, esquema, tabla)
    n_in, n_out = df_bronze.count(), df_silver.count()
    n_avisos = df_silver.where("advertencias IS NOT NULL").count()
    RESULTADOS.append((f"{esquema}.{tabla}", n_in, n_out, n_in - n_out, n_avisos))
    print(f"{esquema}.{tabla}: Bronze={n_in} · Silver={n_out} · rechazadas={n_in - n_out} · con advertencia={n_avisos}")


def existe(df_ref, llave, alias):
    return df_ref.select(F.col(llave), F.lit(True).alias(alias)).distinct()

# %% [markdown]
# ## 4. Pozos (OpenWell)
#
# | Regla | Tipo | Qué atiende |
# |---|---|---|
# | POZ-C1 | Corrección | campo escrito con variantes (`Morichal`, `MORICHAL `, `morichal`) |
# | POZ-C2 | Corrección | latitud y longitud invertidas |
# | POZ-C3 | Corrección | fechas en dos formatos y profundidad con texto (`8828 ft`) |
# | POZ-01 | Rechazo | identificador de pozo inválido |
# | POZ-02 | Rechazo | coordenadas fuera de Colombia |
# | POZ-03 | Advertencia | coordenadas vacías |
# | POZ-04 | Advertencia | coordenadas que venían invertidas |
# | POZ-05 | Advertencia | método de levantamiento sin dato |
# | POZ-DUP | Rechazo | el mismo pozo dos veces: gana la actualización más reciente |

# %%
b_pozos = leer_bronze("pozos")
lat, lon = numero(F.col("latitud")), numero(F.col("longitud"))
invertidas = (lat < -60) & (lon > -20)

pozo = b_pozos.select(
    clave_pozo(F.col("id_pozo")).alias("clave_pozo"),
    texto(F.col("id_pozo")).alias("uwi"),
    F.regexp_replace(F.trim("nombre_pozo"), r"\s+", " ").alias("nombre_pozo"),
    texto(F.col("campo")).alias("campo"),
    F.when(invertidas, lon).otherwise(lat).alias("latitud"),
    F.when(invertidas, lat).otherwise(lon).alias("longitud"),
    invertidas.alias("_invertidas"),
    texto(F.col("estado")).alias("estado"),
    F.coalesce(texto(F.col("metodo_levantamiento")), F.lit("SIN DATO")).alias("metodo_levantamiento"),
    fecha(F.col("fecha_spud")).alias("fecha_spud"),
    numero(F.col("profundidad_ft")).cast("int").alias("profundidad_ft"),
    fecha(F.col("fecha_actualizacion")).alias("fecha_actualizacion"),
    "_archivo_origen", "_orden")
corregir("pozos", "POZ-C1", "Campo homologado a mayúsculas sin espacios ni tildes")
corregir("pozos", "POZ-C2", "Latitud y longitud invertidas se intercambian")
corregir("pozos", "POZ-C3", "Fechas dd/MM/yyyy y profundidad con texto se convierten")

pozo = validar("pozos", pozo, [
    ("POZ-01", "Identificador de pozo inválido", "RECHAZO", F.col("clave_pozo").isNull()),
    ("POZ-02", "Coordenadas fuera de Colombia", "RECHAZO",
     ~F.col("latitud").between(-4.3, 13.5) | ~F.col("longitud").between(-82, -66)),
    ("POZ-03", "Coordenadas vacías", "ADVERTENCIA", F.col("latitud").isNull() | F.col("longitud").isNull()),
    ("POZ-04", "Coordenadas venían invertidas (corregidas)", "ADVERTENCIA", F.col("_invertidas")),
    ("POZ-05", "Método de levantamiento sin dato", "ADVERTENCIA", F.col("metodo_levantamiento") == "SIN DATO"),
], "uwi")
pozo = deduplicar("pozos", pozo, ["clave_pozo"], [F.col("fecha_actualizacion").desc()], "uwi", "POZ-DUP").drop("_invertidas")
escribir_silver("pozos", b_pozos, pozo)
display(pozo.where("advertencias IS NOT NULL").select("clave_pozo", "campo", "latitud", "longitud", "advertencias"))

# %% [markdown]
# ## 5. Equipos BES (SAP)
#
# | Regla | Tipo | Qué atiende |
# |---|---|---|
# | EQU-C1 | Corrección | fabricante con variantes (`FABRICANTE A`, `Fab. A`) |
# | EQU-C2 | Corrección | la ubicación técnica SAP se homologa a `clave_pozo`; potencia 0 → vacía |
# | EQU-01 | Rechazo | el pozo no existe en el maestro (integridad referencial) |
# | EQU-02 | Rechazo | fecha de instalación futura o inválida |
# | EQU-03 | Advertencia | potencia sin dato |
# | EQU-DUP | Rechazo | el mismo equipo dos veces |

# %%
b_equipos = leer_bronze("equipos_bes")
hp = numero(F.col("potencia_hp"))
equipo = (b_equipos.select(
              F.trim("id_equipo").alias("id_equipo"),
              clave_pozo(F.col("ubicacion_tecnica")).alias("clave_pozo"),
              texto(F.col("tipo")).alias("tipo_equipo"),
              F.initcap(F.regexp_replace(texto(F.col("fabricante")), r"^FAB\.? ", "FABRICANTE ")).alias("fabricante"),
              F.trim("modelo").alias("modelo"),
              F.when(hp > 0, hp).alias("potencia_hp"),
              fecha(F.col("fecha_instalacion")).alias("fecha_instalacion"),
              texto(F.col("estado")).alias("estado"),
              "_archivo_origen", "_orden")
          .join(existe(pozo, "clave_pozo", "_pozo_existe"), "clave_pozo", "left"))
corregir("equipos_bes", "EQU-C1", "Fabricante homologado (FABRICANTE A, Fab. A → Fabricante A)")
corregir("equipos_bes", "EQU-C2", "Ubicación técnica SAP → clave_pozo; potencia 0 → vacía")

equipo = validar("equipos_bes", equipo, [
    ("EQU-01", "El pozo no existe en el maestro de pozos", "RECHAZO", F.col("_pozo_existe").isNull()),
    ("EQU-02", "Fecha de instalación futura o inválida", "RECHAZO",
     F.col("fecha_instalacion").isNull() | (F.col("fecha_instalacion") > F.current_date())),
    ("EQU-03", "Potencia sin dato", "ADVERTENCIA", F.col("potencia_hp").isNull()),
], "id_equipo")
equipo = deduplicar("equipos_bes", equipo, ["id_equipo"], [F.col("_orden").desc()], "id_equipo", "EQU-DUP").drop("_pozo_existe")
escribir_silver("equipos_bes", b_equipos, equipo)
display(equipo.groupBy("fabricante").count())

# %% [markdown]
# ## 6. Órdenes de mantenimiento (SAP PM)
#
# | Regla | Tipo | Qué atiende |
# |---|---|---|
# | ORD-C1 | Corrección | estados con variantes (`TECO`, `Cerrada`, `CERRADO`) → Creada / Liberada / Cerrada |
# | ORD-C2 | Corrección | costo en formatos `$ 12.500.000`, `12.500.000,00` |
# | ORD-01 | Rechazo | el equipo no existe (o fue rechazado en Silver) |
# | ORD-02 | Rechazo | fecha de fin anterior a la de inicio |
# | ORD-03 | Rechazo | estado no reconocido |
# | ORD-04 | Advertencia | orden cerrada sin costo |
# | ORD-DUP | Rechazo | la misma orden en dos versiones: gana la última modificación |

# %%
b_ordenes = leer_bronze("ordenes_mantenimiento")
est = texto(F.col("estado"))
tipo = texto(F.col("tipo_orden"))
orden = (b_ordenes.select(
             F.trim("numero_orden").alias("numero_orden"),
             F.trim("id_equipo").alias("id_equipo"),
             F.when(tipo == "PM01", "Correctivo").when(tipo == "PM02", "Preventivo")
              .when(tipo == "PM03", "Predictivo").alias("tipo_orden"),
             F.trim("descripcion").alias("descripcion"),
             fecha(F.col("fecha_inicio")).alias("fecha_inicio"),
             fecha(F.col("fecha_fin")).alias("fecha_fin"),
             F.when(est.isin("CRTD", "CREADA"), "Creada")
              .when(est.isin("REL", "LIBERADA"), "Liberada")
              .when(est.isin("TECO", "CERRADA", "CERRADO"), "Cerrada").alias("estado"),
             pesos(F.col("costo_cop")).alias("costo_cop"),
             F.to_timestamp("fecha_modificacion").alias("fecha_modificacion"),
             "_archivo_origen", "_orden")
         .join(existe(equipo, "id_equipo", "_equipo_existe"), "id_equipo", "left"))
corregir("ordenes_mantenimiento", "ORD-C1", "Estados SAP y texto libre → Creada / Liberada / Cerrada")
corregir("ordenes_mantenimiento", "ORD-C2", "Costo con $, puntos de miles y decimales → número")

orden = validar("ordenes_mantenimiento", orden, [
    ("ORD-01", "El equipo no existe (o fue rechazado en Silver)", "RECHAZO", F.col("_equipo_existe").isNull()),
    ("ORD-02", "Fecha de fin anterior a la de inicio", "RECHAZO", F.col("fecha_fin") < F.col("fecha_inicio")),
    ("ORD-03", "Estado no reconocido", "RECHAZO", F.col("estado").isNull()),
    ("ORD-04", "Orden cerrada sin costo", "ADVERTENCIA", (F.col("estado") == "Cerrada") & F.col("costo_cop").isNull()),
], "numero_orden")
orden = deduplicar("ordenes_mantenimiento", orden, ["numero_orden"],
                   [F.col("fecha_modificacion").desc(), F.col("_orden").desc()], "numero_orden", "ORD-DUP").drop("_equipo_existe")
escribir_silver("ordenes_mantenimiento", b_ordenes, orden)

# %% [markdown]
# ## 7. Pruebas de pozo (OpenWell)
#
# | Regla | Tipo | Qué atiende |
# |---|---|---|
# | PRU-C1 | Corrección | pozo escrito con variantes; el BSW se **recalcula** con agua / (agua + aceite) |
# | PRU-01 | Rechazo | el pozo no existe |
# | PRU-02 | Rechazo | prueba sin aceite medido |
# | PRU-03 | Rechazo | duración de la prueba 0 o vacía |
# | PRU-04 | Rechazo | fecha de prueba futura |
# | PRU-05 | Advertencia | el BSW reportado no coincide con el calculado (se usa el calculado) |
# | PRU-DUP | Rechazo | la misma prueba dos veces |

# %%
b_pruebas = leer_bronze("pruebas_pozo")
prueba = (b_pruebas.select(
              F.trim("id_prueba").alias("id_prueba"),
              clave_pozo(F.col("pozo")).alias("clave_pozo"),
              fecha(F.col("fecha_prueba")).alias("fecha_prueba"),
              numero(F.col("duracion_horas")).alias("duracion_horas"),
              numero(F.col("aceite_bpd")).alias("aceite_bpd"),
              numero(F.col("agua_bpd")).alias("agua_bpd"),
              numero(F.col("gas_kpcd")).alias("gas_kpcd"),
              numero(F.col("bsw_pct")).alias("bsw_reportado_pct"),
              numero(F.col("presion_cabeza_psi")).alias("presion_cabeza_psi"),
              "_archivo_origen", "_orden")
          .withColumn("bsw_pct", F.round(100 * F.col("agua_bpd") / (F.col("agua_bpd") + F.col("aceite_bpd")), 2))
          .join(existe(pozo, "clave_pozo", "_pozo_existe"), "clave_pozo", "left"))
corregir("pruebas_pozo", "PRU-C1", "Pozo homologado a clave_pozo; BSW recalculado con agua y aceite")

prueba = validar("pruebas_pozo", prueba, [
    ("PRU-01", "El pozo no existe en el maestro", "RECHAZO", F.col("_pozo_existe").isNull()),
    ("PRU-02", "Prueba sin aceite medido", "RECHAZO", F.col("aceite_bpd").isNull()),
    ("PRU-03", "Duración de la prueba 0 o vacía", "RECHAZO", F.coalesce(F.col("duracion_horas"), F.lit(0)) <= 0),
    ("PRU-04", "Fecha de prueba futura", "RECHAZO", F.col("fecha_prueba") > F.current_date()),
    ("PRU-05", "BSW reportado distinto al calculado (se usa el calculado)", "ADVERTENCIA",
     F.abs(F.col("bsw_reportado_pct") - F.col("bsw_pct")) > 1),
], "id_prueba")
prueba = deduplicar("pruebas_pozo", prueba, ["id_prueba"], [F.col("_orden").desc()], "id_prueba", "PRU-DUP").drop("_pozo_existe")
escribir_silver("pruebas_pozo", b_pruebas, prueba)

# %% [markdown]
# ## 8. Producción diaria
#
# | Regla | Tipo | Qué atiende |
# |---|---|---|
# | PRO-C1 | Corrección | pozo con variantes y números con coma decimal (`245,5`) |
# | PRO-01 | Rechazo | el pozo no existe en el maestro (`MOR-099`) |
# | PRO-02 | Rechazo | volumen negativo |
# | PRO-03 | Rechazo | horas de operación fuera de 0–24 |
# | PRO-04 | Rechazo | fecha inválida o vacía |
# | PRO-DUP | Rechazo | mismo pozo y fecha más de una vez: gana la última fila del archivo |

# %%
b_prod = leer_bronze("produccion_diaria")
prod = (b_prod.select(
            clave_pozo(F.col("pozo")).alias("clave_pozo"),
            fecha(F.col("fecha")).alias("fecha"),
            numero(F.col("aceite_bbl")).alias("aceite_bbl"),
            numero(F.col("agua_bbl")).alias("agua_bbl"),
            numero(F.col("gas_kpc")).alias("gas_kpc"),
            numero(F.col("horas_operacion")).alias("horas_operacion"),
            F.trim("reportado_por").alias("reportado_por"),
            vacio_a_nulo(F.trim("observaciones")).alias("observaciones"),
            F.col("pozo").alias("pozo_original"),
            "_archivo_origen", "_orden")
        .join(existe(pozo, "clave_pozo", "_pozo_existe"), "clave_pozo", "left"))
corregir("produccion_diaria", "PRO-C1", "Pozo homologado a clave_pozo; coma decimal → punto")

prod = validar("produccion_diaria", prod, [
    ("PRO-01", "El pozo no existe en el maestro", "RECHAZO", F.col("_pozo_existe").isNull()),
    ("PRO-02", "Volumen negativo", "RECHAZO",
     (F.col("aceite_bbl") < 0) | (F.col("agua_bbl") < 0) | (F.col("gas_kpc") < 0)),
    ("PRO-03", "Horas de operación fuera de 0–24", "RECHAZO", ~F.col("horas_operacion").between(0, 24)),
    ("PRO-04", "Fecha inválida o vacía", "RECHAZO", F.col("fecha").isNull()),
], "pozo_original")
prod = (deduplicar("produccion_diaria", prod, ["clave_pozo", "fecha"], [F.col("_orden").desc()], "pozo_original", "PRO-DUP")
        .drop("_pozo_existe", "pozo_original")
        .withColumn("bsw_pct", F.round(100 * F.col("agua_bbl") / (F.col("agua_bbl") + F.col("aceite_bbl")), 2)))
escribir_silver("produccion_diaria", b_prod, prod)
display(prod.groupBy("fecha").agg(F.count("*").alias("pozos"), F.round(F.sum("aceite_bbl"), 0).alias("aceite_bbl"))
        .orderBy("fecha"))

# %% [markdown]
# ## 9. Cuarentena, catálogo de reglas y resumen de calidad

# %%
cuarentena = CUARENTENA[0]
for c in CUARENTENA[1:]:
    cuarentena = cuarentena.unionByName(c)
guardar(cuarentena.withColumn("fecha_carga", F.current_timestamp()), "calidad", "cuarentena")

catalogo = spark.createDataFrame(CATALOGO, "objeto string, regla string, descripcion string, severidad string").distinct()
guardar(catalogo, "calidad", "catalogo_reglas")

hallazgos = HALLAZGOS[0]
for h in HALLAZGOS[1:]:
    hallazgos = hallazgos.unionByName(h)
resumen = (hallazgos.groupBy("objeto", "regla").agg(F.count("*").alias("filas"))
           .join(catalogo, ["objeto", "regla"], "left")
           .select(F.current_timestamp().alias("fecha_ejecucion"), "objeto", "regla", "descripcion", "severidad", "filas"))
guardar(resumen, "calidad", "resumen_calidad", modo="append")   # se acumula para ver la evolución

display(resumen.select("objeto", "regla", "severidad", "descripcion", "filas").orderBy("objeto", "regla"))

# %%
display(spark.createDataFrame(RESULTADOS, "tabla string, filas_bronze long, filas_silver long, rechazadas long, con_advertencia long"))
