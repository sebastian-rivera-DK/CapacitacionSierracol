-- =====================================================================
-- Tabla de control de cargas: carpeta "fuente" (simula SFTP o carpeta local) → Bronze
-- Motor: Warehouse de Microsoft Fabric (WH_ControlCargas)
--
-- Ejecútalo en dos pasos:
--   1) Selecciona solo la línea CREATE SCHEMA y ejecútala (debe ir sola).
--   2) Selecciona desde CREATE TABLE hasta el final del INSERT y ejecútalo.
-- =====================================================================

-- Paso 1
CREATE SCHEMA ControlCargas;

-- Paso 2
CREATE TABLE ControlCargas.TablaControlCargas (
    IdCarga             INT             NOT NULL,
    NombreProceso       VARCHAR(100)    NOT NULL,
    RutaOrigen          VARCHAR(400)    NOT NULL,   -- carpeta en el lakehouse Source (Files/...)
    PatronArchivo       VARCHAR(200)    NOT NULL,   -- nombre exacto o con comodín *
    Delimitador         VARCHAR(5)      NOT NULL,
    RutaDestino         VARCHAR(400)    NOT NULL,   -- carpeta en el lakehouse Bronze (Files/...)
    Esquema             VARCHAR(100)    NOT NULL,
    Tabla               VARCHAR(100)    NOT NULL,   -- tabla Delta de Bronze
    TipoCarga           VARCHAR(20)     NOT NULL,   -- FULL | INCREMENTAL
    Llave               VARCHAR(200)    NULL,       -- INCREMENTAL con llave = MERGE · sin llave = agrega filas
    ColumnaIncremental  VARCHAR(100)    NULL,       -- si un registro llega dos veces, gana el valor más reciente
    FechaUltimaCarga    DATETIME2(0)    NOT NULL,   -- watermark: solo archivos modificados después de esta fecha
    Activo              BIT             NOT NULL,
    Observaciones       VARCHAR(255)    NULL,
    FechaModificacion   DATETIME2(0)    NOT NULL
);

INSERT INTO ControlCargas.TablaControlCargas
    (IdCarga, NombreProceso, RutaOrigen, PatronArchivo, Delimitador, RutaDestino, Esquema, Tabla,
     TipoCarga, Llave, ColumnaIncremental, FechaUltimaCarga, Activo, Observaciones, FechaModificacion)
VALUES
    (1, 'pozos',                 'fuente/openwell',                  'pozos.csv',                   ';', 'bronze/openwell',                  'OpenWell',   'pozos',
     'FULL',        NULL,           NULL,                 '1900-01-01', 1, 'Maestro de pozos',                            '2026-11-17'),
    (2, 'equipos_bes',           'fuente/sap',                       'equipos_bes.csv',             ';', 'bronze/sap',                       'SAP',        'equipos_bes',
     'FULL',        NULL,           NULL,                 '1900-01-01', 1, 'Export SAP: encabezados con espacios',        '2026-11-17'),
    (3, 'ordenes_mantenimiento', 'fuente/sap/ordenes_mantenimiento', 'ordenes_mantenimiento_*.csv', ';', 'bronze/sap/ordenes_mantenimiento', 'SAP',        'ordenes_mantenimiento',
     'INCREMENTAL', 'numero_orden', 'fecha_modificacion', '1900-01-01', 1, 'Extracciones incrementales: MERGE por orden', '2026-11-17'),
    (4, 'pruebas_pozo',          'fuente/openwell',                  'pruebas_pozo.csv',            ';', 'bronze/openwell',                  'OpenWell',   'pruebas_pozo',
     'FULL',        NULL,           NULL,                 '1900-01-01', 1, 'Pruebas de pozo',                             '2026-11-17'),
    (5, 'produccion_diaria',     'fuente/produccion',                'produccion_diaria_*.csv',     ';', 'bronze/produccion',                'Produccion', 'produccion_diaria',
     'INCREMENTAL', NULL,           NULL,                 '1900-01-01', 1, 'Un archivo por día: se agregan los nuevos',   '2026-11-17');

-- Verificación
SELECT * FROM ControlCargas.TablaControlCargas ORDER BY IdCarga;

-- ---------------------------------------------------------------------
-- Consultas que usará el pipeline (siguientes pasos de la guía)
-- ---------------------------------------------------------------------
-- Lookup: qué cargar
-- SELECT * FROM ControlCargas.TablaControlCargas WHERE Activo = 1 ORDER BY IdCarga;

-- Al terminar bien cada objeto: mover el watermark
-- UPDATE ControlCargas.TablaControlCargas
-- SET    FechaUltimaCarga = '@{pipeline().TriggerTime}', FechaModificacion = GETDATE()
-- WHERE  IdCarga = @{item().IdCarga};

-- Reprocesar desde cero un objeto
-- UPDATE ControlCargas.TablaControlCargas SET FechaUltimaCarga = '1900-01-01' WHERE IdCarga = 5;

-- Borrar la tabla (para empezar de nuevo)
-- DROP TABLE ControlCargas.TablaControlCargas;
