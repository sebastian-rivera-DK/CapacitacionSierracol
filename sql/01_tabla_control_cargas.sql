-- =====================================================================
-- Tabla de control de cargas: carpeta "fuente" (simula SFTP o carpeta local) → Bronze
-- Motor: Warehouse de Microsoft Fabric (WH_ControlCargas)
-- Ejecútalo en dos pasos:
--   1) CREATE SCHEMA (debe ir solo)
--   2) CREATE TABLE + INSERT
-- =====================================================================

-- Paso 1
CREATE SCHEMA ControlCargas;

-- Paso 2
CREATE TABLE ControlCargas.TablaControlCargas (
    IdCarga             INT             NOT NULL,
    NombreProceso       VARCHAR(100)    NOT NULL,
    RutaOrigen          VARCHAR(400)    NOT NULL,   -- carpeta en el lakehouse Source (Files/...)
    NombreArchivo       VARCHAR(200)    NOT NULL,
    Delimitador         VARCHAR(5)      NOT NULL,
    RutaDestino         VARCHAR(400)    NOT NULL,   -- carpeta en el lakehouse Bronze (Files/...)
    Esquema             VARCHAR(100)    NOT NULL,
    Tabla               VARCHAR(100)    NOT NULL,   -- tabla Delta de Bronze (se reemplaza completa)
    Activo              BIT             NOT NULL,
    Observaciones       VARCHAR(255)    NULL,
    FechaModificacion   DATETIME2(0)    NOT NULL
);

INSERT INTO ControlCargas.TablaControlCargas
    (IdCarga, NombreProceso, RutaOrigen, NombreArchivo, Delimitador, RutaDestino, Esquema, Tabla, Activo, Observaciones, FechaModificacion)
VALUES
    (1, 'pozos',                 'fuente/openwell',   'pozos.csv',                 ';', 'bronze/openwell',   'OpenWell',   'pozos',                 1, 'Maestro de pozos',                     '2026-11-17'),
    (2, 'equipos_bes',           'fuente/sap',        'equipos_bes.csv',           ';', 'bronze/sap',        'SAP',        'equipos_bes',           1, 'Export SAP: encabezados con espacios', '2026-11-17'),
    (3, 'ordenes_mantenimiento', 'fuente/sap',        'ordenes_mantenimiento.csv', ';', 'bronze/sap',        'SAP',        'ordenes_mantenimiento', 1, 'Órdenes de mantenimiento SAP PM',      '2026-11-17'),
    (4, 'pruebas_pozo',          'fuente/openwell',   'pruebas_pozo.csv',          ';', 'bronze/openwell',   'OpenWell',   'pruebas_pozo',          1, 'Pruebas de pozo',                      '2026-11-17'),
    (5, 'produccion_diaria',     'fuente/produccion', 'produccion_diaria.csv',     ';', 'bronze/produccion', 'Produccion', 'produccion_diaria',     1, 'Producción diaria por pozo (4 días)',  '2026-11-17');

-- Verificación: deben aparecer 5 filas
SELECT * FROM ControlCargas.TablaControlCargas WHERE Activo = 1 ORDER BY IdCarga;

-- Borrar la tabla (para empezar de nuevo)
-- DROP TABLE ControlCargas.TablaControlCargas;
