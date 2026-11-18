-- =====================================================================
-- Log de auditoría del control de cargas
-- Ejecútalo en dos pasos (el procedimiento debe ir solo):
--   1) CREATE OR ALTER PROCEDURE
--   2) CREATE TABLE
-- =====================================================================

-- Paso 1: procedimiento que llama el pipeline al terminar cada copia
CREATE OR ALTER PROCEDURE [ControlCargas].[PostLogAuditoriaControlCargas]
    @IdCarga                INT = NULL,
    @TipoFuente             VARCHAR(100),
    @NombreFuente           VARCHAR(350),
    @EstadoEjecucion        VARCHAR(50),
    @TiempoCopiadoSegundos  INT,
    @FechaInicio            DATETIME2(0)
AS
BEGIN
    INSERT INTO [ControlCargas].[TablaLogAuditoria]
    (
        IdCarga,
        TipoFuente,
        NombreFuente,
        EstadoEjecucion,
        TiempoCopiadoSegundos,
        FechaInicio
    )
    VALUES
    (
        @IdCarga,
        @TipoFuente,
        @NombreFuente,
        @EstadoEjecucion,
        @TiempoCopiadoSegundos,
        @FechaInicio
    );
END;

-- Paso 2: tabla de log (una fila por cada archivo que procesa el pipeline)
CREATE TABLE [ControlCargas].[TablaLogAuditoria]
(
    IdCarga                 INT             NULL,       -- IdCarga de ControlCargas.TablaControlCargas
    TipoFuente              VARCHAR(100)    NOT NULL,   -- proceso que se cargó
    NombreFuente            VARCHAR(350)    NOT NULL,   -- Esquema.Tabla
    EstadoEjecucion         VARCHAR(50)     NOT NULL,   -- Succeeded | Failed
    TiempoCopiadoSegundos   INT             NOT NULL,
    FechaInicio             DATETIME2(0)    NOT NULL
);

-- Borrar (para empezar de nuevo)
-- DROP PROCEDURE [ControlCargas].[PostLogAuditoriaControlCargas];
-- DROP TABLE [ControlCargas].[TablaLogAuditoria];
