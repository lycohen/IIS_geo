INVALIDATE METADATA  d_lnd_tables.iis_geo;
INVALIDATE METADATA  d_dl_views.iis_geo;
INVALIDATE METADATA d_dl_tables.iis_geo_servicios;
INVALIDATE METADATA d_dl_tables.iis_geo_segmentos;
INVALIDATE METADATA d_dl_tables.iis_geo_clientes;
INVALIDATE METADATA d_dl_tables.iis_geo_accesos;

-- DROP TABLE d_dl_views.iis_geo_segmentos
CREATE TABLE IF NOT EXISTS d_dl_tables.iis_geo_segmentos
    STORED AS PARQUET
    LOCATION 'hdfs://GALICIAHADOOP/galicia/d/landing_files/iis_geo/segmentos'
AS SELECT * FROM d_dl_views.iis_geo_segmentos;

-- DROP TABLE d_dl_views.iis_geo_servicios
CREATE TABLE IF NOT EXISTS d_dl_tables.iis_geo_servicios
    STORED AS PARQUET
    LOCATION 'hdfs://GALICIAHADOOP/galicia/d/landing_files/iis_geo/servicios'
AS SELECT * FROM d_dl_views.iis_geo_servicios;

-- DROP TABLE d_dl_views.iis_geo_clientes
CREATE TABLE IF NOT EXISTS d_dl_tables.iis_geo_clientes
    STORED AS PARQUET
    LOCATION 'hdfs://GALICIAHADOOP/galicia/d/landing_files/iis_geo/clientes'
AS SELECT * FROM d_dl_views.iis_geo_clientes;


-- DROP VIEW d_dl_views.iis_geo_accesos
CREATE VIEW IF NOT EXISTS d_dl_views.iis_geo_accesos AS
SELECT raw.ts
, raw.hora
, raw.`day`
, raw.`dayofweek`
, raw.accuracy
, raw.latitud
, raw.longitud
, raw.country
, cli.client_id
, seg.segmento
, ser.servicio_id
, ser.canal
, ser.proto
, ser.host
, raw.dt from d_dl_views.iis_geo raw
    INNER JOIN d_dl_tables.iis_geo_clientes cli
        ON cast(raw.document_number as bigint) = cast(cli.number as bigint) AND raw.document_type = cli.document_type
    INNER JOIN d_dl_tables.iis_geo_servicios ser
        ON instr(raw.servicio, ser.canal) > 0 OR instr(raw.servicio, ser.host) > 0
    INNER JOIN d_dl_tables.iis_geo_segmentos seg
        ON instr(raw.segmento, seg.segmento) > 0;


-- DROP table d_dl_tables.iis_geo_accesos;
CREATE TABLE IF NOT EXISTS d_dl_tables.iis_geo_accesos
    STORED AS PARQUET
    LOCATION 'hdfs://GALICIAHADOOP/galicia/d/landing_files/iis_geo/accesos'
AS SELECT * FROM d_dl_views.iis_geo_accesos;
