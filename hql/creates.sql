-- DROP TABLE d_lnd_tables.iis_geo_test;
CREATE EXTERNAL TABLE IF NOT EXISTS d_lnd_tables.iis_geo (
 value string
)
STORED AS PARQUET
LOCATION 'hdfs://GALICIAHADOOP/galicia/d/landing_files/iis_geo/test';

-- DROP VIEW d_dl_views.iis_geo;
CREATE VIEW IF NOT EXISTS d_dl_views.iis_geo AS
SELECT
    cast(get_json_object(value, '$.timestamp') as timestamp) as ts
     , hour(cast(get_json_object(value, '$.timestamp') as timestamp)) as hora
     , day(cast(get_json_object(value, '$.timestamp') as timestamp)) as `day`
     , dayofweek(cast(get_json_object(value, '$.timestamp') as timestamp)) as `dayofweek`
     , cast(get_json_object(value, '$.latitud') as double) as latitud
     , cast(get_json_object(value, '$.longitud') as double) as longitud
     , get_json_object(value, '$.country') as country
     , get_json_object(value, '$.documentnbr') as document_number
     , get_json_object(value, '$.documenttype') as document_type
     , cast(get_json_object(value, '$.accuracy') as int) as accuracy
     , get_json_object(value, '$.segmento') as segmento
     , get_json_object(value, '$.servicio') as servicio
     , get_json_object(value, '$.level') as level
     , get_json_object(value, '$.dt') as dt
FROM d_lnd_tables.iis_geo
WHERE get_json_object(value, '$.segmento') != '0'
  AND cast(get_json_object(value, '$.latitud') as float) IS NOT NULL
  AND cast(get_json_object(value, '$.longitud') as float) IS NOT NULL
  AND length(trim(get_json_object(value, '$.segmento'))) > 0;


-- DROP VIEW d_dl_views.iis_geo_segmentos;
CREATE VIEW IF NOT EXISTS d_dl_views.iis_geo_segmentos AS
SELECT distinct(trim(segmento_txt)) as segmento FROM (
    SELECT explode(split(geo.segmento,',')) as segmento_txt from d_dl_views.iis_geo geo
) seg ORDER BY segmento;


-- DROP VIEW d_dl_views.iis_geo_servicios;
CREATE VIEW IF NOT EXISTS d_dl_views.iis_geo_servicios AS
SELECT DISTINCT(servicio_id), canal, proto, host FROM
(
    SELECT sha2(concat_ws('|', canal, proto, host), 256) as servicio_id, canal, proto, host
    FROM (
        SELECT
        CASE
            WHEN length(regexp_extract(servicio, '^(http[s]?):.*\/', 0)) > 0 THEN
                'web'
            ELSE servicio
            END as canal,
        CASE
            WHEN length(regexp_extract(servicio, '^(http[s]?):.*\/', 0)) > 0 THEN
                split(regexp_extract(servicio, '^(http[s]?):.*\/', 0), '://')[0]
            ELSE ''
            END as proto,
        CASE
            WHEN length(regexp_extract(servicio, '^(http[s]?):.*\/', 0)) > 0 THEN
                split(split(regexp_extract(servicio, '^(http[s]?):.*\/', 0), '://')[1], '/')[0]
            ELSE ''
            END as host
        FROM (
            SELECT distinct(trim(servicio_txt)) as servicio FROM (
                SELECT explode(split(geo.servicio,'\\|')) as servicio_txt from d_dl_views.iis_geo geo
            ) ser WHERE length(trim(servicio_txt)) > 0 ORDER BY servicio
        ) oof
    ) abc
) canales;

-- DROP VIEW d_dl_views.iis_geo_clientes
CREATE VIEW IF NOT EXISTS d_dl_views.iis_geo_clientes AS
SELECT DISTINCT(client_id), document_type,  cast(cast(document_number as bigint) as string)
FROM (
    SELECT sha2(concat_ws('|salt_string_secreto_837673|', cast(cast(document_number as bigint) as string), document_type), 256) as client_id
    , document_type
    , document_number
    FROM d_dl_views.iis_geo
) cli;
