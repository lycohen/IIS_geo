-- DROP TABLE d_lnd_tables.iis_geo_test;
CREATE EXTERNAL TABLE IF NOT EXISTS d_lnd_tables.iis_geo_test (
 value string
)
STORED AS PARQUET
LOCATION 'hdfs://GALICIAHADOOP/galicia/d/landing_files/iis_geo/test';

-- DROP VIEW d_dl_views.iis_geo;
CREATE VIEW IF NOT EXISTS d_dl_views.iis_geo AS
SELECT
    cast(get_json_object(value, '$.timestamp') as timestamp) as ts
     , hour(cast(get_json_object(value, '$.timestamp') as timestamp)) as hora
     , dayofweek(cast(get_json_object(value, '$.timestamp') as timestamp)) as `dayofweek`
     , cast(get_json_object(value, '$.latitud') as float) as latitud
     , cast(get_json_object(value, '$.longitud') as float) as longitud
     , get_json_object(value, '$.country') as country
     , get_json_object(value, '$.documentnbr') as document_number
     , get_json_object(value, '$.documenttype') as document_type
     , cast(get_json_object(value, '$.accuracy') as int) as accuracy
     , get_json_object(value, '$.segmento') as segmento
     , get_json_object(value, '$.servicio') as servicio
     , get_json_object(value, '$.level') as level
     , get_json_object(value, '$.dt') as dt
FROM d_lnd_tables.iis_geo_test
WHERE
    get_json_object(value, '$.segmento') != '0'
    AND cast(get_json_object(value, '$.latitud') as float) IS NOT NULL;