-- impala
INVALIDATE METADATA d_lnd_tables.iis_geo;
INVALIDATE METADATA d_dl_views.iis_geo;
INVALIDATE METADATA d_dl_tables.iis_geo_servicios;
INVALIDATE METADATA d_dl_tables.iis_geo_segmentos;
INVALIDATE METADATA d_dl_tables.iis_geo_clientes;
INVALIDATE METADATA d_dl_tables.iis_geo_accesos;

-- tabla accesos con client_id
SELECT * FROM d_dl_tables.iis_geo_accesos;

-- totales por segmento
SELECT count(*), segmento FROM d_dl_tables.iis_geo_accesos GROUP BY segmento;

-- hits clientes por servicio
SELECT count(*) as hits, acc.client_id, ser.canal, ser.host
FROM d_dl_tables.iis_geo_accesos acc
         INNER JOIN d_dl_tables.iis_geo_servicios ser on acc.servicio_id = ser.servicio_id
GROUP BY acc.client_id, ser.canal, ser.host ORDER BY hits DESC;

-- hits por canal / host
SELECT count(hits) as hits, canal, host FROM
(
    SELECT count(*) as hits, acc.client_id, ser.canal, concat(ser.proto, '://', ser.host) as host
    FROM d_dl_tables.iis_geo_accesos acc
        INNER JOIN d_dl_tables.iis_geo_servicios ser on acc.servicio_id = ser.servicio_id
    GROUP BY acc.client_id, ser.canal, host
) ht GROUP BY host, canal ORDER BY hits DESC, canal, host;

-- hits canal / host por hora
SELECT count(hits) as hits, hora, canal, host FROM
    (
        SELECT count(*) as hits, acc.hora, acc.client_id, ser.canal, concat(ser.proto, '://', ser.host) as host
        FROM d_dl_tables.iis_geo_accesos acc
                 INNER JOIN d_dl_tables.iis_geo_servicios ser on acc.servicio_id = ser.servicio_id
        GROUP BY acc.hora, acc.client_id, ser.canal, host
    ) ht GROUP BY hora, canal, host ORDER BY hora DESC, hits DESC;


-- mas de un país distinto de acceso para el mismo cliente
SELECT DISTINCT * FROM d_dl_tables.iis_geo_accesos acc
INNER JOIN d_dl_views.iis_geo_accesos_extranjero ext ON acc.client_id = ext.client_id
WHERE acc.country = 'Argentina';

-- ejemplo de acceso desde distintos países
SELECT DISTINCT country, ts from d_dl_tables.iis_geo_accesos where client_id = 'cae12536f010f2eeb562205d800bee85c30cc2ea00e3002714193b415d735cdb';

