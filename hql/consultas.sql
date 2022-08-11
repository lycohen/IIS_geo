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

-- hits por host
SELECT count(hits) as hits, host FROM
    (
        SELECT count(*) as hits, acc.client_id, ser.canal, concat(ser.proto, '://', ser.host) as host
        FROM d_dl_tables.iis_geo_accesos acc
                 INNER JOIN d_dl_tables.iis_geo_servicios ser on acc.servicio_id = ser.servicio_id
        GROUP BY acc.client_id, ser.canal, host ORDER BY hits DESC, acc.client_id
    ) ht GROUP BY host;
