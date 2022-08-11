INVALIDATE METADATA  d_lnd_tables.iis_geo;
INVALIDATE METADATA  d_dl_views.iis_geo;

SELECT DISTINCT `dayofweek` FROM d_dl_views.iis_geo;
SELECT count(*) as accesos, document_number, segmento FROM d_dl_views.iis_geo WHERE `dayofweek` = 3 GROUP BY document_number, segmento order by accesos DESC;
SELECT count(*), `dayofweek` FROM d_dl_views.iis_geo GROUP BY `dayofweek`;


