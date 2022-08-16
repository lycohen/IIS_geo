# IIS_geo
## Planificación

## Definición de alcance

## Temática

Geolocalizacion en Streaming.

Cyberseguridad

Geolocalizacion de Eventos para Marketing

## Preguntas de negocio

¿ Puede un mismo cliente estar en 2 paises un mismo dia? 

¿Puedo venderle un seguro de viaje si detecto que no esta en el pais?



## Descripción de la solución

Geolocalizar los eventos   del Online Banking de la compañia que son canalizados a traves del streaming de eventos de  elasticseach con informacion de geolocalizacion que permita obtener operacion realizada, ubicacion con cierto nivel de precision , party id y  segmento . Todo esto en real time.
Esto nos permite proveer datos para Cyberseguridad y  Marketing respondiendo preguntas del tipo .¿ Puede un mismo cliente estar en 2 paises un mismo dia? ¿Puedo venderle un seguro de viaje si detecto que no esta en el pais?


## Diagrama de arquitectura
## Flujo de datos
## Herramienta usada en cada etapa

Kafka ,Logstash, Cloudera Data Flow (NIFI) , Pyspark , HDFS , Impala , Hive , Parquet y MaxMind Database (Geolocalizacion) .

#### Datos de entrada: tópicos, etc.
###### Topicos de entrada:

  -olbcyxtera_iis_evtlog
  
  -onb (Topico original de logstash)
#### Datos de salida: tópicos, etc.
###### Topicos de entrada:

  -cyxtera_onb_evt_geoclasificado (Topico original de logstash)
###### hdfs

  -hdfs://GALICIAHADOOP/galicia/d/landing_files/iis_geo/test

###### Tablas y vistas de Impala

d_lnd_tables.iis_geo

d_dl_views.iis_geo

d_dl_views.iis_geo_segmentos

d_dl_views.iis_geo_servicios

d_dl_views.iis_geo_clientes

d_dl_views.iis_geo_accesos_sus

#### Backlog
22/7: Seleccion del caso de uso y definicion de alcance.

3/8: NIFI : Bifurcacion de los eventos de ONB del pipeline de elastic  en NIFI y flujo inicial con un pipeline que enriquece segun IP del cliente.


https://cdh001-e02:9444/nifi/?processGroupId=7c0fee0f-017a-1000-b52f-e2f289761aa1&componentIds=13f63b61-0182-1000-e12a-49715f9beeab

https://cdh001-e01:9444/nifi/?processGroupId=77ee193d-3ab2-1026-bd0c-20ce40541a49&componentIds=b098321e-7b7a-1f3b-3007-f0e591f90c17

Acordamos extraer IP y party id.

5/8 : Configuracion del entorno de pyspark para leer streaming
Modificacion del stream de eventos para extraer el partyid

9/8: Script de pyspark 

12/8 : 

Vistas en Impala y Hive

Primer draft de Power BI.



