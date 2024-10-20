CREATE EXTENSION IF NOT EXISTS pg_partman;

SELECT CREATE_PARENT('ods.chartevents_grouped', 'charttime', 'time', 'daily');