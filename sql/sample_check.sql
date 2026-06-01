select * from airflow.BO_CHANNEL;
select * from airflow.LV1_BO_CHANNEL where cdc_tmsp >= date'2026-5-27';

select count(1) from airflow.bda_study_source_table where disp_date>= date'2026-4-17';
select max(disp_date) from airflow.bda_study_source_table;

select disp_date, sum(disp_qty) as qty_count from airflow.bda_study_source_table group by disp_date;

select disp_qty, t.* from airflow.bda_study_source_table t where rownum <= 10 order by disp_date desc;
