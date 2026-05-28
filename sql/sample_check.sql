select * from airflow.BO_CHANNEL;
select * from airflow.LV1_BO_CHANNEL where cdc_tmsp >= date'2026-5-27';

select count(1) from airflow.bda_study_source_table where disp_date>= date'2026-4-17';
select max(disp_date) from airflow.bda_study_source_table;

  git config --global user.email "adamxu@aslcn.com.cn"
  git config --global user.name "adamxu"