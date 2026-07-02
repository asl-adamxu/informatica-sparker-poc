select * from airflow.BO_CHANNEL;
select * from airflow.LV1_BO_CHANNEL where cdc_tmsp >= date'2026-5-27';

select count(1) from airflow.bda_study_source_table where disp_date>= date'2026-4-17';
select max(disp_date) from airflow.bda_study_source_table;

select disp_date, sum(disp_qty) as qty_count from airflow.bda_study_source_table group by disp_date;

select disp_qty, t.* from airflow.bda_study_source_table t where rownum <= 10 order by disp_date desc;

select * from airflow.SOR_SYS_PRPTY;
SELECT * FROM  AIRFLOW.DPA_FACT_GMS_DLY_MSD_INCDT;
SELECT * FROM  AIRFLOW.DDS_FACT_GMS_DLY_MSD_INCDT;
SELECT * FROM  AIRFLOW.DPA_FACT_GMS_DLY_MSD_SMRY;
SELECT * FROM  AIRFLOW.DDS_FACT_GMS_DLY_MSD_SMRY;
SELECT * FROM  AIRFLOW.DPA_FACT_GMS_DLY_DOG_RGSTR;
SELECT * FROM  AIRFLOW.DDS_FACT_GMS_DLY_DOG_RGSTR;
SELECT * FROM  AIRFLOW.PKG_CDI_TRUNCATE_LOG ORDER BY TRUNCATED_AT DESC;

SELECT SOR_SYS_PRPTY.VAL as VAL, SOR_SYS_PRPTY.PRPTY_DESP as PRPTY_DESP, SOR_SYS_PRPTY.PRPTY as PRPTY 
FROM airflow.SOR_SYS_PRPTY;

select * from DDS_DMNS_HSHLD_SIZE;

-- M_DPA_SUMMARIZE_FACT_CMS_CASE_SMRY        
select * from DPA_FACT_CMS_CASE_SMRY;
-- M_DPA_SUMMARIZE_FACT_CMS_CASE_OSTD_SMRY   
select * from DPA_FACT_CMS_CASE_OSTD_SMRY;
-- M_DPA_SUMMARIZE_FACT_CMS_ORD_SMRY         
select * from DPA_FACT_CMS_ORD_SMRY;
-- M_DPA_SUMMARIZE_FACT_CMS_CASE_PRNT_SMRY   
select * from DPA_FACT_CMS_CASE_PRNT_SMRY order by LAST_REC_TXN_DATE DESC;
-- M_DDS_APLY_FACT_CMS_CASE_SMRY             
select * from DDS_FACT_CMS_CASE_SMRY;
-- M_DPA_APLY_FACT_CMS_CASE_OSTD_SMRY        
select * from DDS_FACT_CMS_CASE_OSTD_SMRY;
-- M_DDS_APLY_FACT_CMS_ORD_SMRY              
select * from DDS_FACT_CMS_ORD_SMRY;
-- M_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY        
select * from DDS_FACT_CMS_CASE_PRNT_SMRY order by LAST_REC_TXN_DATE DESC;
