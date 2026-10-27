// Databricks notebook source
// DBTITLE 1,Model Info
import directives._
ModelInfo(team="GDIT", modelID="DemoModelAn", endDate="2024-07-31")

// COMMAND ----------

// DBTITLE 1,Import Discovery Data Table for Model Research or Ad-Hoc Analysis (Resulting Table Name: FPS_MODEL_DISCOV_GDIT.AdHocAnalysisHCPCS)
DataSourceImport(dataSourceName="AdHocAnalysisHCPCS", fullLogs=false,  modelID="DemoModelAn",team="GDIT", 
                columns="HCPCS varchar(250)",
                columnName="", columnType="", newTable=true, public=false, modelDiscov=true)

// COMMAND ----------

// DBTITLE 1,Import Model-Specific Data Table into Snowflake Warehouse (Resulting Table Name: FPS_MODELS_REF.DemoModelAn_HCPCS)
DataSourceImport(dataSourceName="HCPCS", fullLogs=false,  modelID="DemoModelAn",team="GDIT",
                columns="HCPCS varchar(250)",
                columnName="", columnType="", newTable=true, public=false)

// COMMAND ----------

// DBTITLE 1,Import Data Table to be Shared Across Models into Snowflake Warehouse (Resulting Table Name: FPS_MODELS_REF.Shared_CovidHCPCS)
DataSourceImport(dataSourceName="CovidHCPCS", fullLogs=false,  modelID="DemoModelAn",team="GDIT",
                columns="HCPCS varchar(250)",
                columnName="", columnType="", newTable=true, public=true)

// COMMAND ----------

// DBTITLE 1,(Discovery Version) COVID-19 Testing Claims Data; Limited to Providers with $20k+ in Payment (Resulting Table Name: FPS_MODEL_DISCOV_GDIT.DemoModelAn_Claim)
// Data Query #1
DataSource(dataSourceName="Claim", modelDiscov=true, query="""
with clm as(
select
  t1.clm_rndrg_prvdr_npi_num,
  t1.clm_rfrg_prvdr_npi_num,
  t1.clm_line_prvdr_pmt_amt,
  t1.clm_line_num,
  t1.clm_line_from_dt,
  t1.clm_line_hcpcs_cd,
  t1.bene_sk,
  t1.clm_cntl_num,
  t1.clm_cntrctr_num,
  t1.prvdr_mdcr_id,
  sum(clm_line_prvdr_pmt_amt) over(partition by clm_rndrg_prvdr_npi_num) as prdpmt, 
  t1.clm_pos_cd,
  t1.clm_rndrg_fed_prvdr_spclty_cd
from fps_dw.ssf_ecl t1 

inner join FPS_MODEL_DISCOV_GDIT.AdHocAnalysisHCPCS t2 on t1.clm_line_hcpcs_cd = t2.HCPCS

where t1.clm_src_type='B','A','DME' and
      t1.clm_type_cd = 2700 and
      t1.clm_line_from_dt >= dateadd(day,1,add_months(~ENDDATE~,-12)) and
      t1.clm_line_from_dt <= ~ENDDATE~ and 
      t1.clm_line_alowd_chrg_amt>0  ) 
      
select  
  t1.clm_rndrg_prvdr_npi_num as clm_rndrg_prvdr_npi_num,
  t1.clm_rfrg_prvdr_npi_num as clm_rfrg_prvdr_npi_num,
  t1.clm_line_prvdr_pmt_amt as clm_line_prvdr_pmt_amt,
  t1.clm_line_num as clm_line_num,
  t1.clm_line_from_dt as clm_line_from_dt,
  t1.clm_line_hcpcs_cd as clm_line_hcpcs_cd,
  t1.bene_sk as bene_sk,
  t1.clm_cntl_num as clm_cntl_num,
  t1.clm_cntrctr_num as clm_cntrctr_num,
  t1.prvdr_mdcr_id as PTAN,
  t1.prdpmt as prdpmt, 
  t1.clm_pos_cd as clm_pos_cd,
  t1.clm_rndrg_fed_prvdr_spclty_cd as clm_rndrg_fed_prvdr_spclty_cd, 
  t2.mac_jurisdiction as mac,
  t3.prvdr_prctc_state_upic_cd as upic

from clm t1 
     left join fps_dw.fps_npi_top_mac_by_pmt t2 
           on t1.clm_rndrg_prvdr_npi_num = t2.npi_num
     left join fps_dw.cmn_providerupic t3
           on t1.clm_rndrg_prvdr_npi_num = t3.prvdr_npi_num
where prdpmt >= 20000
""")

// COMMAND ----------

// DBTITLE 1,(ModDev Version) COVID-19 Testing Claims Data; Limited to Providers with $20k+ in Payment (Resulting Table Name: FPS_MODELS.DemoModelAn_Claim)
DataSource(dataSourceName="Claim", query="""
with clm as(
  select
  t1.clm_rndrg_prvdr_npi_num as clm_rndrg_prvdr_npi_num,
  t1.clm_rfrg_prvdr_npi_num,
  t1.clm_line_prvdr_pmt_amt,
  t1.clm_line_num,
  t1.clm_line_from_dt,
  t1.clm_line_hcpcs_cd,
  t1.bene_sk,
  t1.clm_cntl_num,
  t1.clm_cntrctr_num,
  t1.prvdr_mdcr_id,
  sum(clm_line_prvdr_pmt_amt) over(partition by clm_rndrg_prvdr_npi_num) as prdpmt, 
  t1.clm_pos_cd,
  t1.clm_rndrg_fed_prvdr_spclty_cd
from fps_dw.ssf_ecl t1 

inner join fps_models_ref.DemoModelAn_HCPCS t2 on t1.clm_line_hcpcs_cd = t2.HCPCS

inner join fps_models_ref.Shared_CovidHCPCS t3 on t1.clm_line_hcpcs_cd = t3.HCPCS

where t1.clm_src_type='B' and
      t1.clm_type_cd = 2700 and
      t1.clm_line_from_dt >= dateadd(day,1,add_months(~ENDDATE~,-12)) and
      t1.clm_line_from_dt <= ~ENDDATE~ and 
      t1.clm_line_alowd_chrg_amt>0  ) 
      
select  
  t1.clm_rndrg_prvdr_npi_num as clm_rndrg_prvdr_npi_num,
  t1.clm_rfrg_prvdr_npi_num as clm_rfrg_prvdr_npi_num,
  t1.clm_line_prvdr_pmt_amt as clm_line_prvdr_pmt_amt,
  t1.clm_line_num as clm_line_num,
  t1.clm_line_from_dt as clm_line_from_dt,
  t1.clm_line_hcpcs_cd as clm_line_hcpcs_cd,
  t1.bene_sk as bene_sk,
  t1.clm_cntl_num as clm_cntl_num,
  t1.clm_cntrctr_num as clm_cntrctr_num,
  t1.prvdr_mdcr_id as PTAN,
  t1.prdpmt as prdpmt, 
  t1.clm_pos_cd as clm_pos_cd,
  t1.clm_rndrg_fed_prvdr_spclty_cd as clm_rndrg_fed_prvdr_spclty_cd, 
  t2.mac_jurisdiction as mac,
  t3.prvdr_prctc_state_upic_cd as upic

from clm t1 
     left join fps_dw.fps_npi_top_mac_by_pmt t2 
           on t1.clm_rndrg_prvdr_npi_num = t2.npi_num
     left join fps_dw.cmn_providerupic t3
           on t1.clm_rndrg_prvdr_npi_num = t3.prvdr_npi_num
where prdpmt >= 20000
""")

// COMMAND ----------

// DBTITLE 1,(ModDev Version) Use Created DataSource as Interim Table to Create Other DataSources
DataSource(dataSourceName="refprd", query="""
select distinct t1.clm_rfrg_prvdr_npi_num as clm_rfrg_prvdr_npi_num,
       t2.prvdr_drvd_npi_name as refpnm
       from  fps_models.DemoModelAn_Claim t1 
             left join prvdr_npi t2 
             on t1.clm_rfrg_prvdr_npi_num = t2.prvdr_npi_num
             """)

// COMMAND ----------

// DBTITLE 1,DataSourceList
//DataSourceLists are no longer required as the query will run and complete in Databricks before moving to next paragraph.

// COMMAND ----------

