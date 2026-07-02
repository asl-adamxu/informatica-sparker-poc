# WF_CMS_DDS_APLY_MTH

## Execution Flow

Auto-generated from Informatica PowerCenter workflow

```mermaid
flowchart TD
    n1["T_RM_CMS_CACHE_FACT"]
    style n1 fill:#ffd,stroke:#aaa,stroke-width:1px
    n2["S_CMS_FACT_PARAM_SETUP<br/><i>(M_UTL_PARAM_SETUP)</i>"]
    n3["S_CMS_ETL_FACT_TRUNCATE<br/><i>(M_UTL_DPA_TRUNCATE)</i>"]
    subgraph pg4[Parallel]
        n1
        n2
        n3
    end
    subgraph wkl5["WL_CMS_DPA_FACT_MTH_SUMMARIZE"]
    n6["S_DPA_SUMMARIZE_FACT_CMS_CASE_PRNT_SMRY<br/><i>(M_DPA_SUMMARIZE_FACT_CMS_CASE_PRNT_SMRY)</i>"]
    n7["S_DPA_SUMMARIZE_FACT_CMS_CASE_SMRY<br/><i>(M_DPA_SUMMARIZE_FACT_CMS_CASE_SMRY)</i>"]
    subgraph pg8[Parallel]
        n6
        n7
    end
    n9["S_DPA_SUMMARIZE_FACT_CMS_CASE_OSTD_SMRY<br/><i>(M_DPA_SUMMARIZE_FACT_CMS_CASE_OSTD_SMRY)</i>"]
    n10["S_DPA_SUMMARIZE_FACT_CMS_ORD_SMRY<br/><i>(M_DPA_SUMMARIZE_FACT_CMS_ORD_SMRY)</i>"]
        pg8 --> n9
        n9 --> n10
    end
    subgraph wkl11["WL_CMS_DDS_FACT_MTH_APLY"]
    n12["S_DDS_APLY_FACT_CMS_CASE_SMRY<br/><i>(M_DDS_APLY_FACT_CMS_CASE_SMRY)</i>"]
    n13["S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY<br/><i>(M_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY)</i>"]
    subgraph pg14[Parallel]
        n12
        n13
    end
    n15["S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY<br/><i>(M_DPA_APLY_FACT_CMS_CASE_OSTD_SMRY)</i>"]
    n16["S_DDS_APLY_FACT_CMS_ORD_SMRY<br/><i>(M_DDS_APLY_FACT_CMS_ORD_SMRY)</i>"]
        pg14 --> n15
        n15 --> n16
    end
    n17["T_MAIL_SUCCESS"]
    style n17 fill:#ffd,stroke:#aaa,stroke-width:1px
    pg4 --> wkl5
    wkl5 --> wkl11
    wkl11 --> n17
```

## Session to Mapping

| Session | Mapping | Plan-Level |
|---------|---------|------------|
| T_RM_CMS_CACHE_FACT | - | Top-Level |
| S_CMS_FACT_PARAM_SETUP | M_UTL_PARAM_SETUP | Top-Level |
| S_CMS_ETL_FACT_TRUNCATE | M_UTL_DPA_TRUNCATE | Top-Level |
| S_DPA_SUMMARIZE_FACT_CMS_CASE_PRNT_SMRY | M_DPA_SUMMARIZE_FACT_CMS_CASE_PRNT_SMRY | Worklet: WL_CMS_DPA_FACT_MTH_SUMMARIZE |
| S_DPA_SUMMARIZE_FACT_CMS_CASE_SMRY | M_DPA_SUMMARIZE_FACT_CMS_CASE_SMRY | Worklet: WL_CMS_DPA_FACT_MTH_SUMMARIZE |
| S_DPA_SUMMARIZE_FACT_CMS_CASE_OSTD_SMRY | M_DPA_SUMMARIZE_FACT_CMS_CASE_OSTD_SMRY | Worklet: WL_CMS_DPA_FACT_MTH_SUMMARIZE |
| S_DPA_SUMMARIZE_FACT_CMS_ORD_SMRY | M_DPA_SUMMARIZE_FACT_CMS_ORD_SMRY | Worklet: WL_CMS_DPA_FACT_MTH_SUMMARIZE |
| S_DDS_APLY_FACT_CMS_CASE_SMRY | M_DDS_APLY_FACT_CMS_CASE_SMRY | Worklet: WL_CMS_DDS_FACT_MTH_APLY |
| S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY | M_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY | Worklet: WL_CMS_DDS_FACT_MTH_APLY |
| S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY | M_DPA_APLY_FACT_CMS_CASE_OSTD_SMRY | Worklet: WL_CMS_DDS_FACT_MTH_APLY |
| S_DDS_APLY_FACT_CMS_ORD_SMRY | M_DDS_APLY_FACT_CMS_ORD_SMRY | Worklet: WL_CMS_DDS_FACT_MTH_APLY |
| T_MAIL_SUCCESS | - | Top-Level |