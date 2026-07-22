# WF_GMS_DDS_APLY_DLY

## Execution Flow

Auto-generated from Informatica PowerCenter workflow

```mermaid
flowchart TD
    n1["S_GMS_ETL_DPA_TRUNCATE<br/><i>(M_UTL_DPA_TRUNCATE)</i>"]
    n2["S_GMS_ETL_PARAM_SETUP<br/><i>(M_UTL_PARAM_SETUP)</i>"]
    subgraph pg3[Parallel]
        n1
        n2
    end
    subgraph wkl4["WL_GMS_DDS_SUM"]
    n5["S_DPA_SUM_FACT_GMS_DLY_MSD_SMRY<br/><i>(M_DPA_SUM_FACT_GMS_DLY_MSD_SMRY)</i>"]
    n6["S_DPA_SUM_FACT_GMS_DLY_DOG_RGSTR<br/><i>(M_DPA_SUM_FACT_GMS_DLY_DOG_RGSTR)</i>"]
    n7["S_DPA_SUM_FACT_GMS_DLY_MSD_INCDT<br/><i>(M_DPA_SUM_FACT_GMS_DLY_MSD_INCDT)</i>"]
    subgraph pg8[Parallel]
        n5
        n6
        n7
    end
    end
    subgraph wkl9["WL_GMS_DDS_APLY"]
    n10["S_DDS_APL_FACT_GMS_DLY_DOG_RGSTR<br/><i>(M_DDS_APL_FACT_GMS_DLY_DOG_RGSTR)</i>"]
    n11["S_DDS_APL_FACT_GMS_DLY_MSD_SMRY<br/><i>(M_DDS_APL_FACT_GMS_DLY_MSD_SMRY)</i>"]
    n12["S_DDS_APL_FACT_GMS_DLY_MSD_INCDT<br/><i>(M_DDS_APL_FACT_GMS_DLY_MSD_INCDT)</i>"]
    subgraph pg13[Parallel]
        n10
        n11
        n12
    end
    end
    n14["T_MAIL_SUCCESS"]
    style n14 fill:#ffd,stroke:#aaa,stroke-width:1px
    pg3 --> wkl4
    wkl4 --> wkl9
    wkl9 --> n14
```

## Session to Mapping

| Session | Mapping | Plan-Level |
|---------|---------|------------|
| S_GMS_ETL_DPA_TRUNCATE | M_UTL_DPA_TRUNCATE | Top-Level |
| S_GMS_ETL_PARAM_SETUP | M_UTL_PARAM_SETUP | Top-Level |
| S_DPA_SUM_FACT_GMS_DLY_MSD_SMRY | M_DPA_SUM_FACT_GMS_DLY_MSD_SMRY | Worklet: WL_GMS_DDS_SUM |
| S_DPA_SUM_FACT_GMS_DLY_DOG_RGSTR | M_DPA_SUM_FACT_GMS_DLY_DOG_RGSTR | Worklet: WL_GMS_DDS_SUM |
| S_DPA_SUM_FACT_GMS_DLY_MSD_INCDT | M_DPA_SUM_FACT_GMS_DLY_MSD_INCDT | Worklet: WL_GMS_DDS_SUM |
| S_DDS_APL_FACT_GMS_DLY_DOG_RGSTR | M_DDS_APL_FACT_GMS_DLY_DOG_RGSTR | Worklet: WL_GMS_DDS_APLY |
| S_DDS_APL_FACT_GMS_DLY_MSD_SMRY | M_DDS_APL_FACT_GMS_DLY_MSD_SMRY | Worklet: WL_GMS_DDS_APLY |
| S_DDS_APL_FACT_GMS_DLY_MSD_INCDT | M_DDS_APL_FACT_GMS_DLY_MSD_INCDT | Worklet: WL_GMS_DDS_APLY |
| T_MAIL_SUCCESS | - | Top-Level |