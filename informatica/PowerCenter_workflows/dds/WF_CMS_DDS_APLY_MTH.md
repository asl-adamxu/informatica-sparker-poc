# WF_CMS_DDS_APLY_MTH 工作流摘要

## 概要
- 本工作流（worklet `WL_CMS_DDS_FACT_MTH_APLY`）用于将多个 CMS 相关的 DPA 源表汇总并应用到 DDS 目标表（按月汇总）。包含 4 个主要 Session：
  - `S_DDS_APLY_FACT_CMS_CASE_SMRY`
  - `S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY`
  - `S_DDS_APLY_FACT_CMS_ORD_SMRY`
  - `S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY`（并行分支，可失败不影响父流程）

## 数据流 / Mapping 对应关系
- `M_DPA_APLY_FACT_CMS_CASE_OSTD_SMRY`: `DPA_FACT_CMS_CASE_OSTD_SMRY` -> `DDS_FACT_CMS_CASE_OSTD_SMRY`
- `M_DDS_APLY_FACT_CMS_CASE_SMRY`: `DPA_FACT_CMS_CASE_SMRY` -> `DDS_FACT_CMS_CASE_SMRY`
- `M_DDS_APLY_FACT_CMS_ORD_SMRY`: `DPA_FACT_CMS_ORD_SMRY` -> `DDS_FACT_CMS_ORD_SMRY`
- `M_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY`: `DPA_FACT_CMS_CASE_PRNT_SMRY` -> `DDS_FACT_CMS_CASE_PRNT_SMRY`（该 session 在 target instance 上定义了 Pre SQL）

## 执行顺序（工作流链接）
- Start -> `S_DDS_APLY_FACT_CMS_CASE_SMRY` -> `S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY` -> `S_DDS_APLY_FACT_CMS_ORD_SMRY`
- 同时，Start -> `S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY` （并行）
## 关键会话配置
- 连接：`DB_PCIS`（用于 Source/Target 的 ConnectionReference）
- 参数文件：`$PMSourceFileDir\\PCIS01\\cms_mth_job_param.txt`
- 提交：`Commit Interval = 10000`, `Commit On End Of File = YES`
- Recovery Strategy：`Fail task and continue workflow`
- 日志：各 session 指定 `Session Log File`（例：`S_DDS_APLY_FACT_CMS_CASE_SMRY.log`）
- Failure Email：使用复用任务 `T_MAIL_FAIL`（收件人 `xxx@test.aaa`），会在 Session 失败时发送通知
- 并发：`Allow Concurrent Run = NO`

## 各 Session 的重要差异与特殊处理
- `S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY`（目标 `DDS_FACT_CMS_CASE_OSTD_SMRY`）
  - Writer 属性：`Insert=YES`, `Update as Update=YES`, `Delete=YES`（支持插入/更新/删除）
- `S_DDS_APLY_FACT_CMS_CASE_SMRY`（目标 `DDS_FACT_CMS_CASE_SMRY`）
  - Writer 属性：`Insert=YES`, `Update as Update=NO`, `Delete=NO`（仅插入）
- `S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY`（目标 `DDS_FACT_CMS_CASE_PRNT_SMRY`）
  - 在 Target Instance 上定义 `Pre SQL`：
    - `delete from dds_fact_cms_case_prnt_smry where time_dmns_key in (select distinct time_dmns_key from dpa_fact_cms_case_prnt_smry)`
  - Writer 属性：`Insert=YES`, `Update as Update=YES`, `Delete=YES`
  - 注意：对应的 TaskInstance 配置为失败不影响父流程（`FAIL_PARENT_IF_INSTANCE_FAILS="NO"`），因此为可选/容错分支
- `S_DDS_APLY_FACT_CMS_ORD_SMRY`（目标 `DDS_FACT_CMS_ORD_SMRY`）
  - Writer 属性：`Insert=YES`, `Delete=NO`

## 其他注意点
- 多数 Mapping 的 Source Qualifier 未配置额外的 Source Filter 或 Pushdown SQL，默认以源表全量/增量（取决于参数文件）读取。
- 各 Session 都将 Reject 文件写到 `$PMBadFileDir\\`（reject filename 在 SessionWriter 中定义）。
- 若需调度或排查：检查参数文件 `cms_mth_job_param.txt` 中的时间窗/基准参数，以及 DB_PCIS 的运行权限和目标表索引/约束。

---
_生成于 PowerCenter 导出 XML：WF_CMS_DDS_APLY_MTH.XML_
