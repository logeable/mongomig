# mongomig

在 **S3 兼容 OSS** 上，把 MongoDB 中 **按租户隔离的数据** 以固定目录规则备份下来，并支持原样导入；**文档数据与索引规格**与官方 Database Tools 的 dump/restore 语义对齐，保证 **导出与导入一致**。

## 工具定位

- **主路径**：`mongomig oss export` / `mongomig oss import`（推荐）。
- **OSS 目录**：`{remote-prefix}/{tenant_key 路径段}/{created_at 路径段}/{backup_run_id}/...`
  - **`tenant_key`**：业务租户 id（与查询条件一致）；写入路径前会做 **路径安全化**（非法字符替换为 `_` 等）。
  - **`created_at`**：分区时间，由你指定，用于「按租户 + 时间」归档；支持 **`YYYY-MM-DD`**（按 UTC 零点）或 **RFC3339 / RFC3339Nano**（精确到秒/纳秒）。
  - **`created_at` 路径段**：由该时间 UTC 格式化为 `2006-01-02T15-04-05Z` 后再把 `:` 换成 `-`，例如 `2026-05-14T08-30-00Z`，保证 **单层路径、可排序、无歧义**。
  - **`backup_run_id`**：每次导出自动生成的时间戳串，同一天可多次导出互不覆盖。
- **数据与索引一致**：
  - 每个集合使用 `mongodump` 带租户 `query`，生成 **`*.bson(.gz)` + `*.metadata.json(.gz)`**；`metadata.json` 中含 **MongoDB 记录的索引定义**，`mongorestore` 会按官方语义恢复索引与集合选项（与工具版本、服务端版本匹配时最为可靠）。
  - 额外写入 **`_indexes/<db>__<coll>.json`**：对应当前 `listIndexes` 结果的 JSON 副本，便于审计与人工查看（**仍以 `metadata.json` + mongorestore 为恢复主路径**）。
- **源库默认不删数**：`oss export`、`oss export-days`、`backup full` / `incremental` / `tenant` 等备份流程只通过 `mongodump` **读取** `--mongo-uri` 指向的库，**不会**删除或改写源库中的数据。`--cleanup-local` 仅删除**本机** `--staging-dir` 下的暂存文件，与源库无关。
- **可能删数的是「导入」**：`oss import` / `restore` 在**目标**连接串上执行 `mongorestore`；只有显式传入 **`--drop`** 时才会按工具语义在目标库先删集合再导入，**与备份命令无关**。

## 依赖

- Go 1.22+
- 本机安装 [MongoDB Database Tools](https://www.mongodb.com/docs/database-tools/)：`mongodump`、`mongorestore`（与 MongoDB Server **主版本尽量一致**，有利于完全一致恢复）。
- **OSS 导出/导入**：必须配置 `--s3-endpoint`、`--s3-bucket` 等（或环境变量 `MONGOMIG_S3_*`）。
- **环境变量**：除 `MONGOMIG_MONGO_URI`、`MONGOMIG_STAGING_DIR`、`MONGOMIG_S3_ACCESS_KEY_ID` / `MONGOMIG_S3_SECRET_ACCESS_KEY` 外，`mongomig` 也会读取常见 `.env` 键名：`S3_ENDPOINT`、`S3_BUCKET`、`S3_REGION`、`ACCESS_KEY_ID`、`SECRET_ACCESS_KEY`；若同时设置 **`MONGOMIG_S3_ENDPOINT`**，则 **优先于 `S3_ENDPOINT`**（便于火山 TOS 使用地域域名 `https://tos-s3-cn-shanghai.volces.com` 等，而保留控制台给出的 bucket 子域 URL 给其他工具）。

## 构建

```bash
go build -o mongomig ./cmd/mongomig
```

## OSS 导出（主命令）

```bash
./mongomig --mongo-uri "$MONGOMIG_MONGO_URI" --staging-dir /data/staging \
  --remote-prefix mongomig \
  --s3-endpoint "https://..." --s3-bucket "..." --s3-region "..." [--s3-path-style=false] \
  oss export \
  --tenant-key "acme-001" \
  --created-at "2026-05-14T10:00:00Z" \
  --collections "app.orders,app.line_items" \
  [--tenant-field tenant_key] \
  [--tenant-key-numeric] \
  [--cleanup-local]
```

- **`--collections`**：`db.collection` 列表，逗号分隔（只按第一个 `.` 切分库名与集合名）。
- **`--cleanup-local`**：上传成功后删除本次本地暂存目录（仍受「该租户在这些集合上的导出体积」峰值限制）。

### 按租户 + 日历日连续导出（进度在 OSS）

从 **该租户在指定集合上 `aggregate $min/$max` 得到的最早 `time-field` 日期** 起，按 **IANA 时区下的「日历日」**（默认 `Asia/Shanghai`）逐天导出：每天一条 `mongodump` 查询为 **`tenant_field` + `time-field` ∈ [当日 0 点, 次日 0 点)（该时区）→ UTC 边界`**，与 demo 脚本按上海自然日的语义一致。每天仍写入原有路径：`{remote-prefix}/{tenant}/{partition_path}/{backup_id}/`。

**`--collections` 支持多个**：逗号分隔多个 `db.collection`（与 `oss export` 相同）。同一天、同一 `backup_id` 下会包含 **每个集合** 各自的 dump 文件，且对每个集合使用 **同一套** `tenant_key` + 当日 `created_at`（`--time-field`，默认 `created_at`）时间窗。日历起止范围取 **所有列出集合** 上 `time-field` 的 **全局最早 `min` 与全局最晚 `max`**，再与 `--from-day` / `--to-day` /「今天（该时区）」求交。

进度与范围元数据写入 OSS：

`{remote-prefix}/{tenant}/_meta/day-sync-{sync-job-id}.json`

默认 **跳过已在进度里标记为 `ok` 的日期**（断点续跑）；`--redo-failed` 只重试失败日；`--no-skip-ok` 强制重导已成功的日。

```bash
./mongomig --mongo-uri "$MONGOMIG_MONGO_URI" --staging-dir /data/staging \
  --remote-prefix mongomig \
  --s3-endpoint "https://..." --s3-bucket "..." --s3-region "..." \
  oss export-days \
  --tenant-key "your-tenant" \
  --collections "revol.liveroom_platform_raw_business_overviews,revol.liveroom_platform_raw_data,revol.liveroom_platform_raw_live_histories" \
  [--timezone Asia/Shanghai] \
  [--time-field created_at] \
  [--from-day 2024-01-01] [--to-day 2026-05-14] \
  [--sync-job-id default] \
  [--dry-run] [--cleanup-local] [--redo-failed] [--no-skip-ok]
```

大集合上 `$min/$max` 聚合已开启 **`allowDiskUse`**；生产环境仍建议在 **`tenant_key + time-field`** 上有合适索引。

## OSS 导入

```bash
./mongomig --mongo-uri "$MONGOMIG_RESTORE_URI" --staging-dir /data/restore-staging \
  ... 与导出时相同的 S3 参数与 --remote-prefix ... \
  oss import --force \
  --tenant-key "acme-001" \
  --created-at "2026-05-14T10:00:00Z" \
  [--run-id 20060102T150405.000000000Z]
```

- **`--created-at`** 必须与导出时使用的分区时间 **解析后得到相同路径段**（同一字符串最稳妥）。
- 不传 **`--run-id`** 时，在该 `tenant_key + created_at` 分区下选择 **字典序最大** 的一次运行（时间戳 id 下即最新一次）。
- **`--drop`**：交给 `mongorestore --drop`（按工具语义先删集合再导入）。

## 兼容与旧命令

- **`backup tenant`**：等价于 OSS 导出（推荐使用 `oss export`）；可用 `--tenant-date`（仅日期）或 `--created-at`（优先）指定分区；二者皆空则默认 **当天 UTC 日期**。
- **`restore`**：若设置 `--restore-tenant-key` 且提供 `--restore-created-at` 或 **`--restore-tenant-date`（仅 YYYY-MM-DD）**，则走 **OSS 租户导入**；否则仍为 **整库 oplog 链** 恢复（`backup full` / `incremental` 场景）。
- **旧 OSS 布局**：若第二段路径曾为纯 `YYYY-MM-DD`（无 `T00-00-00Z` 后缀），导入时在 **新布局找不到** 时会自动回退尝试 **纯日期** 前缀。

## 磁盘说明

导出仍会先落到 `--staging-dir` 再上传 OSS；单租户 + 指定集合通常 **远小于整库**。详见历史说明：大库整实例备份请用大盘或跳板机。

## 端到端脚本

`scripts/e2e.sh` 主要针对 **整库 oplog 链**；租户 OSS 路径请用上面 `oss export` / `oss import` 自行联调。

## Legacy：整库 + oplog

仍保留 `backup full`、`backup incremental`、`restore`（链模式）与 `status`，便于与历史流程共存；新项目的默认心智模型应为 **OSS 租户导出/导入**。
