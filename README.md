# mongomig

按 **UTC 小时** + **集合** 维度，将 MongoDB 多租户数据 discover 后备份到 **S3 兼容 OSS**；`dump.tar` 内保留 mongodump 默认目录结构（`*.bson.gz` + `*.metadata.json.gz`）。

## 工具定位

- **主命令**：`mongomig backup`；`mongomig restore` 从 OSS 按小时恢复租户 dump；`mongomig status` / `repair` 审计与修复 meta。
- **OSS 布局**：

```text
{remote-prefix}/{db}/{collection}/
  meta.json          # 集合级：oldest/newest complete、active（restore 用 active 扩展恢复上界）
  indexes.json       # 每次 backup 覆盖
  {year}/{month}/{day}/{hour}/
    meta.json        # 小时级：status、tenants[]
    {shard}/{tenant_key}/dump.tar
```

- **`shard`**：`tenant_key` 最后一个 `_` 之后后缀的 **前 2 字符**（例 `isolation_1eff7eac...` → `1e`）。
- **小时状态**：`partial` | `complete`（未完成一律 `partial`；下次调度 **整桶删除后全量重备**）。
- **同进程中断**（SIGINT/SIGTERM）：按 hour meta 中 `uploaded` **续跑**已成功租户。

## 依赖

- Go 1.23+
- [MongoDB Database Tools](https://www.mongodb.com/docs/database-tools/)：`mongodump`（及系统 `tar`）
- 配置：**`mongomig.yaml`**（当前目录，可参考 `mongomig.example.yaml`），或 **`--config /path/to/file.yaml`**；命令行 flags 可覆盖

## 构建

```bash
go build -o mongomig ./cmd/mongomig
```

## 备份

```bash
./mongomig --config ./mongomig.yaml \
  --mongo-uri "$MONGOMIG_MONGO_URI" --staging-dir ./staging \
  --remote-prefix mongomig \
  --s3-endpoint "https://tos-s3-cn-shanghai.volces.com" \
  --s3-bucket "your-bucket" --s3-region "cn-shanghai" \
  --s3-path-style=false \
  backup \
  --db revol \
  [--collections "liveroom_platform_raw_data,liveroom_platform_raw_data"] \
  [--tenant-field tenant_key] \
  [--time-field created_at] \
  [--from-hour 2026-05-15T00] [--to-hour 2026-05-15T17] \
  [--cleanup-local] [--dry-run] [--force-hour]
```

| Flag | 说明 |
|------|------|
| `--db` | 必填（或 `mongomig.yaml` 中 `db`），要备份的 MongoDB 库名 |
| `--collections` | 可选；逗号分隔**集合名**（相对 `--db`）。省略则自动发现该库下全部非 system 集合；**始终排除** `restore_checkpoint_collection`（默认 `_mongomig_restore`） |
| `--from-hour` / `--to-hour` | UTC `YYYY-MM-DDTHH`；默认 `to` = 当前 UTC 小时 |
| `--force-hour` | 已 `complete` 的小时仍重备 |

## 配置文件 `mongomig.yaml`

```yaml
mongo_uri: "mongodb://..."
staging_dir: "./staging"
db: "revol"
remote_prefix: "mongomig"
s3:
  endpoint: "https://tos-s3-cn-shanghai.volces.com"
  region: "cn-shanghai"
  bucket: "your-bucket"
  access_key_id: "..."
  secret_access_key: "..."
  use_path_style: false
```

火山 TOS 建议使用地域 endpoint，且 **`use_path_style: false`**（virtual-hosted）。

## 恢复

### 用户场景

| 场景 | 命令 | 小时范围 |
|------|------|----------|
| **首次灌空库** | `restore --db revol --collections c --drop` | OSS `oldest_completed` → `newest_completed` |
| **日常增量**（backup 后又跑了新小时） | `restore --db revol --collections c` | checkpoint `newest_restored+1` → `max(newest_completed, active)` |
| **中断续跑** | 同上（重跑当前未写完 checkpoint 的整小时） | 从 `newest_restored+1` 或中断小时重试 |
| **换集群 / 全量重灌** | `restore --db revol --drop --reset-checkpoint` | 清 checkpoint 后从 OSS 最旧小时重来 |
| **只恢复某段** | `restore ... --from-hour ... --to-hour ...` | 显式覆盖上表 |

约定：恢复 **`complete` 与 `partial`（含 collection `active` 指向的进行中小时）** 桶内 `uploaded=true` 的租户；`partial` 小时**不推进** restore checkpoint，backup 补全该小时后再次 `restore` 会续上剩余租户。自动上界为 `max(newest_completed, active)`。

### 命令与参数

```bash
./mongomig --config ./mongomig.yaml restore \
  --db revol \
  [--collections "coll_a,coll_b"] \
  [--from-hour 2026-05-15T00] [--to-hour 2026-05-15T17] \
  [--drop] [--reset-checkpoint] [--dry-run]
```

| Flag | 说明 |
|------|------|
| `--db` | 目标库（恢复写入 `mongo_uri`；checkpoint 存在该库下） |
| `--collections` | 可选；默认发现该库全部非 system 集合（排除 `_mongomig_restore`） |
| `--from-hour` / `--to-hour` | 可选；覆盖自动范围（见上表） |
| `--tenant-field` / `--time-field` | 须与 backup 一致；restore 前按该窗口 deleteMany 以实现重复覆盖 |
| `--drop` | 空库首次灌入：每个集合第一次 mongorestore 前 drop |
| `--reset-checkpoint` | 删除 `{db}._mongomig_restore` 中该集合进度文档 |
| `--dry-run` | 只打印计划 |
| `--shutdown-grace` | 首次 Ctrl+C 后等待当前租户 restore 完成的最长时间（默认 30s） |

**优雅退出**：中断**不**写入 checkpoint；`newest_restored` 仅在整小时全部租户 restore 成功后推进，下次重跑未写完的整小时。

**配置项**（非命令行）：`restore_checkpoint_collection`（默认 `_mongomig_restore`）、`mongo_uri`、`remote_prefix`、`staging_dir`（仅作下载临时目录，进度不落盘）。

**OSS `active`**：集合 `meta.json` 里指向正在备份或中断后仍为 `partial` 的 UTC 小时。restore 默认恢复范围上界为 **`max(newest_completed, active)`**，并恢复该 `partial` 小时已上传租户；**不写回** OSS `active`。

**Restore checkpoint**（MongoDB）：`{db}.{restore_checkpoint_collection}` 文档 `_id={remote_prefix}/{db}/{collection}`，含 `newest_restored` / `oldest_restored` 与 `active`（**镜像** OSS 集合 `meta.json` 里 backup 的 `active`，仅作记录、**不参与** restore 调度）。`newest_restored` 仅在 OSS 小时 `complete` 且整桶租户恢复成功后推进；中断不写 checkpoint。

**重复覆盖**：每个租户 restore 前对 `tenant_field` + `time_field` 在当小时窗口执行 `deleteMany`（与 backup 查询一致），再 `mongorestore`；`--tenant-field` / `--time-field` 须与 backup 一致。全库重灌仍可用 `--drop --reset-checkpoint`。

**索引**：每个集合 restore 时从 OSS `indexes.json`（backup 写入）创建索引；`--drop` 首次灌入时在第一个租户 `mongorestore` 之后建索引，增量场景在灌数据前确保索引已存在。

流程：下载 `dump.tar` → 解压 →（deleteMany 当小时切片）→ `mongorestore --gzip` →（按集合确保 `indexes.json`）。
