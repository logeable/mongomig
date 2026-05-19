# mongomig

按 **UTC 小时** + **集合** 维度，将 MongoDB 多租户数据 discover 后备份到 **S3 兼容 OSS**；`dump.tar` 内保留 mongodump 默认目录结构（`*.bson.gz` + `*.metadata.json.gz`）。

## 工具定位

- **主命令**：`mongomig backup`；`mongomig restore` 从 OSS 按小时恢复租户 dump；`mongomig status` / `repair` 审计与修复 meta。
- **OSS 布局**：

```text
{remote-prefix}/{db}/{collection}/
  meta.json          # 集合级：oldest/newest complete、active
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
| `--collections` | 可选；逗号分隔**集合名**（相对 `--db`）。省略则自动 `listCollections` 发现该库下全部非 system 集合 |
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
| **日常增量**（backup 后又跑了新小时） | `restore --db revol --collections c` | checkpoint `newest_restored+1` → OSS `newest_completed` |
| **中断续跑** | 同上（自动跳过已完成租户） | 同上 |
| **换集群 / 全量重灌** | `restore --db revol --drop --reset-checkpoint` | 清 checkpoint 后从 OSS 最旧小时重来 |
| **只恢复某段** | `restore ... --from-hour ... --to-hour ...` | 显式覆盖上表 |

约定：只恢复 OSS 上 **`status=complete`** 的小时；`partial` 用 `mongomig status` 查看后对该小时 `backup` 补全，再 `restore`。

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
| `--collections` | 可选；默认发现该库全部非 system 集合 |
| `--from-hour` / `--to-hour` | 可选；覆盖自动范围（见上表） |
| `--drop` | 空库首次灌入：每个集合第一次 mongorestore 前 drop |
| `--reset-checkpoint` | 删除 `{db}._mongomig_restore` 中该集合进度文档 |
| `--dry-run` | 只打印计划 |

**配置项**（非命令行）：`restore_checkpoint_collection`（默认 `_mongomig_restore`）、`mongo_uri`、`remote_prefix`、`staging_dir`（仅作下载临时目录，进度不落盘）。

**Checkpoint**：`{db}.{restore_checkpoint_collection}` 中 `_id={remote_prefix}/{db}/{collection}`，记录已恢复小时/租户。已有 checkpoint 时 `--drop` 不会再次 drop。

流程：下载 `dump.tar` → 解压 → `mongorestore --gzip`。
