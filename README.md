# mongomig

按 **UTC 小时** + **集合** 维度，将 MongoDB 多租户数据 discover 后备份到 **S3 兼容 OSS**；`dump.tar` 内保留 mongodump 默认目录结构（`*.bson.gz` + `*.metadata.json.gz`）。

## 工具定位

- **主命令**：`mongomig backup`（`restore` / `status` 已注册，本期为占位）。
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
- **小时状态**：`in_progress` | `partial` | `complete`（未结束 UTC 小时备份后标 `partial`；下一调度对 `partial` **整桶覆盖**重备）。
- **同进程中断**（SIGINT/SIGTERM）：按 hour meta 中 `uploaded` **续跑**已成功租户。

## 依赖

- Go 1.23+
- [MongoDB Database Tools](https://www.mongodb.com/docs/database-tools/)：`mongodump`（及系统 `tar`）
- 配置 S3：`--s3-endpoint`、`--s3-bucket` 或 `.env` / `MONGOMIG_*`、`S3_*`

## 构建

```bash
go build -o mongomig ./cmd/mongomig
```

## 备份

```bash
./mongomig --mongo-uri "$MONGOMIG_MONGO_URI" --staging-dir ./staging \
  --remote-prefix mongomig \
  --s3-endpoint "https://tos-s3-cn-shanghai.volces.com" \
  --s3-bucket "your-bucket" --s3-region "cn-shanghai" \
  --s3-path-style=false \
  backup \
  --collections "revol.liveroom_platform_raw_data" \
  [--tenant-field tenant_key] \
  [--time-field created_at] \
  [--from-hour 2026-05-15T00] [--to-hour 2026-05-15T17] \
  [--cleanup-local] [--dry-run] [--force-hour] [--reset-hour 2026-05-15T17]
```

| Flag | 说明 |
|------|------|
| `--collections` | 必填，逗号分隔 `db.collection` |
| `--from-hour` / `--to-hour` | UTC `YYYY-MM-DDTHH`；默认 `to` = 当前 UTC 小时 |
| `--force-hour` | 已 `complete` 的小时仍重备 |
| `--reset-hour` | 先删除该小时 OSS 前缀再备 |

## 环境变量

`MONGOMIG_MONGO_URI`、`MONGOMIG_S3_*`，以及 `.env` 中常见的 `S3_ENDPOINT`、`S3_BUCKET`、`ACCESS_KEY_ID`、`SECRET_ACCESS_KEY`。火山 TOS 建议使用地域 endpoint（如 `https://tos-s3-cn-shanghai.volces.com`）且 **`--s3-path-style=false`**。

## 恢复（未实现）

```bash
./mongomig restore   # 返回「尚未实现」
./mongomig status    # 返回「尚未实现」
```

将来 restore：`tar -xf dump.tar` 后 `mongorestore --gzip`。
