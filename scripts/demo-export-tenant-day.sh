#!/usr/bin/env bash
# Demo: 导出「单个租户 + 上海时区某一天」的 liveroom_platform_raw_data（mongodump + queryFile）。
#
# 默认: mongodb://admin:admin@localhost:37017/?authSource=admin
#       （凭证在 admin；业务库名仍由 DB=revol、mongodump --db 指定）
#
#   bash scripts/demo-export-tenant-day.sh
#
# 环境变量可选:
#   MONGO_URI TENANT_KEY SHANGHAI_DAY DB COLL OUT_DIR
# 若本机 PATH 无 mongodump：优先使用项目根目录下的 Database Tools（解压后含 bin/mongodump），
#   识别路径依次为: mongodb-database-tools/ 、 tools/ 、 mongo-tools/ 、 bin/mongodump 、 mongodb-database-tools-*/
# 其次使用 .cache 或从 fastdl 下载（需网络）。
#   MONGODB_TOOLS_VERSION   默认 100.9.4
#   MONGODB_TOOLS_URL       完整 .tgz 地址，覆盖自动拼接的 URL
#   MONGODB_TOOLS_DIR       已解压目录，其下应有 bin/mongodump（显式指定时优先于项目内自动探测）
#   MONGODB_TOOLS_BIN       mongodump 可执行文件绝对路径
#   SKIP_MONGODB_TOOLS_DOWNLOAD=1  禁止自动下载，未找到 mongodump 时直接失败

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

detect_tools_platform() {
	case "$(uname -s)" in
	Darwin)
		case "$(uname -m)" in
		arm64) echo macos-arm64 ;;
		x86_64) echo macos-x86_64 ;;
		*) return 1 ;;
		esac
		;;
	Linux)
		case "$(uname -m)" in
		x86_64) echo ubuntu2204-x86_64 ;;
		aarch64 | arm64) echo ubuntu2204-aarch64 ;;
		*) return 1 ;;
		esac
		;;
	*) return 1 ;;
	esac
}

ensure_mongodump() {
	if command -v mongodump >/dev/null 2>&1; then
		return 0
	fi
	if [[ -n "${MONGODB_TOOLS_BIN:-}" && -x "$MONGODB_TOOLS_BIN" ]]; then
		PATH="$(dirname "$MONGODB_TOOLS_BIN"):$PATH"
		export PATH
		return 0
	fi
	if [[ -n "${MONGODB_TOOLS_DIR:-}" && -x "${MONGODB_TOOLS_DIR}/bin/mongodump" ]]; then
		PATH="${MONGODB_TOOLS_DIR}/bin:$PATH"
		export PATH
		return 0
	fi
	local _cand
	for _cand in "$ROOT/mongodb-database-tools" "$ROOT/tools" "$ROOT/mongo-tools"; do
		if [[ -x "$_cand/bin/mongodump" ]]; then
			echo "使用项目内 Database Tools: $_cand" >&2
			PATH="$_cand/bin:$PATH"
			export PATH
			return 0
		fi
	done
	if [[ -x "$ROOT/bin/mongodump" ]]; then
		echo "使用项目内 mongodump: $ROOT/bin" >&2
		PATH="$ROOT/bin:$PATH"
		export PATH
		return 0
	fi
	shopt -s nullglob
	for _cand in "$ROOT"/mongodb-database-tools-*/; do
		_cand="${_cand%/}"
		if [[ -x "$_cand/bin/mongodump" ]]; then
			echo "使用项目内 Database Tools: $_cand" >&2
			PATH="$_cand/bin:$PATH"
			export PATH
			shopt -u nullglob
			return 0
		fi
	done
	shopt -u nullglob

	local plat ver cache_dir install_dir url tgz top
	plat="$(detect_tools_platform)" || plat=""
	ver="${MONGODB_TOOLS_VERSION:-100.9.4}"
	cache_dir="$ROOT/.cache/mongodb-database-tools"
	if [[ -n "$plat" ]]; then
		install_dir="$cache_dir/mongodb-database-tools-${plat}-${ver}"
		if [[ -x "$install_dir/bin/mongodump" ]]; then
			PATH="$install_dir/bin:$PATH"
			export PATH
			return 0
		fi
	fi

	if [[ "${SKIP_MONGODB_TOOLS_DOWNLOAD:-0}" == "1" ]]; then
		echo "未找到 mongodump，且已设置 SKIP_MONGODB_TOOLS_DOWNLOAD=1。" >&2
		echo "请将 Database Tools 解压到项目根下的 mongodb-database-tools-* 或 tools/，或设置 MONGODB_TOOLS_DIR / MONGODB_TOOLS_BIN。" >&2
		return 1
	fi
	if [[ -z "$plat" ]]; then
		echo "无法识别本机平台，无法自动下载 Database Tools（仅支持 macOS x86_64/arm64、Linux x86_64/aarch64）。" >&2
		echo "请将工具解压到项目目录，或设置 MONGODB_TOOLS_DIR / MONGODB_TOOLS_BIN。" >&2
		return 1
	fi
	install_dir="$cache_dir/mongodb-database-tools-${plat}-${ver}"
	command -v curl >/dev/null 2>&1 || {
		echo "自动下载需要 curl。请安装 MongoDB Database Tools，或设置 MONGODB_TOOLS_DIR / MONGODB_TOOLS_BIN。" >&2
		return 1
	}
	url="${MONGODB_TOOLS_URL:-https://fastdl.mongodb.org/tools/db/mongodb-database-tools-${plat}-${ver}.tgz}"
	tgz="$(mktemp -t mongodb-tools.XXXXXX.tgz)"
	echo "未在 PATH 中找到 mongodump，正在下载 Database Tools ${ver} (${plat})…" >&2
	echo "  $url" >&2
	if ! curl -fL --retry 3 --connect-timeout 30 -A "mongomig-demo-export/1.0" -o "$tgz" "$url"; then
		rm -f "$tgz"
		echo "自动下载失败（可能被网络或 CDN 策略拦截）。请任选其一：" >&2
		echo "  macOS: brew tap mongodb/brew && brew install mongodb-database-tools" >&2
		echo "  或从 https://www.mongodb.com/try/download/database-tools 下载对应平台包，解压后设置：" >&2
		echo "  export MONGODB_TOOLS_DIR=/path/to/解压目录" >&2
		return 1
	fi
	local extract
	extract="$(mktemp -d)"
	if ! tar -xzf "$tgz" -C "$extract"; then
		rm -rf "$extract" "$tgz"
		echo "解压 Database Tools 失败。" >&2
		return 1
	fi
	rm -f "$tgz"
	top="$(find "$extract" -maxdepth 1 -type d -name 'mongodb-database-tools-*' | head -n 1)"
	if [[ -z "$top" || ! -x "$top/bin/mongodump" ]]; then
		rm -rf "$extract"
		echo "下载包内未找到 mongodump，请检查 MONGODB_TOOLS_VERSION / MONGODB_TOOLS_URL。" >&2
		return 1
	fi
	mkdir -p "$cache_dir"
	rm -rf "$install_dir"
	mv "$top" "$install_dir"
	rmdir "$extract" 2>/dev/null || rm -rf "$extract"
	PATH="$install_dir/bin:$PATH"
	export PATH
}

MONGO_URI="${MONGO_URI:-mongodb://admin:admin@localhost:37017/?authSource=admin}"
DB="${DB:-revol}"
COLL="${COLL:-liveroom_platform_raw_data}"
TENANT_KEY="${TENANT_KEY:-isolation_1eff7eac34924bd390ead2f9431f04bd}"
SHANGHAI_DAY="${SHANGHAI_DAY:-2026-05-15}"
OUT_DIR="${OUT_DIR:-./demo-export-${TENANT_KEY}-${SHANGHAI_DAY}}"

QUERY_FILE="$(mktemp -t mongodump-query.XXXXXX.json)"
cleanup() { rm -f "$QUERY_FILE"; }
trap cleanup EXIT

command -v python3 >/dev/null 2>&1 || { echo "需要 python3" >&2; exit 1; }
ensure_mongodump || exit 1
command -v mongodump >/dev/null 2>&1 || {
	echo "仍无法使用 mongodump。请将 Database Tools 放在项目根（见脚本头部说明），或设置 MONGODB_TOOLS_DIR。" >&2
	exit 1
}

export TENANT_KEY SHANGHAI_DAY
python3 - <<'PY' >"$QUERY_FILE"
import json
import os
from datetime import datetime, timedelta, timezone

tenant = os.environ["TENANT_KEY"]
day = os.environ["SHANGHAI_DAY"]
d = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone(timedelta(hours=8)))
start = d.astimezone(timezone.utc)
end = (d + timedelta(days=1)).astimezone(timezone.utc)
fmt = "%Y-%m-%dT%H:%M:%S.000Z"

q = {
    "$and": [
        {"tenant_key": tenant},
        {
            "created_at": {
                "$gte": {"$date": start.strftime(fmt)},
                "$lt": {"$date": end.strftime(fmt)},
            }
        },
    ]
}
print(json.dumps(q, separators=(",", ":")))

start_s = start.strftime(fmt)
end_s = end.strftime(fmt)
print(f"# UTC range [ {start_s} , {end_s} )  (上海日历日 {day})", file=__import__("sys").stderr)
PY

echo "URI:         $MONGO_URI"
echo "namespace:   $DB.$COLL"
echo "tenant:      $TENANT_KEY"
echo "shanghai day $SHANGHAI_DAY"
echo "query file:  $QUERY_FILE"
echo "query:       $(cat "$QUERY_FILE")"
echo "out:         $OUT_DIR"
echo

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

mongodump --uri="$MONGO_URI" \
  --db="$DB" \
  --collection="$COLL" \
  --queryFile="$QUERY_FILE" \
  --gzip \
  --out="$OUT_DIR"

echo
echo "完成。产物:"
echo "  $OUT_DIR/$DB/${COLL}.bson.gz"
echo "  $OUT_DIR/$DB/${COLL}.metadata.json.gz"
echo
echo "恢复到其他实例示例（请替换目标 URI；若用户在 admin 库认证请加 ?authSource=admin，生产慎用）:"
echo "  mongorestore --uri=\"mongodb://user:pass@host/?authSource=admin\" --gzip \"$OUT_DIR\""
