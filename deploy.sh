#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

readonly REPO_URL="https://github.com/yukaijun2001/muse2api-new.git"
readonly REPO_BRANCH="master"
readonly APP_PORT="10002"
readonly ADMIN_USERNAME="sys_wutong"

log() {
    printf '[muse2api] %s\n' "$*"
}

die() {
    printf '[muse2api] 错误：%s\n' "$*" >&2
    exit 1
}

is_root() {
    [[ "$(id -u)" -eq 0 ]]
}

install_debian_dependencies() {
    command -v apt-get >/dev/null 2>&1 || die "缺少必要命令，且当前系统不是 Debian/Ubuntu。请先安装 Docker Engine、Docker Compose v2、git、curl 和 openssl。"
    is_root || die "自动安装 Docker 需要 root 权限。请使用 sudo 运行 deploy.sh。"

    log "安装基础依赖..."
    apt-get update
    apt-get install -y ca-certificates curl git openssl
}

ensure_commands() {
    local missing=0
    for command_name in curl git openssl; do
        if ! command -v "$command_name" >/dev/null 2>&1; then
            missing=1
        fi
    done
    if (( missing )); then
        install_debian_dependencies
    fi

    if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
        command -v apt-get >/dev/null 2>&1 || die "未检测到 Docker Compose v2。请先安装 Docker Engine 和 Compose 插件。"
        is_root || die "自动安装 Docker 需要 root 权限。请使用 sudo 运行 deploy.sh。"
        command -v curl >/dev/null 2>&1 || install_debian_dependencies

        log "安装 Docker Engine 和 Compose 插件..."
        local installer
        installer=$(mktemp)
        curl -fsSL https://get.docker.com -o "$installer"
        sh "$installer"
        rm -f "$installer"
    fi

    if command -v systemctl >/dev/null 2>&1; then
        systemctl enable --now docker >/dev/null 2>&1 || true
    fi
    docker info >/dev/null 2>&1 || die "Docker daemon 未运行或当前用户无权访问。请启动 Docker，或使用 sudo 运行 deploy.sh。"
    docker compose version >/dev/null 2>&1 || die "Docker Compose v2 不可用。"
}

script_directory() {
    local source_path=${BASH_SOURCE[0]}
    if [[ "$source_path" != /dev/* && -f "$source_path" ]]; then
        cd -- "$(dirname -- "$source_path")" && pwd
        return
    fi
    printf '%s\n' "${MUSE2API_INSTALL_DIR:-/opt/muse2api}"
}

prepare_repository() {
    local install_dir=$1
    if [[ -d "$install_dir/.git" ]]; then
        local origin
        origin=$(git -C "$install_dir" remote get-url origin 2>/dev/null || true)
        [[ "$origin" == "$REPO_URL" ]] || die "$install_dir 已是其他 Git 仓库，请设置 MUSE2API_INSTALL_DIR 指向安装目录。"
        [[ -z "$(git -C "$install_dir" status --porcelain --untracked-files=no)" ]] || die "仓库有未提交的跟踪文件修改。请先提交或处理这些修改，再运行部署脚本。"
        log "从 GitHub 拉取最新代码..."
        git -C "$install_dir" fetch --prune origin "$REPO_BRANCH"
        git -C "$install_dir" merge --ff-only FETCH_HEAD
    else
        [[ ! -e "$install_dir" || -z "$(find "$install_dir" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]] || die "$install_dir 已存在且非空，不会覆盖其中的文件。请设置 MUSE2API_INSTALL_DIR 指向空目录。"
        mkdir -p "$(dirname -- "$install_dir")"
        log "克隆 GitHub 最新代码到 $install_dir..."
        git clone --depth 1 --branch "$REPO_BRANCH" "$REPO_URL" "$install_dir"
    fi
}

set_env_value() {
    local env_file=$1 key=$2 value=$3 tmp_file
    tmp_file=$(mktemp "${env_file}.XXXXXX")
    if [[ -f "$env_file" ]]; then
        awk -v key="$key" -v value="$value" '
            index($0, key "=") == 1 { print key "=" value; found=1; next }
            { print }
            END { if (!found) print key "=" value }
        ' "$env_file" > "$tmp_file"
    else
        printf '%s=%s\n' "$key" "$value" > "$tmp_file"
    fi
    chmod 600 "$tmp_file"
    mv "$tmp_file" "$env_file"
}

write_admin_password() {
    local password_file=$1 password confirmation
    if [[ -s "$password_file" ]]; then
        return
    fi

    if [[ -n "${MUSE2API_ADMIN_PASSWORD:-}" ]]; then
        password=$MUSE2API_ADMIN_PASSWORD
    else
        [[ -r /dev/tty ]] || die "首次部署需要设置管理员密码。请在交互式终端运行脚本。"
        printf '请输入管理员密码（至少 8 位，输入不回显）： ' >&2
        IFS= read -r -s password < /dev/tty
        printf '\n请再次输入管理员密码： ' >&2
        IFS= read -r -s confirmation < /dev/tty
        printf '\n' >&2
        [[ "$password" == "$confirmation" ]] || die "两次输入的管理员密码不一致。"
    fi

    (( ${#password} >= 8 )) || die "管理员密码至少需要 8 个字符。"
    printf '%s' "$password" > "$password_file"
    unset password confirmation MUSE2API_ADMIN_PASSWORD
}

create_secrets() {
    local secrets_dir="$1/deploy/secrets"
    mkdir -p "$secrets_dir"
    chmod 700 "$secrets_dir"

    [[ -s "$secrets_dir/muse2api_key" ]] || openssl rand -hex 32 > "$secrets_dir/muse2api_key"
    write_admin_password "$secrets_dir/muse2api_admin_password"
    [[ -s "$secrets_dir/muse2api_credential_key" ]] || openssl rand -base64 32 > "$secrets_dir/muse2api_credential_key"
    chmod 444 "$secrets_dir"/*
}

detect_server_ip() {
    local address
    address=$(curl -4fsS --max-time 5 https://api.ipify.org 2>/dev/null || true)
    if [[ "$address" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]]; then
        printf '%s\n' "$address"
        return
    fi

    address=$(curl -6fsS --max-time 5 https://api64.ipify.org 2>/dev/null || true)
    if [[ "$address" == *:* ]]; then
        printf '[%s]\n' "$address"
        return
    fi

    if command -v ip >/dev/null 2>&1; then
        address=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{ for (i=1; i<=NF; i++) if ($i == "src") { print $(i+1); exit } }')
    fi
    if [[ -z "$address" ]] && command -v hostname >/dev/null 2>&1; then
        address=$(hostname -I 2>/dev/null | awk '{ for (i=1; i<=NF; i++) if ($i !~ /^127\./) { print $i; exit } }')
    fi
    if [[ "$address" == *:* ]]; then
        printf '[%s]\n' "$address"
        return
    fi
    printf '%s\n' "${address:-无法自动检测，请查看服务器网络配置}"
}

main() {
    ensure_commands

    local install_dir
    install_dir=$(script_directory)
    prepare_repository "$install_dir"
    cd "$install_dir"

    set_env_value .env MUSE2API_ADMIN_USERNAME "$ADMIN_USERNAME"
    set_env_value .env MUSE2API_BIND 0.0.0.0
    set_env_value .env MUSE2API_HTTP_PORT "$APP_PORT"
    set_env_value .env MUSE2API_UID 10001
    set_env_value .env MUSE2API_GID 10001
    create_secrets "$install_dir"

    log "准备持久化数据目录..."
    sh deploy/init-data.sh

    log "构建并启动 Docker Compose 服务..."
    docker compose -f docker-compose.yml -f deploy/docker-compose.secrets.yml up -d --build

    log "等待服务健康检查..."
    local ready=0
    for _ in {1..60}; do
        if curl -fsS --max-time 2 "http://127.0.0.1:${APP_PORT}/healthz" >/dev/null; then
            ready=1
            break
        fi
        sleep 2
    done
    (( ready )) || {
        docker compose -f docker-compose.yml -f deploy/docker-compose.secrets.yml logs --tail=80 muse2api >&2 || true
        die "服务未能在 120 秒内通过健康检查。"
    }

    local server_ip
    server_ip=$(detect_server_ip)
    log "部署完成。"
    printf '管理后台: http://%s:%s/admin\n' "$server_ip" "$APP_PORT"
    printf '健康检查: http://%s:%s/healthz\n' "$server_ip" "$APP_PORT"
    printf '管理员账号: %s\n' "$ADMIN_USERNAME"
    printf '管理员密码: 首次运行时在终端中输入的密码\n'
    printf '容器状态: docker compose -f docker-compose.yml -f deploy/docker-compose.secrets.yml ps\n'
}

main "$@"
