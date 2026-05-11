#!/usr/bin/env bash
set -e

# --- 配置区 ---
APP_NAME="ai-store"
APP_DISPLAY_NAME="AI Store Analysis System"
APP_DIR="/opt/ai-store-analysis"
PORT="3000"
SERVICE_FILE="/etc/systemd/system/$APP_NAME.service"

# 自动识别项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 必须 root 运行
if [ "$(id -u)" -ne 0 ]; then
  echo "❌ 请使用 root 运行：sudo bash scripts/install.sh"
  exit 1
fi

# 核心部署逻辑
do_deploy() {
    echo "------------------------------------------"
    echo " 🚀 正在进入部署流程..."
    echo " 1) 稳定版 (main)"
    echo " 2) 开发版 (dev)"
    echo " 3) 返回"
    read -p "请选择安装版本 [1-3]: " br_choice < /dev/tty
    
    local target_branch=""
    case "$br_choice" in
        1) target_branch="main" ;;
        2) target_branch="dev" ;;
        *) return ;;
    esac

    echo "==> 1. 正在同步代码 ($target_branch)..."
    # 如果已经在项目目录且是 git 仓库，尝试切换分支
    if [ -d "$PROJECT_ROOT/.git" ]; then
        cd "$PROJECT_ROOT"
        git fetch origin "$target_branch"
        git checkout "$target_branch"
        git reset --hard "origin/$target_branch"
    fi

    # 准备生产目录
    mkdir -p "$APP_DIR"
    if [ "$PROJECT_ROOT" != "$APP_DIR" ]; then
        cp -r "$PROJECT_ROOT/." "$APP_DIR/"
    fi

    cd "$APP_DIR"

    echo "==> 2. 正在准备系统环境与依赖..."
    # 仅在第一次安装或依赖变化时可能需要 apt
    apt update && apt install -y python3 python3-pip python3-venv nginx --no-install-recommends >/dev/null 2>&1

    # 处理虚拟环境
    [ -d "venv" ] || python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip >/dev/null
    pip install -r requirements.txt >/dev/null

    echo "==> 3. 正在更新系统服务配置..."
    cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=$APP_DISPLAY_NAME
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python3 -m uvicorn apps.api.src.main:app --host 0.0.0.0 --port $PORT
Restart=always
RestartSec=3s
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

    echo "==> 4. 正在重启服务并应用更改..."
    systemctl daemon-reload
    systemctl enable "$APP_NAME" >/dev/null 2>&1
    systemctl restart "$APP_NAME"

    # Nginx 配置 (仅当配置不存在时创建)
    if [ ! -f "/etc/nginx/sites-available/$APP_NAME" ]; then
        echo "==> 配置 Nginx 反向代理..."
        cat > "/etc/nginx/sites-available/$APP_NAME" <<EOF
server {
    listen 80;
    server_name _;
    location / {
        proxy_pass http://127.0.0.1:$PORT;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_cache_bypass \$http_upgrade;
        proxy_set_header X-Accel-Buffering no;
        proxy_buffering off;
        proxy_read_timeout 600s;
    }
}
EOF
        ln -sf "/etc/nginx/sites-available/$APP_NAME" "/etc/nginx/sites-enabled/$APP_NAME"
        rm -f /etc/nginx/sites-enabled/default
        systemctl restart nginx
    fi

    echo
    echo "✅ 部署/更新成功！当前版本: $target_branch"
    echo "🔗 访问地址: http://$(curl -fsSL ifconfig.me || echo 'localhost')"
}

# --- 主界面 ---
clear
CURRENT_BR=$(git branch --show-current 2>/dev/null || echo "未知")
echo "=========================================="
echo "    $APP_DISPLAY_NAME 管理工具"
echo "    当前版本: [$CURRENT_BR]"
echo "=========================================="
echo " 1) 部署 / 更新系统 (自动重启)"
echo " 2) 停止服务"
echo " 3) 查看实时日志"
echo " 4) 查看运行状态"
echo " 5) 退出"
echo "=========================================="

read -p "请选择操作 [1-5]: " choice < /dev/tty

case "$choice" in
    1) do_deploy ;;
    2) systemctl stop "$APP_NAME" && echo "✅ 服务已停止。" ;;
    3) journalctl -u "$APP_NAME" -f ;;
    4) systemctl status "$APP_NAME" ;;
    5) exit 0 ;;
    *) echo "❌ 无效选项"; exit 1 ;;
esac