#!/usr/bin/env bash
set -e

# --- 配置区 ---
APP_NAME="ai-store"
APP_DISPLAY_NAME="AI Store Analysis System"
APP_DIR="/opt/ai-store-analysis"
PORT="3000"
SERVICE_FILE="/etc/systemd/system/$APP_NAME.service"

echo "=========================================="
echo "    $APP_DISPLAY_NAME (纯 Python 版)"
echo "    不再依赖 Node.js / PM2"
echo "=========================================="

# 必须 root 运行
if [ "$(id -u)" -ne 0 ]; then
  echo "❌ 请使用 root 运行：sudo bash scripts/install.sh"
  exit 1
fi

# 自动识别项目根目录 (脚本所在目录的上一级)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

do_deploy() {
    echo "==> 1. 正在安装系统基础环境..."
    apt update
    apt install -y git curl ufw nginx ca-certificates python3 python3-pip python3-venv --no-install-recommends

    # 准备目录
    mkdir -p "$APP_DIR"
    
    echo "==> 同步代码从 $PROJECT_ROOT 到 $APP_DIR..."
    cp -r "$PROJECT_ROOT/." "$APP_DIR/"

    cd "$APP_DIR"

    echo "==> 2. 正在准备 Python 虚拟环境..."
    python3 -m venv venv
    source venv/bin/activate
    
    echo "==> 3. 正在安装 Python 依赖..."
    pip install --upgrade pip
    pip install -r requirements.txt

    echo "==> 4. 正在生成 Systemd 服务配置..."
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

    echo "==> 5. 正在启动系统服务..."
    systemctl daemon-reload
    systemctl enable "$APP_NAME"
    systemctl restart "$APP_NAME"

    echo "==> 6. 正在配置 Nginx 反向代理..."
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
        
        # SSE 流式支持关键配置
        proxy_set_header X-Accel-Buffering no;
        proxy_buffering off;
        chunked_transfer_encoding on;
        proxy_read_timeout 600s;
    }
}
EOF

    ln -sf "/etc/nginx/sites-available/$APP_NAME" "/etc/nginx/sites-enabled/$APP_NAME"
    rm -f /etc/nginx/sites-enabled/default

    nginx -t
    systemctl restart nginx

    echo "==> 7. 正在配置防火墙..."
    ufw allow OpenSSH
    ufw allow 80
    ufw --force enable

    PUBLIC_IP=$(curl -fsSL ifconfig.me || true)

    echo
    echo "✅ 纯净版部署完成！"
    echo "📁 运行目录: $APP_DIR"
    echo "⚙️ 服务状态: systemctl status $APP_NAME"
    echo "📝 查看日志: journalctl -u $APP_NAME -f"
    echo "🔗 访问地址: http://$PUBLIC_IP"
}

do_stop() {
    echo "==> 正在停止服务..."
    systemctl stop "$APP_NAME"
    echo "✅ 服务已停止。"
}

do_restart() {
    echo "==> 正在重启服务..."
    systemctl restart "$APP_NAME"
    echo "✅ 服务已重启。"
}

do_logs() {
    echo "==> 正在查看实时日志 (按 Ctrl+C 退出)..."
    journalctl -u "$APP_NAME" -f
}

do_status() {
    systemctl status "$APP_NAME"
}

echo " 1) 全自动安装 / 更新 (纯 Python 模式)"
echo " 2) 停止服务"
echo " 3) 重启服务"
echo " 4) 查看实时日志"
echo " 5) 查看运行状态"
echo " 6) 退出"
echo "=========================================="

read -p "请选择操作 [1-6]: " choice < /dev/tty

case "$choice" in
    1) do_deploy ;;
    2) do_stop ;;
    3) do_restart ;;
    4) do_logs ;;
    5) do_status ;;
    6) exit 0 ;;
    *) echo "❌ 无效选项"; exit 1 ;;
esac