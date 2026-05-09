#!/usr/bin/env bash
set -e

APP_NAME="ai-store-analysis"
APP_DIR="/opt/$APP_NAME"
PORT="3000"

echo "=========================================="
echo "    AI Store Analysis 系统管理工具"
echo "=========================================="

if [ "$(id -u)" -ne 0 ]; then
  echo "❌ 请使用 root 运行：sudo bash scripts/install.sh"
  exit 1
fi

do_deploy() {
    echo "==> 正在准备环境和依赖..."
    apt update
    apt install -y git curl ufw nginx ca-certificates --no-install-recommends

    if ! command -v node >/dev/null 2>&1; then
      echo "==> 正在安装 Node.js 22..."
      curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
      apt install -y nodejs
    fi

    if ! command -v pm2 >/dev/null 2>&1; then
      echo "==> 正在安装 PM2..."
      npm install -g pm2
    fi

    cd "$APP_DIR"

    echo "==> 正在安装依赖..."
    npm install

    echo "==> 正在启动服务..."
    pm2 delete "$APP_NAME" >/dev/null 2>&1 || true

    if [ -f "package.json" ] && grep -q '"start"' package.json; then
      pm2 start npm --name "$APP_NAME" -- start
    else
      pm2 start server.js --name "$APP_NAME"
    fi

    pm2 save
    pm2 startup systemd -u root --hp /root >/dev/null 2>&1 || true

    echo "==> 正在配置 nginx..."
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
    }
}
EOF

    ln -sf "/etc/nginx/sites-available/$APP_NAME" "/etc/nginx/sites-enabled/$APP_NAME"
    rm -f /etc/nginx/sites-enabled/default

    nginx -t
    systemctl restart nginx

    echo "==> 正在配置防火墙..."
    ufw allow OpenSSH
    ufw allow 80
    ufw allow 443
    ufw --force enable

    PUBLIC_IP=$(curl -fsSL ifconfig.me || true)

    echo
    echo "✅ 部署/更新完成！"
    echo "📁 项目目录: $APP_DIR"
    echo "🔗 访问地址: http://$PUBLIC_IP"
}

do_stop() {
    echo "==> 正在停止服务..."
    pm2 stop "$APP_NAME" || echo "⚠️ 服务未在运行"
    pm2 save
    echo "✅ 服务已停止。"
}

do_restart() {
    echo "==> 正在重启服务..."
    pm2 restart "$APP_NAME" || pm2 start server.js --name "$APP_NAME"
    pm2 save
    echo "✅ 服务已重启。"
}

do_logs() {
    pm2 logs "$APP_NAME"
}

do_uninstall() {
    read -p "⚠️ 确定要彻底卸载吗？这会删除代码、PM2服务和nginx配置！[y/N]: " confirm < /dev/tty

    if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
        pm2 delete "$APP_NAME" >/dev/null 2>&1 || true
        pm2 save || true

        rm -f "/etc/nginx/sites-available/$APP_NAME"
        rm -f "/etc/nginx/sites-enabled/$APP_NAME"
        systemctl restart nginx || true

        rm -rf "$APP_DIR"

        echo "✅ 卸载完成。"
    else
        echo "取消操作。"
    fi
}

echo " 1) 安装 / 更新部署"
echo " 2) 停止服务"
echo " 3) 重启服务"
echo " 4) 查看日志"
echo " 5) 彻底卸载"
echo " 6) 退出"
echo "=========================================="

read -p "请选择操作 [1-6]: " choice < /dev/tty

case "$choice" in
    1) do_deploy ;;
    2) do_stop ;;
    3) do_restart ;;
    4) do_logs ;;
    5) do_uninstall ;;
    6) exit 0 ;;
    *) echo "❌ 无效选项"; exit 1 ;;
esac