#!/bin/bash
set -e

echo "=== 福州门店AI分析系统 部署 ==="

# 系统依赖
pip install -r requirements.txt -q

# Smolagents 管线（方法2）依赖
pip install smolagents duckdb pandas -q

echo "依赖安装完成"
echo "启动服务: npm run dev:api"
