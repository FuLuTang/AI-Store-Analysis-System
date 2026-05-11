# 福州门店 AI 分析系统 (AI Store Analysis System)

基于 AI 的门店经营分析与诊断系统，采用 Python/FastAPI 核心与模块化架构。

## 🚀 快速启动

### 1. 安装环境
本系统采用纯 Python 后端，建议使用 Python 3.10+。

```bash
# 安装 Python 依赖
pip install -r requirements.txt
```

### 2. 启动服务 (端口 3000)
系统会自动托管前端静态文件，启动后端即可。

```bash
# 方式 A: 使用 npm (已封装命令)
npm run dev

# 方式 B: 直接运行 uvicorn
python3 -m uvicorn apps.api.src.main:app --host 0.0.0.0 --port 3000 --reload
```

### 3. 访问界面
启动后访问 [http://localhost:3000](http://localhost:3000)

## 📂 项目结构 (Python 架构)

- **`apps/`**
  - `api/src/main.py`: **FastAPI 后端核心**，负责路由、SSE 日志推送及静态文件托管。
  - `web/public/`: 前端原生静态资源 (HTML/CSS/JS)。
- **`packages/`**
  - `core/`: **核心引擎**。包含 `cleaner.py` (Pandas 数据清洗) 和 `metrics.py` (业务指标计算)。
  - `ai/`: **AI 驱动层**。包含 `ai_caller.py` (流式编排) 和 `error_reviewer.py` (逻辑审计)。
- **`storage/`**: 自动创建。包含 `uploads/` (上传缓存) 和 `cache/` (处理中间件)。
- **`scripts/`**: 包含 `install.sh` (Systemd 一键安装脚本)。

## ⚙️ 核心技术栈

- **后端**: Python 3 + FastAPI + Uvicorn
- **分析**: Pandas (数据处理)
- **AI**: OpenAI SDK + Streaming SSE (支持大模型深度诊断)
- **前端**: Vanilla JS + CSS (无构建依赖)
- **部署**: Systemd + Nginx (零 Node.js 生产依赖)

---
详细开发指南请参考 [docs/开发文档.md](./docs/开发文档.md)
