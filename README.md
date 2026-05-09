# 福州门店 AI 分析系统 (AI Store Analysis System)

基于 AI 的门店经营分析与诊断系统，采用模块化 Monorepo 架构。

## 🚀 快速启动

### 1. 安装依赖
```bash
npm install
```

### 2. 启动 API 服务 (端口 3000)
```bash
npm run dev:api
```

### 3. 访问 Web 界面
启动后访问 [http://localhost:3000](http://localhost:3000)

## 📂 项目结构

- **`apps/`**
  - `api/`: 后端 Express 服务，负责数据处理与 AI 编排。
  - `web/`: 前端静态资源。
- **`packages/`**
  - `core/`: 核心计算引擎（指标计算、数据清洗）。
  - `ai/`: AI 适配器与 Prompt 模板。
  - `db/`: 数据库访问层（待扩展）。
- **`data/`**: 包含 `samples/` 示例数据。
- **`docs/`**: 项目文档与开发指南。
- **`storage/`**: 运行时的上传文件、缓存与生成的报告。

## 🛠️ 开发工具

- **部署脚本**: `scripts/install.sh`
- **备份工具**: `scripts/backup-db.sh`

---
详细开发指南请参考 [docs/开发文档.md](./docs/开发文档.md)
