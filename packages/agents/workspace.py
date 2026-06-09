"""隔离工作区：storage/artifacts/{report_id}，持久化，用于审计。

目录结构：
  storage/artifacts/{report_id}/
    input/          原始上传文件
    context/        注入的上下文文档
    output/         中间 & 最终产物
    scripts/        Agent 生成的 Python 脚本
    tables/         parquet 文件
    analysis.duckdb DuckDB 持久数据库
    manifest.json   工作区表清单
    agent_trace.json Agent 执行日志
"""

import json
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd

from .models import ColumnMeta, Manifest, RawTable, TableMeta
from .core.file_domains import DEFAULT_FILE_DOMAINS, join_domain_path, split_domain_path

ARTIFACTS_ROOT = Path("storage/artifacts")


class Workspace:

    def __init__(
        self,
        label: str = "",
        report_id: Optional[str] = None,
        base_dir: Optional[Path] = None,
        read_root: Optional[Path] = None,
        read_roots: Optional[dict[str, Path]] = None,
        default_read_domain: Optional[str] = None,
        write_root: Optional[Path] = None,
        script_root: Optional[Path] = None,
    ):
        self.label = label
        if base_dir:
            self._dir = Path(base_dir)
            self.report_id = self._dir.name
        else:
            self.report_id = report_id or f"{label}_{_short_uuid()}"
            self._dir = Path(ARTIFACTS_ROOT) / self.report_id
        self._read_roots: dict[str, Path] = {}
        if read_roots:
            self._read_roots = {str(domain): Path(root) for domain, root in read_roots.items()}
            if not self._read_roots:
                raise ValueError("read_roots 不能为空")
            self._default_read_domain = str(default_read_domain or next(iter(self._read_roots.keys())))
            if self._default_read_domain not in self._read_roots:
                raise ValueError(f"默认读域不存在: {self._default_read_domain}")
            self._read_root = self._read_roots[self._default_read_domain]
        else:
            self._default_read_domain = None
            self._read_root = Path(read_root) if read_root else self._dir
        self._write_root = Path(write_root) if write_root else self._dir
        self._input_dir = self._write_root / "input"
        self._output_dir = self._write_root / "output"
        self._context_dir = self._write_root / "context"
        self._scripts_dir = self._write_root / "scripts"
        self._tables_dir = self._write_root / "tables"
        self._script_root = Path(script_root) if script_root else self._scripts_dir

        for d in [
            self._dir,
            self._read_root,
            self._write_root,
            self._input_dir,
            self._output_dir,
            self._context_dir,
            self._scripts_dir,
            self._tables_dir,
            self._script_root,
        ]:
            try:
                d.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
        for d in self._read_roots.values():
            try:
                d.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

        self._manifest = Manifest(report_id=self.report_id, workspace_dir=str(self._write_root))
        self._copy_old_session_scripts()

    def _copy_old_session_scripts(self) -> None:
        try:
            if self._script_root.resolve() != self._scripts_dir.resolve():
                return
            import shutil
            # self._dir is storage/accounts/{account_id}/runs/{task_type}/{run_id}/workspace
            run_dir = self._dir.parent
            task_runs_dir = run_dir.parent
            if not task_runs_dir.exists() or not task_runs_dir.is_dir():
                return

            # Find sibling runs that have a workspace/scripts directory
            siblings = []
            for p in task_runs_dir.iterdir():
                if p.is_dir() and p.name != run_dir.name:
                    if (p / "workspace" / "scripts").is_dir():
                        siblings.append(p)

            # Sort by name (timestamp format ensures correct chronological sorting)
            siblings.sort(key=lambda x: x.name, reverse=True)
            target_runs = siblings[:3]

            old_scripts_root = self._scripts_dir / "old_session_scripts"
            if old_scripts_root.exists():
                return

            for past_run in target_runs:
                past_scripts_dir = past_run / "workspace" / "scripts"
                py_files = [f for f in past_scripts_dir.iterdir() if f.is_file() and f.suffix == ".py"]
                if py_files:
                    dest_dir = old_scripts_root / past_run.name
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    for py_file in py_files:
                        shutil.copyfile(py_file, dest_dir / py_file.name)
        except Exception:
            pass

    @property
    def dir(self) -> Path:
        return self._dir

    @property
    def input_dir(self) -> Path:
        return self._input_dir

    @property
    def output_dir(self) -> Path:
        return self._output_dir

    @property
    def context_dir(self) -> Path:
        return self._context_dir

    @property
    def scripts_dir(self) -> Path:
        return self._scripts_dir

    @property
    def tables_dir(self) -> Path:
        return self._tables_dir

    @property
    def read_root(self) -> Path:
        return self._read_root

    @property
    def read_roots(self) -> dict[str, Path]:
        if self._read_roots:
            return dict(self._read_roots)
        return {DEFAULT_FILE_DOMAINS[0]: self._read_root}

    @property
    def default_read_domain(self) -> Optional[str]:
        return self._default_read_domain

    @property
    def has_multi_read_roots(self) -> bool:
        return len(self._read_roots) > 0

    @property
    def write_root(self) -> Path:
        return self._write_root

    @property
    def script_root(self) -> Path:
        return self._script_root

    @property
    def duckdb_path(self) -> str:
        return str(self._write_root / "analysis.duckdb")

    # ---- 输入输出 ----

    def write_input(self, name: str, data: bytes) -> Path:
        p = self._input_dir / name
        p.write_bytes(data)
        return p

    def write_input_json(self, name: str, obj: dict) -> Path:
        p = self._input_dir / name
        p.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        return p

    def unpack_archives(self) -> None:
        """扫描并自动解压 input/ 目录下的所有 zip, rar, 7z 压缩包到 input/ 目录下，解压后删除原压缩包。"""
        import zipfile
        
        has_rar = False
        try:
            import rarfile
            has_rar = True
        except ImportError:
            pass
            
        has_7z = False
        try:
            import py7zr
            has_7z = True
        except ImportError:
            pass

        for p in list(self._input_dir.iterdir()):
            if not p.is_file():
                continue
            suffix = p.suffix.lower()
            if suffix == ".zip":
                try:
                    with zipfile.ZipFile(p, 'r') as zip_ref:
                        zip_ref.extractall(self._input_dir)
                    p.unlink()
                except Exception:
                    pass
            elif suffix == ".rar":
                if has_rar:
                    try:
                        with rarfile.RarFile(p) as rar_ref:
                            rar_ref.extractall(self._input_dir)
                        p.unlink()
                    except Exception:
                        pass
            elif suffix == ".7z":
                if has_7z:
                    try:
                        with py7zr.SevenZipFile(p, mode='r') as sz_ref:
                            sz_ref.extractall(self._input_dir)
                        p.unlink()
                    except Exception:
                        pass

    def write_context(self, name: str, text: str) -> Path:
        p = self._context_dir / name
        p.write_text(text, encoding="utf-8")
        return p

    def resolve(self, rel: str) -> Path:
        return self.resolve_read(rel)

    def _resolve_from(self, root: Path, rel: str) -> Path:
        resolved = (root / rel).resolve()
        try:
            resolved.relative_to(root.resolve())
        except ValueError:
            raise ValueError(f"路径越界: {rel}")
        return resolved

    def resolve_read(self, rel: str) -> Path:
        if self._read_roots:
            domain, inner = split_domain_path(rel, allowed_domains=self._read_roots.keys())
            return self._resolve_from(self._read_roots[domain], inner)
        return self._resolve_from(self._read_root, rel)

    def resolve_read_domain(self, domain: str, rel: str = "") -> Path:
        if not self._read_roots:
            if domain != DEFAULT_FILE_DOMAINS[0]:
                raise ValueError(f"不支持的文件域: {domain}")
            return self._resolve_from(self._read_root, rel)
        if domain not in self._read_roots:
            raise ValueError(f"不支持的文件域: {domain}")
        return self._resolve_from(self._read_roots[domain], rel)

    def resolve_write(self, rel: str) -> Path:
        return self._resolve_from(self._write_root, rel)

    def format_read_path(self, path: str, domain: Optional[str] = None) -> str:
        if not self._read_roots:
            return path
        if domain is None:
            domain, rel = split_domain_path(path, allowed_domains=self._read_roots.keys())
        else:
            rel = path
        return join_domain_path(domain, rel)

    def resolve_script(self, rel: str) -> Path:
        return self._resolve_from(self._script_root, rel)

    def list_inputs(self) -> list[str]:
        return [p.name for p in self._input_dir.iterdir() if p.is_file()]

    def list_outputs(self) -> list[str]:
        return [p.name for p in self._output_dir.iterdir() if p.is_file()]

    def list_scripts(self) -> list[str]:
        return [p.name for p in self._scripts_dir.iterdir() if p.is_file()]

    def read_output(self, name: str) -> Optional[bytes]:
        p = self._output_dir / name
        return p.read_bytes() if p.exists() else None

    def read_output_json(self, name: str) -> Optional[dict]:
        raw = self.read_output(name)
        return json.loads(raw.decode()) if raw else None

    # ---- parquet 读写 ----

    def write_raw_parquet(self, tables: list[RawTable]) -> list[TableMeta]:
        metas: list[TableMeta] = []
        for t in tables:
            df = pd.DataFrame(t.rows)
            file_stem = t.name.replace(" ", "_").replace("/", "_")
            path = self._tables_dir / f"{file_stem}.parquet"
            df.to_parquet(path, index=False)
            meta = self._df_to_meta(t.name, str(path), df, duckdb_name=file_stem)
            metas.append(meta)
            self._manifest.tables.append(meta)
        self._save_manifest()
        return metas

    def write_parquet(self, name: str, df: pd.DataFrame) -> TableMeta:
        file_stem = name.replace(" ", "_").replace("/", "_")
        path = self._tables_dir / f"{file_stem}.parquet"
        df.to_parquet(path, index=False)
        meta = self._df_to_meta(name, str(path), df, duckdb_name=file_stem)
        self._manifest.tables.append(meta)
        self._save_manifest()
        return meta

    def read_parquet(self, name: str) -> pd.DataFrame:
        for t in self._manifest.tables:
            if t.name == name:
                return pd.read_parquet(t.path)
        raise FileNotFoundError(f"table {name!r} not in workspace")

    def list_parquet_files(self) -> list[str]:
        return [p.name for p in self._tables_dir.glob("*.parquet")]

    # ---- 文本文件 ----

    def write_file(self, filename: str, content: str) -> Path:
        p = self.resolve_write(filename)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return p

    def read_file(self, filename: str) -> str:
        return self.resolve_read(filename).read_text(encoding="utf-8")

    def list_files(self) -> list[str]:
        return [p.name for p in self._read_root.iterdir() if p.is_file()]

    # ---- manifest ----

    @property
    def manifest(self) -> Manifest:
        return self._manifest

    def _save_manifest(self) -> None:
        self._manifest.tables = sorted(self._manifest.tables, key=lambda t: t.name)
        (self._dir / "manifest.json").write_text(
            self._manifest.model_dump_json(indent=2), encoding="utf-8")

    def load_manifest(self) -> Manifest:
        raw = json.loads((self._dir / "manifest.json").read_text(encoding="utf-8"))
        self._manifest = Manifest(**raw)
        return self._manifest

    # ---- 清理（需显式调用，生产不自动删） ----

    def cleanup(self) -> None:
        if self._dir.exists():
            shutil.rmtree(self._dir, ignore_errors=True)

    def cleanup_large_files(self) -> None:
        """仅删除 parquet 和 duckdb，保留 manifest / scripts / trace"""
        for path in list(self._tables_dir.glob("*.parquet")):
            path.unlink(missing_ok=True)
        duckdb_file = self._dir / "analysis.duckdb"
        if duckdb_file.exists():
            duckdb_file.unlink()

    # ---- DuckDB 初始化 ----

    def scan_parquet_tables(self) -> list[dict]:
        """扫描 tables/ 目录，返回可用 parquet 表清单（表名、路径、行数）"""
        tables = []
        for p in sorted(self._tables_dir.glob("*.parquet")):
            df = pd.read_parquet(p)
            tables.append({
                "name": p.stem,
                "path": str(p),
                "columns": list(df.columns),
                "row_count": len(df),
                "size_kb": round(p.stat().st_size / 1024, 1),
            })
        return tables

    def register_all_parquet(self) -> str:
        """扫描 tables/ 并注册所有 parquet 为 DuckDB 视图，返回摘要"""
        import duckdb
        tables = self.scan_parquet_tables()
        con = duckdb.connect(self.duckdb_path)
        try:
            lines = []
            for t in tables:
                safe_name = _quote_ident(t["name"])
                con.execute(
                    f"CREATE OR REPLACE VIEW {safe_name} AS "
                    f"SELECT * FROM read_parquet('{t['path']}')"
                )
                row_count = con.execute(f"SELECT COUNT(*) FROM {safe_name}").fetchone()[0]
                lines.append(f"  {t['name']}: {t['columns']} → {row_count} 行")
            return "DuckDB 初始化完成，已注册:\n" + "\n".join(lines)
        finally:
            con.close()

    def init_duckdb(self) -> str:
        """初始化 DuckDB：扫描 parquet + 注册视图，返回摘要。幂等。"""
        return self.register_all_parquet()

    # ---- 快照 / trace ----

    def save_trace(self, trace: dict) -> None:
        p = self._dir / "agent_trace.json"
        existing = []
        if p.exists():
            existing = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(existing, list):
            existing = []
        existing.append(trace)
        p.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---- 内部 ----

    @staticmethod
    def _df_to_meta(name: str, path: str, df: pd.DataFrame, duckdb_name: str = "") -> TableMeta:
        columns: list[ColumnMeta] = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            samples = df[col].dropna().head(3).tolist()
            null_count = int(df[col].isna().sum())
            columns.append(ColumnMeta(name=col, dtype=dtype, null_count=null_count, sample_values=samples))
        sample_rows = df.head(3).to_dict(orient="records")
        return TableMeta(name=name, duckdb_name=duckdb_name, path=path, columns=columns,
                         row_count=len(df), sample_rows=sample_rows)


def _short_uuid() -> str:
    import uuid
    return uuid.uuid4().hex[:12]


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
