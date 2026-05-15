"""隔离工作区：每个 report 一个目录，管理 parquet / manifest / 脚本文件。"""

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

import pandas as pd

from packages.agents.models import ColumnMeta, Manifest, RawTable, TableMeta

ARTIFACTS_ROOT = Path("storage/artifacts")


def quote_identifier(name: str) -> str:
    return f'"{name.replace(chr(34), chr(34)+chr(34))}"'


def _ensure_artifacts_root() -> Path:
    ARTIFACTS_ROOT.mkdir(parents=True, exist_ok=True)
    return ARTIFACTS_ROOT


class Workspace:
    """每个 report 一个独立目录，负责读写 parquet 和 manifest。"""

    def __init__(self, report_id: str | None = None):
        self.report_id = report_id or str(uuid.uuid4())
        root = _ensure_artifacts_root()
        self.dir = root / self.report_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self._manifest = Manifest(
            report_id=self.report_id,
            workspace_dir=str(self.dir),
        )

    # ---- parquet 读写 ----

    def write_raw_parquet(
        self, tables: list[RawTable]
    ) -> list[TableMeta]:
        """输入 RawTable[]，写 parquet 到 workspace，返回 TableMeta[]。"""
        metas: list[TableMeta] = []
        for t in tables:
            df = pd.DataFrame(t.rows)
            file_stem = t.name.replace(" ", "_")
            path = self.dir / f"{file_stem}.parquet"
            df.to_parquet(path, index=False)
            meta = self._df_to_meta(t.name, str(path), df)
            metas.append(meta)
            self._manifest.tables.append(meta)
        self._save_manifest()
        return metas

    def write_parquet(self, name: str, df: pd.DataFrame) -> TableMeta:
        """写任意 DataFrame 为 parquet，返回 TableMeta。"""
        file_stem = name.replace(" ", "_")
        path = self.dir / f"{file_stem}.parquet"
        df.to_parquet(path, index=False)
        meta = self._df_to_meta(name, str(path), df)
        self._manifest.tables.append(meta)
        self._save_manifest()
        return meta

    def read_parquet(self, name: str) -> pd.DataFrame:
        """按表名读取 parquet。"""
        for t in self._manifest.tables:
            if t.name == name:
                return pd.read_parquet(t.path)
        raise FileNotFoundError(f"table {name!r} not in workspace")

    def list_parquet_files(self) -> list[str]:
        return [p.name for p in self.dir.glob("*.parquet")]

    # ---- 文本文件 ----

    def write_file(self, filename: str, content: str) -> Path:
        p = self.dir / filename
        p.write_text(content, encoding="utf-8")
        return p

    def read_file(self, filename: str) -> str:
        return (self.dir / filename).read_text(encoding="utf-8")

    def list_files(self) -> list[str]:
        return [p.name for p in self.dir.iterdir() if p.is_file()]

    # ---- manifest ----

    @property
    def manifest(self) -> Manifest:
        return self._manifest

    def _save_manifest(self) -> None:
        self._manifest.tables = sorted(
            self._manifest.tables, key=lambda t: t.name
        )
        (self.dir / "manifest.json").write_text(
            self._manifest.model_dump_json(indent=2), encoding="utf-8"
        )

    def load_manifest(self) -> Manifest:
        raw = json.loads((self.dir / "manifest.json").read_text(encoding="utf-8"))
        self._manifest = Manifest(**raw)
        return self._manifest

    # ---- 清理 ----

    def cleanup(self) -> None:
        if self.dir.exists():
            shutil.rmtree(self.dir)

    # ---- 内部 ----

    @staticmethod
    def _df_to_meta(name: str, path: str, df: pd.DataFrame) -> TableMeta:
        columns: list[ColumnMeta] = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            samples = df[col].dropna().head(3).tolist()
            null_count = int(df[col].isna().sum())
            columns.append(
                ColumnMeta(
                    name=col,
                    dtype=dtype,
                    null_count=null_count,
                    sample_values=samples,
                )
            )
        sample_rows = df.head(3).to_dict(orient="records")
        return TableMeta(
            name=name,
            path=path,
            columns=columns,
            row_count=len(df),
            sample_rows=sample_rows,
        )
