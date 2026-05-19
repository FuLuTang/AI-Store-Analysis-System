"""check_plan 的检查执行器：验证每个 step 的产物"""
from pathlib import Path
from glob import glob

from ...workspace import Workspace


def run_step_checks(ws: Workspace, step: dict) -> tuple[list[str], list[str]]:
    """执行 checks 列表，返回 (passed, failed) 各一条描述字符串。"""
    passed = []
    failed = []

    for check in step.get("checks", []):
        typ = check["type"]
        try:
            if typ == "file_exists":
                path = ws.resolve(check["path"])
                if path.exists():
                    passed.append(f"file_exists: {check['path']}")
                else:
                    failed.append(f"file_exists: {check['path']} 不存在")

            elif typ == "glob_exists":
                pattern = str(ws.resolve(check["pattern"]))
                matches = glob(pattern)
                if matches:
                    passed.append(f"glob_exists: {check['pattern']} -> {len(matches)} 个文件")
                else:
                    failed.append(f"glob_exists: {check['pattern']} 未匹配到任何文件")

            elif typ == "parquet_non_empty":
                pattern = str(ws.resolve(check["pattern"]))
                matches = glob(pattern)
                if not matches:
                    failed.append(f"parquet_non_empty: {check['pattern']} 无匹配文件")
                else:
                    import pandas as pd
                    errors = []
                    for f in matches:
                        try:
                            df = pd.read_parquet(f)
                            if len(df) == 0:
                                errors.append(f"{Path(f).name} 行数为 0")
                        except Exception as e:
                            errors.append(f"{Path(f).name} 读取失败 - {e}")
                    if errors:
                        for e in errors:
                            failed.append(f"parquet_non_empty: {e}")
                    else:
                        passed.append(f"parquet_non_empty: {check['pattern']} 所有文件行数 > 0")

            elif typ == "duckdb_query_ok":
                import duckdb
                con = duckdb.connect(str(ws.duckdb_path))
                try:
                    con.execute(check["sql"]).fetchall()
                    passed.append(f"duckdb_query_ok: SQL 执行成功")
                except Exception as e:
                    failed.append(f"duckdb_query_ok: SQL 执行失败 - {e}")
                finally:
                    con.close()

            elif typ == "validate_result":
                path = ws.resolve(check["path"])
                if not path.exists():
                    failed.append(f"validate_result: {check['path']} 不存在")
                else:
                    from .validate_impl import validate_result_impl
                    import json
                    try:
                        data = json.loads(path.read_text(encoding="utf-8"))
                        result = validate_result_impl(data)
                        if result["valid"]:
                            passed.append(f"validate_result: {check['path']} 验证通过")
                        else:
                            failed.append(f"validate_result: {check['path']} 验证失败 - {result['errors']}")
                    except json.JSONDecodeError as e:
                        failed.append(f"validate_result: {check['path']} JSON 解析失败 - {e}")
                    except Exception as e:
                        failed.append(f"validate_result: {check['path']} 执行异常 - {e}")

            else:
                failed.append(f"未知检查类型: {typ}")

        except Exception as e:
            failed.append(f"{typ} 检查异常: {e}")

    return passed, failed
