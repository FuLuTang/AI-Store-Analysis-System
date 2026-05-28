"""底层实现：在 SQLite 数据库上执行只读 SQL 查询"""
import json
import sqlite3
from urllib.request import pathname2url
from ...workspace import Workspace


def query_sqlite_impl(ws: Workspace, path: str, sql: str) -> str:
    """对指定的 SQLite 数据库执行只读 SELECT 查询，返回 JSON。"""
    p = ws.resolve(path)
    if not p.exists():
        return json.dumps({"error": f"数据库文件不存在: {path}"}, ensure_ascii=False)
    if not p.is_file():
        return json.dumps({"error": f"不是有效的数据库文件: {path}"}, ensure_ascii=False)

    # 构造只读连接 URI
    db_uri = f"file:{pathname2url(str(p.resolve()))}?mode=ro"
    
    conn = None
    try:
        conn = sqlite3.connect(db_uri, uri=True)
        cursor = conn.cursor()
        
        # 执行 SQL
        cursor.execute(sql)
        
        # 获取列名
        description = cursor.description
        if not description:
            return json.dumps({"ok": True, "message": "SQL 执行成功，无返回数据（非查询语句）"}, ensure_ascii=False)
            
        cols = [d[0] for d in description]
        
        # 限制读取最大行数 (最多提取 101 行以检测是否有更多数据)
        max_rows = 100
        rows = cursor.fetchmany(max_rows + 1)
        has_more = len(rows) > max_rows
        display_rows = rows[:max_rows]
        
        # 转换为 JSON 兼容的 dict 列表
        data = []
        for r in display_rows:
            data.append(dict(zip(cols, r)))
            
        result = {
            "path": path,
            "columns": cols,
            "row_count": len(data),
            "has_more": has_more,
            "data": data
        }
        if has_more:
            result["note"] = f"查询结果超过 {max_rows} 行限制，已自动截断。"
            
        return json.dumps(result, ensure_ascii=False, default=str)
        
    except sqlite3.OperationalError as e:
        # 特别捕获只读数据库的写操作报错或其他语法报错
        return json.dumps({"error": f"SQL 执行异常 (只读限制): {e}"}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": f"执行失败: {e}"}, ensure_ascii=False)
    finally:
        if conn:
            conn.close()
