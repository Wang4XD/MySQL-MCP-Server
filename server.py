from contextlib import asynccontextmanager
from typing import Dict, List, Any, Optional
from collections.abc import AsyncIterator
from dataclasses import dataclass
import os
import json
from dotenv import load_dotenv

import aiomysql
from mcp.server.fastmcp import FastMCP, Context

# 加载环境变量
load_dotenv()

# 数据库连接信息
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "autocommit": True  # 自动提交事务
}


@dataclass
class AppContext:
    """应用上下文，包含数据库连接池"""
    pool: aiomysql.Pool


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """管理应用生命周期和数据库连接"""
    # 启动时创建数据库连接池
    print("创建数据库连接池...")
    try:
        pool = await aiomysql.create_pool(**DB_CONFIG)
        print(f"成功连接到数据库 {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}")
        yield AppContext(pool=pool)
    except Exception as e:
        print(f"数据库连接失败: {e}")
        raise
    finally:
        # 关闭时释放连接池
        if 'pool' in locals():
            pool.close()
            await pool.wait_closed()
            print("数据库连接池已关闭")


# 创建MCP服务器实例
mcp = FastMCP(
    "MySQL Explorer", 
    lifespan=app_lifespan, 
    dependencies=["aiomysql", "python-dotenv"]
)


@mcp.resource("schema://tables")
async def get_all_tables(ctx: Context) -> str:
    """获取所有表的列表"""
    pool = ctx.request_context.lifespan_context.pool
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SHOW TABLES")
            tables = await cursor.fetchall()
            return "\n".join(f"- {table[0]}" for table in tables)


@mcp.resource("schema://table/{table_name}")
async def get_table_schema(table_name: str, ctx: Context) -> str:
    """获取指定表的结构"""
    pool = ctx.request_context.lifespan_context.pool
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # 获取表结构
            await cursor.execute(f"DESCRIBE `{table_name}`")
            columns = await cursor.fetchall()
            
            # 获取表的创建语句
            await cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            create_table = await cursor.fetchone()
            
            # 格式化输出
            result = f"# 表结构: {table_name}\n\n"
            result += "## 列信息\n\n"
            result += "| 字段名 | 类型 | 可为空 | 键 | 默认值 | 额外信息 |\n"
            result += "| ------ | ---- | ------ | -- | ------ | -------- |\n"
            
            for col in columns:
                result += f"| {col[0]} | {col[1]} | {col[2]} | {col[3]} | {col[4] if col[4] else 'NULL'} | {col[5]} |\n"
            
            result += f"\n## 创建语句\n\n```sql\n{create_table[1]}\n```\n"
            
            return result


@mcp.resource("schema://")
async def list_schemas(ctx: Context) -> str:
    """列出所有可用的模式资源"""
    pool = ctx.request_context.lifespan_context.pool
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SHOW TABLES")
            tables = await cursor.fetchall()
            
            result = "# 可用的数据库资源\n\n"
            result += "## 表列表\n\n"
            for table in tables:
                result += f"- `schema://table/{table[0]}` - {table[0]}表结构\n"
            
            result += "\n## 其他资源\n\n"
            result += "- `schema://tables` - 所有表的列表\n"
            result += "- `schema://relationships` - 表之间的关系\n"
            
            return result


@mcp.resource("schema://relationships")
async def get_relationships(ctx: Context) -> str:
    """获取表之间的关系信息"""
    pool = ctx.request_context.lifespan_context.pool
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # 查询所有表
            await cursor.execute("SHOW TABLES")
            tables = await cursor.fetchall()
            
            result = "# 数据库关系\n\n"
            has_relations = False
            
            # 对每个表查询外键关系
            for table in tables:
                table_name = table[0]
                await cursor.execute(f"""
                    SELECT 
                        TABLE_NAME as table_name,
                        COLUMN_NAME as column_name,
                        CONSTRAINT_NAME as constraint_name,
                        REFERENCED_TABLE_NAME as referenced_table_name,
                        REFERENCED_COLUMN_NAME as referenced_column_name
                    FROM
                        INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE
                        REFERENCED_TABLE_SCHEMA = '{DB_CONFIG['db']}' AND
                        TABLE_NAME = '{table_name}'
                """)
                
                relations = await cursor.fetchall()
                if relations:
                    has_relations = True
                    result += f"## 表 `{table_name}` 的外键关系\n\n"
                    result += "| 列名 | 约束名 | 引用表 | 引用列 |\n"
                    result += "| ---- | ------ | ------ | ------ |\n"
                    
                    for rel in relations:
                        result += f"| {rel[1]} | {rel[2]} | {rel[3]} | {rel[4]} |\n"
                    
                    result += "\n"
            
            if not has_relations:
                result += "未找到表之间的关系。\n"
                
            return result


@mcp.tool()
async def query_data(sql: str, limit: Optional[int] = 100, ctx: Context) -> str:
    """
    执行只读SQL查询并返回结果
    
    参数:
    - sql: SQL查询语句（仅支持SELECT语句）
    - limit: 可选的结果限制行数，默认100
    """
    # 检查SQL是否为只读查询
    sql = sql.strip()
    if not sql.lower().startswith("select"):
        return "错误: 只支持SELECT查询以保证数据库安全"
    
    # 添加LIMIT子句（如果用户没有指定）
    if "limit" not in sql.lower() and limit is not None:
        sql += f" LIMIT {limit}"
    
    pool = ctx.request_context.lifespan_context.pool
    try:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(sql)
                results = await cursor.fetchall()
                
                # 如果结果为空
                if not results:
                    return "查询执行成功，但没有返回数据。"
                
                # 格式化为Markdown表格
                if isinstance(results, list) and len(results) > 0:
                    # 获取列名
                    columns = list(results[0].keys())
                    
                    # 表头
                    table = "| " + " | ".join(columns) + " |\n"
                    # 分隔行
                    table += "| " + " | ".join(["---" for _ in columns]) + " |\n"
                    
                    # 数据行
                    for row in results:
                        formatted_row = []
                        for col in columns:
                            cell = row[col]
                            # 处理None值和格式化复杂数据类型
                            if cell is None:
                                cell = "NULL"
                            elif isinstance(cell, (dict, list)):
                                cell = json.dumps(cell)
                            formatted_row.append(str(cell).replace("|", "\\|"))
                        table += "| " + " | ".join(formatted_row) + " |\n"
                    
                    return f"查询结果 ({len(results)} 行):\n\n{table}"
                else:
                    return f"查询结果: {results}"
    except Exception as e:
        return f"查询执行错误: {str(e)}"


@mcp.tool()
async def table_statistics(table_name: str, ctx: Context) -> str:
    """
    获取表的统计信息
    
    参数:
    - table_name: 表名
    """
    pool = ctx.request_context.lifespan_context.pool
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # 检查表是否存在
                await cursor.execute("SHOW TABLES LIKE %s", (table_name,))
                if not await cursor.fetchone():
                    return f"错误: 表 '{table_name}' 不存在"
                
                # 获取表行数
                await cursor.execute(f"SELECT COUNT(*) as count FROM `{table_name}`")
                count = await cursor.fetchone()
                
                # 获取表结构
                await cursor.execute(f"DESCRIBE `{table_name}`")
                columns = await cursor.fetchall()
                
                # 计算每列的基本统计数据
                stats = []
                for col in columns:
                    col_name = col[0]
                    col_type = col[1]
                    
                    # 检查列类型，进行相应的统计
                    if any(numeric in col_type.lower() for numeric in ["int", "decimal", "float", "double"]):
                        await cursor.execute(f"""
                            SELECT 
                                MIN(`{col_name}`) as min_val,
                                MAX(`{col_name}`) as max_val,
                                AVG(`{col_name}`) as avg_val,
                                STD(`{col_name}`) as std_val
                            FROM `{table_name}`
                        """)
                        num_stats = await cursor.fetchone()
                        stats.append({
                            "column": col_name,
                            "type": col_type,
                            "min": num_stats[0],
                            "max": num_stats[1],
                            "avg": round(float(num_stats[2]), 2) if num_stats[2] else None,
                            "std": round(float(num_stats[3]), 2) if num_stats[3] else None
                        })
                    elif any(text in col_type.lower() for text in ["char", "text", "varchar"]):
                        await cursor.execute(f"""
                            SELECT 
                                COUNT(DISTINCT `{col_name}`) as unique_count,
                                AVG(LENGTH(`{col_name}`)) as avg_length
                            FROM `{table_name}`
                        """)
                        text_stats = await cursor.fetchone()
                        stats.append({
                            "column": col_name,
                            "type": col_type,
                            "unique_values": text_stats[0],
                            "avg_length": round(float(text_stats[1]), 2) if text_stats[1] else None
                        })
                    else:
                        # 对于其他类型，只获取唯一值数量
                        await cursor.execute(f"""
                            SELECT COUNT(DISTINCT `{col_name}`) as unique_count
                            FROM `{table_name}`
                        """)
                        other_stats = await cursor.fetchone()
                        stats.append({
                            "column": col_name,
                            "type": col_type,
                            "unique_values": other_stats[0]
                        })
                
                # 格式化结果
                result = f"# 表 '{table_name}' 统计信息\n\n"
                result += f"总行数: {count[0]}\n\n"
                
                # 数值型列统计
                num_cols = [s for s in stats if "avg" in s]
                if num_cols:
                    result += "## 数值列统计\n\n"
                    result += "| 列名 | 类型 | 最小值 | 最大值 | 平均值 | 标准差 |\n"
                    result += "| ---- | ---- | ------ | ------ | ------ | ------ |\n"
                    for col in num_cols:
                        result += f"| {col['column']} | {col['type']} | {col['min']} | {col['max']} | {col['avg']} | {col['std']} |\n"
                    result += "\n"
                
                # 文本型列统计
                text_cols = [s for s in stats if "avg_length" in s]
                if text_cols:
                    result += "## 文本列统计\n\n"
                    result += "| 列名 | 类型 | 唯一值数量 | 平均长度 |\n"
                    result += "| ---- | ---- | ---------- | -------- |\n"
                    for col in text_cols:
                        result += f"| {col['column']} | {col['type']} | {col['unique_values']} | {col['avg_length']} |\n"
                    result += "\n"
                
                # 其他类型列统计
                other_cols = [s for s in stats if "avg" not in s and "avg_length" not in s]
                if other_cols:
                    result += "## 其他列统计\n\n"
                    result += "| 列名 | 类型 | 唯一值数量 |\n"
                    result += "| ---- | ---- | ---------- |\n"
                    for col in other_cols:
                        result += f"| {col['column']} | {col['type']} | {col['unique_values']} |\n"
                
                return result
    except Exception as e:
        return f"获取统计信息错误: {str(e)}"


# 定义提示 (Prompts)
@mcp.prompt()
def explore_database() -> str:
    """开始数据库探索"""
    return """
你是一位数据库探索专家。我希望你帮我分析一个MySQL数据库。请按以下步骤进行：

1. 首先，获取并分析所有表的列表
2. 检查重要表的结构
3. 理解表之间的关系
4. 提出一些有价值的查询建议，帮助我理解数据

请在回答中包含你的观察和分析，以及下一步建议。
"""


@mcp.prompt()
def analyze_table(table_name: str) -> str:
    """分析指定表的数据"""
    return f"""
我想深入分析 `{table_name}` 表的数据。请帮我执行以下操作：

1. 获取并分析表结构
2. 生成统计摘要，了解数据分布
3. 查询一些示例数据
4. 提出可能的有趣模式或异常
5. 建议一些可能的数据分析查询

这是我第一次查看此表，所以请提供尽可能详细的分析和见解。
"""


@mcp.prompt()
def create_report(table_name: str, columns: str) -> str:
    """为特定表创建数据报告"""
    return f"""
我需要创建一份关于 `{table_name}` 表的详细报告，特别关注这些列: {columns}

请帮我：
1. 分析这些列的数据分布和质量
2. 识别任何异常值或缺失数据
3. 生成描述性统计信息
4. 提供一些汇总查询的结果
5. 提出基于数据的洞察和建议

最后，请将分析总结为一份结构化报告，包括主要发现和建议。
"""


@mcp.prompt()
def time_series_analysis(table_name: str, time_column: str, value_column: str) -> str:
    """进行时间序列数据分析"""
    return f"""
我需要对 `{table_name}` 表中的时间序列数据进行分析，其中:
- 时间列: `{time_column}`
- 值列: `{value_column}`

请帮我执行以下分析:
1. 检查时间范围和数据完整性
2. 计算按不同时间周期(天、周、月、季度)的聚合统计
3. 识别趋势、季节性模式和异常值
4. 比较不同时间段的变化
5. 提供可视化建议

请提供分析结果和关键见解的总结，以及用于深入分析的SQL查询建议。
"""


@mcp.prompt()
def data_quality_check() -> str:
    """执行数据质量检查"""
    return """
我需要对数据库执行全面的数据质量检查。请帮我：

1. 识别可能包含NULL值的列
2. 检查重复数据
3. 验证日期和数字数据的合理性
4. 检查外键完整性
5. 识别可能的异常值
6. 评估数据的完整性和一致性

请提供每项检查的SQL查询和结果分析，以及如何解决发现的任何数据质量问题的建议。
"""


if __name__ == "__main__":
    mcp.run()
