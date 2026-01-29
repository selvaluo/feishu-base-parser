#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
关联关系图生成器 (Cross-Table Relationship Map Generator)
=========================================================
功能：解析飞书多维表格 .base 文件，生成展示所有跨表关联的文档。
特性：
- 动态解析，自动适应新增字段
- 识别三种关联类型：公式关联、查找引用、选项同步
- 公式翻译为「表名」.「字段名」格式
- 完整展示关联逻辑

输出：关联关系图.md
"""

import json
import base64
import gzip
import io
import datetime
import re

# ========== 配置 ==========
FILE_PATH = "【演示】成品布管理系统.base"
OUTPUT_PATH = "字段关联关系图.md"


def decompress_content(compressed_content):
    """解压 gzip + base64 编码的内容"""
    try:
        if isinstance(compressed_content, str):
            compressed_bytes = base64.b64decode(compressed_content)
        else:
            return None
        with gzip.GzipFile(fileobj=io.BytesIO(compressed_bytes)) as gz:
            return json.loads(gz.read().decode('utf-8'))
    except Exception as e:
        print(f"解压失败: {e}")
        return None


def build_name_registry(snapshot):
    """从快照中构建表名和字段名的映射表"""
    table_map = {}
    field_map = {}
    all_tables = []

    for item in snapshot:
        if 'schema' not in item:
            continue
        
        schema = item['schema']
        
        # 首先从 tableMap 获取表名（这里通常有完整的表名）
        for tid, tinfo in schema.get('tableMap', {}).items():
            if isinstance(tinfo, dict) and tinfo.get('name'):
                table_map[tid] = tinfo['name']
        
        # 然后处理 data 中的表结构
        if 'data' not in schema:
            continue
            
        data = schema['data']
        tables = data.get('tables', [])
        if 'table' in data:
            tables.append(data['table'])
        
        for table in tables:
            if not isinstance(table, dict):
                continue
            
            all_tables.append(table)
            table_id = table.get('meta', {}).get('id')
            table_name = table.get('meta', {}).get('name')
            
            # 只有当 tableMap 中没有这个表时才使用 meta.name
            if table_id and table_id not in table_map:
                table_map[table_id] = table_name or table_id
                
            if table_id:
                for field_id, field_def in table.get('fieldMap', {}).items():
                    field_name = field_def.get('name') or field_id
                    field_map[(table_id, field_id)] = field_name

    return table_map, field_map, all_tables


def get_table_name(table_id, table_map):
    """获取表名，如果找不到则返回友好标记"""
    if not table_id:
        return "未知表"
    name = table_map.get(table_id)
    if name:
        return name
    # 对于找不到的表，返回友好标记但包含ID
    return f"[已删除的表:{table_id}]"


def get_field_name(table_id, field_id, field_map):
    """获取字段名，如果找不到则返回友好标记"""
    if not field_id:
        return "未知字段"
    
    # 先尝试精确匹配
    name = field_map.get((table_id, field_id))
    if name:
        return name
    
    # 再尝试只用字段ID匹配（跨表引用场景）
    for (tid, fid), fname in field_map.items():
        if fid == field_id:
            return fname
    
    # 找不到时返回友好标记但包含ID
    return f"[已删除的字段:{field_id}]"


def translate_formula(formula, current_table_id, table_map, field_map):
    """将公式中的 ID 翻译为可读格式"""
    if not formula:
        return ""
    
    # 替换表引用
    def replace_table(match):
        tid = match.group(1)
        return f"「{get_table_name(tid, table_map)}」"
    
    formula = re.sub(r'bitable::\$table\[(.*?)\]', replace_table, formula)
    
    # 替换字段引用
    def replace_field(match):
        fid = match.group(1)
        return f"「{get_field_name(current_table_id, fid, field_map)}」"
    
    formula = re.sub(r'\$(?:field|column)\[(.*?)\]', replace_field, formula)
    
    # 清理前缀
    formula = formula.replace("bitable::", "")
    
    return formula


def find_cross_table_references(formula, current_table_id):
    """
    检查公式中是否引用了其他表。
    返回引用的表ID列表。
    """
    if not formula:
        return []
    
    # 提取所有表引用
    table_refs = re.findall(r'bitable::\$table\[(.*?)\]', formula)
    
    # 过滤出外部表引用
    external_refs = [tid for tid in table_refs if tid != current_table_id]
    
    return list(set(external_refs))


def extract_filter_conditions(formula, current_table_id, table_map, field_map):
    """
    从公式中提取 FILTER 条件，返回可读的条件描述。
    例如: FILTER(CurrentValue.「字段A」=「表B」.「字段C」) -> 「字段A」 等于 「表B」.「字段C」
    """
    if not formula:
        return ""
    
    # 先翻译整个公式（将所有 ID 转为可读名称）
    translated_formula = translate_formula(formula, current_table_id, table_map, field_map)
    
    # 查找 FILTER 中的条件
    conditions = []
    
    # 提取 FILTER 内的条件表达式（从已翻译的公式提取）
    filter_matches = re.findall(r'\.FILTER\((.*?)\)', translated_formula, re.DOTALL)
    for filter_expr in filter_matches:
        # 查找等于条件: CurrentValue.「字段名」=...
        eq_matches = re.findall(r'CurrentValue\.「([^」]+)」\s*=\s*([^&\)]+)', filter_expr)
        for left_fname, right_expr in eq_matches:
            conditions.append(f"「{left_fname}」= {right_expr.strip()}")
        
        # 查找不等于条件: CurrentValue.「字段名」!="xxx"
        neq_matches = re.findall(r'CurrentValue\.「([^」]+)」\s*!=\s*([^&\)]+)', filter_expr)
        for left_fname, right_expr in neq_matches:
            conditions.append(f"「{left_fname}」≠ {right_expr.strip()}")
    
    if conditions:
        return "筛选条件: " + " 且 ".join(conditions)
    return ""


def extract_relationships(table, table_id, table_map, field_map):
    """
    提取单个表中所有与外部表有关联的字段。
    返回关联字段列表，每个元素为字典：
    {
        'field_name': 字段名,
        'relation_type': 关联类型（公式关联/查找引用/选项同步）,
        'target_table': 目标表名,
        'target_field': 目标字段名,
        'logic': 详细逻辑描述,
        'formula': 完整公式（如果有）,
        'filter_conditions': 筛选条件（如果有）
    }
    """
    relationships = []
    field_map_data = table.get('fieldMap', {})
    
    for field_id, field_def in field_map_data.items():
        field_name = field_def.get('name', field_id)
        field_type = field_def.get('type')
        prop = field_def.get('property', {})
        
        # 1. 公式关联 (type=20)
        if field_type == 20:
            formula = prop.get('formula', '')
            external_refs = find_cross_table_references(formula, table_id)
            
            if external_refs:
                # 有外部表引用
                target_tables = [get_table_name(tid, table_map) for tid in external_refs]
                translated_formula = translate_formula(formula, table_id, table_map, field_map)
                filter_conds = extract_filter_conditions(formula, table_id, table_map, field_map)
                
                logic = "通过公式计算引用外部表数据"
                if filter_conds:
                    logic += f"<br>{filter_conds}"
                
                relationships.append({
                    'field_name': field_name,
                    'relation_type': '公式关联',
                    'target_table': ', '.join(target_tables),
                    'target_field': '-',
                    'logic': logic,
                    'formula': translated_formula
                })
        
        # 2. 查找引用 (type=19)
        elif field_type == 19:
            filter_info = prop.get('filterInfo', {})
            target_tid = filter_info.get('targetTable')
            target_fid = prop.get('targetField')
            
            if target_tid:
                target_tname = get_table_name(target_tid, table_map)
                target_fname = get_field_name(target_tid, target_fid, field_map)
                
                # 提取完整的查找公式
                lookup_formula = prop.get('formula', '')
                translated = translate_formula(lookup_formula, table_id, table_map, field_map) if lookup_formula else ''
                filter_conds = extract_filter_conditions(lookup_formula, table_id, table_map, field_map) if lookup_formula else ''
                
                logic = f"从「{target_tname}」的「{target_fname}」字段获取数据"
                if filter_conds:
                    logic += f"<br>{filter_conds}"
                
                relationships.append({
                    'field_name': field_name,
                    'relation_type': '查找引用',
                    'target_table': target_tname,
                    'target_field': target_fname,
                    'logic': logic,
                    'formula': translated
                })
        
        # 3. 关联/双向关联 (type=18, 21)
        elif field_type in [18, 21]:
            target_tid = prop.get('tableId')
            if target_tid:
                target_tname = get_table_name(target_tid, table_map)
                relation_type = '双向关联' if field_type == 21 else '单向关联'
                
                relationships.append({
                    'field_name': field_name,
                    'relation_type': relation_type,
                    'target_table': target_tname,
                    'target_field': '-',
                    'logic': f"与「{target_tname}」建立记录关联",
                    'formula': ''
                })
        
        # 4. 选项同步 (单选/多选 type=3, 4 且有 optionsRule)
        elif field_type in [3, 4]:
            options_rule = prop.get('optionsRule', {})
            target_tid = options_rule.get('targetTable')
            target_fid = options_rule.get('targetField')
            
            if target_tid:
                target_tname = get_table_name(target_tid, table_map)
                target_fname = get_field_name(target_tid, target_fid, field_map)
                
                relationships.append({
                    'field_name': field_name,
                    'relation_type': '选项同步',
                    'target_table': target_tname,
                    'target_field': target_fname,
                    'logic': f"下拉选项实时同步自「{target_tname}」的「{target_fname}」",
                    'formula': ''
                })
    
    return relationships


def generate_document(all_tables, table_map, field_map):
    """生成关联关系图 Markdown 文档"""
    lines = []
    lines.append("# 关联关系图\n")
    lines.append(f"> 生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    lines.append(f"> 数据表总数: {len(all_tables)}\n\n")
    
    lines.append("本文档列出了系统中所有具有 **跨表关联** 的字段，包括：\n")
    lines.append("- **公式关联**: 通过公式引用其他表的数据进行计算\n")
    lines.append("- **查找引用**: 从关联记录中获取特定字段的值\n")
    lines.append("- **选项同步**: 下拉选项从其他表字段动态获取\n")
    lines.append("- **记录关联**: 与其他表建立记录级别的关联\n\n")
    
    total_relationships = 0
    tables_with_relations = 0
    
    # 按表名排序
    sorted_tables = sorted(all_tables, key=lambda t: table_map.get(t.get('meta', {}).get('id'), ''))
    
    for table in sorted_tables:
        table_id = table.get('meta', {}).get('id')
        table_name = table_map.get(table_id, table_id)
        
        relationships = extract_relationships(table, table_id, table_map, field_map)
        
        if not relationships:
            continue
        
        tables_with_relations += 1
        total_relationships += len(relationships)
        
        lines.append(f"## 📊 {table_name}\n")
        lines.append(f"- 表 ID: `{table_id}`\n")
        lines.append(f"- 对外关联字段数: {len(relationships)}\n\n")
        
        lines.append("| 字段名称 | 关联类型 | 目标表 | 目标字段 | 逻辑说明 |\n")
        lines.append("| :--- | :--- | :--- | :--- | :--- |\n")
        
        for rel in sorted(relationships, key=lambda x: x['field_name']):
            logic = rel['logic']
            if rel['formula']:
                # 添加可展开的公式详情
                formula_clean = rel['formula'].replace('\n', ' ').replace('|', '\\|')
                if len(formula_clean) > 100:
                    logic += f"<br><details><summary>查看完整公式</summary>`{formula_clean}`</details>"
                else:
                    logic += f"<br>公式: `{formula_clean}`"
            
            lines.append(f"| **{rel['field_name']}** | {rel['relation_type']} | {rel['target_table']} | {rel['target_field']} | {logic} |\n")
        
        lines.append("\n---\n\n")
    
    # 添加统计摘要到开头
    summary = f"**统计摘要**: 共 {tables_with_relations} 张表存在跨表关联，涉及 {total_relationships} 个关联字段。\n\n"
    lines.insert(4, summary)
    
    return "".join(lines)


def main():
    print("=" * 50)
    print("关联关系图生成器")
    print("=" * 50)
    
    # 读取文件
    print(f"\n[1/4] 读取文件: {FILE_PATH}")
    try:
        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ 文件读取失败: {e}")
        return
    
    # 解压快照
    print("[2/4] 解压快照数据...")
    snapshot = decompress_content(data.get('gzipSnapshot'))
    if not snapshot:
        print("❌ 快照解压失败")
        return
    
    # 构建名称映射
    print("[3/4] 构建名称映射...")
    table_map, field_map, all_tables = build_name_registry(snapshot)
    print(f"    - 发现 {len(table_map)} 张表")
    print(f"    - 发现 {len(field_map)} 个字段")
    
    # 生成文档
    print("[4/4] 生成文档...")
    document = generate_document(all_tables, table_map, field_map)
    
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(document)
    
    print(f"\n✅ 成功生成: {OUTPUT_PATH}")
    print("=" * 50)


if __name__ == "__main__":
    main()
