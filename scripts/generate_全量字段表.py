#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全量字段表生成器 (Master Schema Generator)
==========================================
功能：解析飞书多维表格 .base 文件，生成包含所有表、所有字段的完整文档。
特性：
- 动态解析，自动适应新增字段
- 公式翻译为「表名」.「字段名」格式
- AI 字段单独标注并展示提示词
- 选项、查找引用等配置完整展示

输出：全量字段表.md
"""

import json
import base64
import gzip
import io
import datetime
import re

# ========== 配置 ==========
FILE_PATH = "【演示】成品布管理系统.base"
OUTPUT_PATH = "全量字段表.md"

# 字段类型映射
FIELD_TYPES = {
    1: "文本", 2: "数字", 3: "单选", 4: "多选", 5: "日期",
    7: "复选框", 11: "人员", 13: "电话", 15: "超链接", 17: "附件",
    18: "关联", 19: "查找引用", 20: "公式", 21: "双向关联",
    22: "地理位置", 23: "群组",
    1001: "创建时间", 1002: "修改时间", 1003: "创建人", 1004: "修改人",
    1005: "自动编号", 3001: "按钮"
}


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
    """
    从快照中构建表名和字段名的映射表。
    返回: (table_map, field_map, all_tables)
    - table_map: {table_id: table_name}
    - field_map: {(table_id, field_id): field_name}
    - all_tables: [table_dict, ...]
    """
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
                
            # 提取字段名
            if table_id:
                for field_id, field_def in table.get('fieldMap', {}).items():
                    field_name = field_def.get('name') or field_id
                    field_map[(table_id, field_id)] = field_name

    return table_map, field_map, all_tables



def get_field_type_name(type_id):
    """获取字段类型的中文名称"""
    return FIELD_TYPES.get(type_id, f"未知类型({type_id})")


def translate_formula(formula, current_table_id, table_map, field_map):
    """
    将公式中的 ID 翻译为可读的「表名」.「字段名」格式。
    """
    if not formula:
        return ""
    
    # 替换表引用: bitable::$table[tblXXX] -> 「表名」
    def replace_table(match):
        tid = match.group(1)
        tname = table_map.get(tid)
        if tname:
            return f"「{tname}」"
        # 未找到时返回友好标记
        return f"「[已删除的表:{tid}]」"
    
    formula = re.sub(r'bitable::\$table\[(.*?)\]', replace_table, formula)
    
    # 替换字段引用: $field[fldXXX] 或 $column[fldXXX] -> 「字段名」
    def replace_field(match):
        fid = match.group(1)
        # 先尝试当前表
        fname = field_map.get((current_table_id, fid))
        if fname:
            return f"「{fname}」"
        # 再尝试所有表
        for (tid, f_id), name in field_map.items():
            if f_id == fid:
                return f"「{name}」"
        # 未找到时返回友好标记
        return f"「[未知字段:{fid}]」"
    
    formula = re.sub(r'\$(?:field|column)\[(.*?)\]', replace_field, formula)
    
    # 清理 bitable:: 前缀
    formula = formula.replace("bitable::", "")
    
    return formula


def extract_ai_config(field_def, field_map):
    """
    提取 AI 字段的配置信息，包括提示词。
    返回: (is_ai_field, ai_description)
    """
    # 方式1: ext.ai（飞书内置 AI）
    ext_ai = field_def.get('ext', {})
    if ext_ai:
        ext_ai = ext_ai.get('ai')
    if ext_ai:
        prompts = ext_ai.get('prompt', [])
        prompt_parts = []
        for p in prompts:
            if p.get('type') == 'text':
                prompt_parts.append(p.get('value', ''))
            elif p.get('type') == 'variable':
                val = p.get('value', {})
                if val.get('valueType') == 'field':
                    fid = val.get('value', {}).get('id')
                    fname = fid
                    for (tid, f_id), name in field_map.items():
                        if f_id == fid:
                            fname = name
                            break
                    prompt_parts.append(f"{{字段:{fname}}}")
        return True, "提示词: " + "".join(prompt_parts)
    
    # 方式2: exInfo.customOpenTypeData（自定义/内置 AI）
    ex_info = field_def.get('exInfo', {})
    if not ex_info:
        return False, ""
    
    custom_data = ex_info.get('customOpenTypeData', {})
    if not custom_data:
        return False, ""
    
    # 检查是否是 AI 扩展（多种检测方式）
    is_ai = False
    ai_name = ""
    prompt_text = ""
    source_field = ""
    
    # 方式2a: innerType == 'ai_extract' 或有 aiPrompt
    inner_type = custom_data.get('innerType', '')
    if inner_type == 'ai_extract' or 'aiPrompt' in custom_data.get('fieldConfigValue', {}):
        is_ai = True
    
    # 方式2b: extensionType == 'field_faas' 且 category 包含 'Bitable_AI_Menu'
    extension_type = custom_data.get('extensionType', '')
    categories = custom_data.get('category', [])
    if extension_type == 'field_faas' and 'Bitable_AI_Menu' in categories:
        is_ai = True
        ai_name = custom_data.get('name', 'AI 扩展')
    
    # 方式2c: 有 aiPaymentInfo（表示使用了 AI 付费功能）
    if ex_info.get('aiPaymentInfo', {}).get('enableAIPayment'):
        is_ai = True
    
    if not is_ai:
        return False, ""
    
    # 提取配置信息
    config = custom_data.get('fieldConfigValue', {})
    form_data = config.get('formData', {})
    
    # 提取提示词（多种可能的字段名）
    prompt_text = form_data.get('promptEdit', '')  # 豆包图片理解
    if not prompt_text:
        prompt_text = form_data.get('content', '')  # 其他 AI
    if not prompt_text:
        prompt_text = form_data.get('custom_rules', '')  # 规则
    
    # 提取来源字段
    source_obj = form_data.get('source', {}) or form_data.get('choiceColumn', {})
    source_id = source_obj.get('id', '') if isinstance(source_obj, dict) else ''
    if source_id:
        for (tid, f_id), name in field_map.items():
            if f_id == source_id:
                source_field = name
                break
        if not source_field:
            source_field = source_id
    
    # 构建描述
    desc_parts = []
    if ai_name:
        desc_parts.append(f"类型: {ai_name}")
    if source_field:
        desc_parts.append(f"来源字段: 「{source_field}」")
    if prompt_text:
        # 截取提示词，避免过长
        prompt_preview = prompt_text[:200].replace('\n', ' ')
        if len(prompt_text) > 200:
            prompt_preview += "..."
        desc_parts.append(f"提示词: {prompt_preview}")
    
    return True, " | ".join(desc_parts) if desc_parts else "AI 字段"


def extract_filter_conditions_from_formula(formula, current_table_id, table_map, field_map):
    """
    从公式中提取 FILTER 条件，返回可读描述。
    """
    if not formula:
        return ""
    
    conditions = []
    
    # 提取 FILTER 内的条件
    filter_matches = re.findall(r'\.FILTER\((.*?)\)', formula, re.DOTALL)
    for filter_expr in filter_matches:
        # 等于条件
        eq_matches = re.findall(r'CurrentValue\.\$(?:column|field)\[(.*?)\]\s*=\s*([^&\)]+)', filter_expr)
        for left_fid, right_expr in eq_matches:
            left_fname = field_map.get((current_table_id, left_fid), left_fid)
            # 尝试全局查找
            if left_fname == left_fid:
                for (tid, fid), name in field_map.items():
                    if fid == left_fid:
                        left_fname = name
                        break
            right_translated = translate_formula(right_expr.strip(), current_table_id, table_map, field_map)
            conditions.append(f"「{left_fname}」= {right_translated}")
        
        # 不等于条件
        neq_matches = re.findall(r'CurrentValue\.\$(?:column|field)\[(.*?)\]\s*!=\s*([^&\)]+)', filter_expr)
        for left_fid, right_expr in neq_matches:
            left_fname = field_map.get((current_table_id, left_fid), left_fid)
            if left_fname == left_fid:
                for (tid, fid), name in field_map.items():
                    if fid == left_fid:
                        left_fname = name
                        break
            right_translated = translate_formula(right_expr.strip(), current_table_id, table_map, field_map)
            conditions.append(f"「{left_fname}」≠ {right_translated}")
    
    return " 且 ".join(conditions) if conditions else ""


def extract_field_config(field_def, current_table_id, table_map, field_map):
    """
    提取字段的配置信息（公式、选项、查找引用等）。
    返回: (config_text, is_ai, ai_desc)
    """
    field_type = field_def.get('type')
    prop = field_def.get('property', {})
    
    # 检查是否是 AI 字段
    is_ai, ai_desc = extract_ai_config(field_def, field_map)
    
    # 公式
    if field_type == 20:
        formula = prop.get('formula', '')
        translated = translate_formula(formula, current_table_id, table_map, field_map)
        return f"`{translated}`", is_ai, ai_desc
    
    # 单选/多选
    if field_type in [3, 4]:
        options = prop.get('options', [])
        option_names = [o.get('name', '') for o in options]
        # 检查是否有选项同步规则
        options_rule = prop.get('optionsRule', {})
        if options_rule.get('targetTable'):
            target_tid = options_rule.get('targetTable')
            target_fid = options_rule.get('targetField')
            target_tname = table_map.get(target_tid, target_tid)
            target_fname = field_map.get((target_tid, target_fid), target_fid)
            return f"选项同步自「{target_tname}」的「{target_fname}」", is_ai, ai_desc
        return f"选项: {', '.join(option_names)}", is_ai, ai_desc
    
    # 查找引用
    if field_type == 19:
        filter_info = prop.get('filterInfo', {})
        target_tid = filter_info.get('targetTable')
        target_fid = prop.get('targetField')
        if target_tid:
            # 翻译目标表名，未找到则标记为已删除
            target_tname = table_map.get(target_tid)
            if not target_tname:
                target_tname = f"[已删除的表:{target_tid}]"
            
            # 翻译目标字段名，未找到则标记为已删除
            target_fname = field_map.get((target_tid, target_fid))
            if not target_fname:
                # 尝试全局查找
                for (tid, fid), name in field_map.items():
                    if fid == target_fid:
                        target_fname = name
                        break
            if not target_fname:
                target_fname = f"[已删除的字段:{target_fid}]"
            
            # 基本信息
            result = f"查找引用自「{target_tname}」的「{target_fname}」"
            
            # 提取公式中的筛选条件
            lookup_formula = prop.get('formula', '')
            if lookup_formula:
                # 提取 FILTER 条件
                filter_conds = extract_filter_conditions_from_formula(lookup_formula, current_table_id, table_map, field_map)
                if filter_conds:
                    result += f"<br>筛选条件: {filter_conds}"
            
            return result, is_ai, ai_desc
    
    # 关联/双向关联
    if field_type in [18, 21]:
        target_tid = prop.get('tableId')
        if target_tid:
            target_tname = table_map.get(target_tid)
            if not target_tname:
                target_tname = f"[已删除的表:{target_tid}]"
            return f"关联到「{target_tname}」", is_ai, ai_desc
    
    # 自动编号
    if field_type == 1005:
        rules = prop.get('ruleFieldOptions', [])
        rule_desc = []
        for rule in rules:
            r_type = rule.get('type')
            r_val = rule.get('value', '')
            if r_type == 1: # 创建时间
                rule_desc.append(f"{{创建时间:{r_val}}}")
            elif r_type == 2: # 固定字符
                rule_desc.append(f"\"{r_val}\"")
            elif r_type == 3: # 自增数字
                rule_desc.append(f"{{自增数字:{r_val}位}}")
            else:
                rule_desc.append(f"{{未知规则:{r_val}}}")
        
        if rule_desc:
            return f"编号规则: {' + '.join(rule_desc)}", is_ai, ai_desc
        return "自动编号 (无规则)", is_ai, ai_desc

    # 日期
    if field_type == 5:
        date_fmt = prop.get('dateFormat', '')
        time_fmt = prop.get('timeFormat', '')
        auto_fill = prop.get('autoFill', False)
        
        parts = []
        full_fmt = f"{date_fmt} {time_fmt}".strip()
        if full_fmt:
            parts.append(f"格式: {full_fmt}")
        if auto_fill:
            parts.append("自动填入创建时间")
            
        return " | ".join(parts) if parts else "日期", is_ai, ai_desc

    # 数字
    if field_type == 2:
        formatter = prop.get('formatter', '')
        if formatter:
            return f"数字格式: {formatter}", is_ai, ai_desc
        return "数字", is_ai, ai_desc

    # 按钮
    if field_type == 3001:
        btn_cfg = prop.get('button', {})
        trigger_cfg = prop.get('trigger', {})
        
        title = btn_cfg.get('title', '未命名按钮')
        # color = btn_cfg.get('color') # 1: blue, etc.
        
        trigger_desc = "无触发"
        if trigger_cfg.get('type') == 0:
            trigger_desc = "触发自动化/脚本"
            
        return f"按钮: [{title}] ({trigger_desc})", is_ai, ai_desc

    # 附件
    if field_type == 17:
        return "允许上传附件", is_ai, ai_desc
    
    # 其他有配置的字段
    if prop:
        # 简化显示，避免过长
        prop_str = str(prop)
        if len(prop_str) > 200:
            prop_str = prop_str[:200] + "..."
        return prop_str, is_ai, ai_desc
    
    return "-", is_ai, ai_desc


def generate_document(all_tables, table_map, field_map):
    """生成全量字段表 Markdown 文档"""
    lines = []
    lines.append("# 全量字段表\n")
    lines.append(f"> 生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    lines.append(f"> 数据表总数: {len(all_tables)}\n\n")
    
    # 按表名排序
    sorted_tables = sorted(all_tables, key=lambda t: table_map.get(t.get('meta', {}).get('id'), ''))
    
    for table in sorted_tables:
        table_id = table.get('meta', {}).get('id')
        table_name = table_map.get(table_id, table_id)
        field_map_data = table.get('fieldMap', {})
        
        lines.append(f"## 📊 {table_name}\n")
        lines.append(f"- 表 ID: `{table_id}`\n")
        lines.append(f"- 字段数量: {len(field_map_data)}\n\n")
        
        lines.append("| 字段名称 | 字段类型 | 是否AI字段 | 业务描述 | 完整配置/公式 |\n")
        lines.append("| :--- | :--- | :--- | :--- | :--- |\n")
        
        # 按字段名排序
        sorted_fields = sorted(field_map_data.items(), key=lambda x: x[1].get('name', ''))
        
        for field_id, field_def in sorted_fields:
            field_name = field_def.get('name', field_id)
            field_type = get_field_type_name(field_def.get('type'))
            description = field_def.get('description', {}).get('text', '').replace('\n', ' ')
            
            config, is_ai, ai_desc = extract_field_config(field_def, table_id, table_map, field_map)
            
            # 处理配置文本，避免破坏表格
            config_clean = config.replace('\n', ' ').replace('|', '\\|')
            if len(config_clean) > 500:
                config_clean = config_clean[:500] + "..."
            
            ai_marker = "🤖 是" if is_ai else "否"
            if is_ai and ai_desc:
                config_clean = f"**AI配置**: {ai_desc}<br><br>{config_clean}"
            
            lines.append(f"| **{field_name}** | {field_type} | {ai_marker} | {description} | {config_clean} |\n")
        
        lines.append("\n---\n\n")
    
    return "".join(lines)


def main():
    print("=" * 50)
    print("全量字段表生成器")
    print("=" * 50)
    
    # 读取 .base 文件
    print(f"\n[1/4] 读取文件: {FILE_PATH}")
    try:
        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ 文件读取失败: {e}")
        return
    
    # 解压快照数据
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
    
    # 写入文件
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(document)
    
    print(f"\n✅ 成功生成: {OUTPUT_PATH}")
    print("=" * 50)


if __name__ == "__main__":
    main()
