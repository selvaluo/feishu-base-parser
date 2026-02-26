#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动化地图生成器 (Automation Map Generator)
============================================
功能：解析飞书多维表格 .base 文件，生成包含所有自动化工作流的完整文档。
特性：
- 动态解析，自动适应新增工作流
- 显示工作流名称和唯一ID
- 显示启用/禁用状态
- 深度解析每个步骤的判断逻辑和条件
- 显示修改的字段和具体值

输出：自动化地图.md
"""

import json
import base64
import gzip
import io
import datetime
import re

import glob
import sys
import os

# ========== 配置 ==========
OUTPUT_PATH = "自动化工作流.md"

def find_base_file():
    """在当前目录下查找 .base 文件"""
    base_files = glob.glob("*.base")
    if not base_files:
        print("❌ 错误：当前目录下未找到 .base 文件，请先导出并上传您的飞书多维表格 .base 文件到本目录。")
        sys.exit(1)
    elif len(base_files) > 1:
        print(f"❌ 错误：当前目录下找到多个 .base 文件 {base_files}，请仅保留一个需要解析的文件。")
        sys.exit(1)
    
    return base_files[0]

# 操作符翻译 (包含 snake_case 和 camelCase 两种格式)
OPERATORS = {
    # 等于/不等于
    "is": "等于",
    "is_not": "不等于",
    "isNot": "不等于",
    # 包含/不包含
    "contains": "包含",
    "does_not_contain": "不包含",
    "doesNotContain": "不包含",
    # 空/非空
    "is_empty": "为空",
    "isEmpty": "为空",
    "is_not_empty": "不为空",
    "isNotEmpty": "不为空",
    # 大小比较
    "greater_than": "大于",
    "isGreater": "大于",
    "less_than": "小于",
    "isLess": "小于",
    "greater_than_or_equal": "大于等于",
    "isGreaterEqual": "大于等于",
    "less_than_or_equal": "小于等于",
    "isLessEqual": "小于等于",
    # 日期比较
    "is_before": "早于",
    "isBefore": "早于",
    "is_after": "晚于",
    "isAfter": "晚于",
    "is_on_or_before": "不晚于",
    "isOnOrBefore": "不晚于",
    "is_on_or_after": "不早于",
    "isOnOrAfter": "不早于",
    # 其他
    "isAnyOf": "是以下任一",
    "isNoneOf": "不是以下任一"
}

# 动作类型翻译
ACTION_TYPES = {
    "AddRecordAction": "新增记录",
    "UpdateRecordAction": "修改记录",
    "FindRecordAction": "查找记录",
    "IfElseBranch": "条件判断（If/Else）",
    "CustomAction": "自定义动作",
    "SendNotification": "发送通知",
    "SendEmail": "发送邮件",
    "DeleteRecordAction": "删除记录",
    "UpdateRecord": "修改记录",
    "AddRecord": "新增记录",
    "FindRecord": "查找记录"
}

# 触发器类型翻译
TRIGGER_TYPES = {
    "AddRecordTrigger": "新增记录时触发",
    "SetRecordTrigger": "记录更新时触发",
    "TimerTrigger": "定时触发",
    "ButtonTrigger": "按钮点击触发",
    "FormSubmitTrigger": "表单提交时触发",
    "ChangeRecordTrigger": "新增/修改的记录满足条件时触发",
    "ChangeRecordNewSatisfyTrigger": "新增/修改的记录满足条件时触发"
}


def decompress_content(compressed_content):
    """解压 gzip 压缩的数据 (支持 int列表 或 Base64字符串)"""
    if not compressed_content:
        return None
        
    # 情况1: List of integers
    if isinstance(compressed_content, list):
        try:
            compressed_bytes = bytes(compressed_content)
            with gzip.GzipFile(fileobj=io.BytesIO(compressed_bytes)) as gz:
                return json.loads(gz.read().decode('utf-8'))
        except Exception as e:
            # print(f"List解压失败: {e}")
            pass

    # 情况2: Base64 String
    if isinstance(compressed_content, str):
        try:
            import base64
            decoded = base64.b64decode(compressed_content)
            with gzip.GzipFile(fileobj=io.BytesIO(decoded)) as gz:
                return json.loads(gz.read().decode('utf-8'))
        except Exception as e:
            # print(f"Base64解压失败: {e}")
            pass
            
    return None


def build_name_registry(snapshot):
    """从快照中构建表名和字段名的映射表"""
    table_map = {}
    field_map = {}
    option_map = {}  # (table_id, field_id, option_id) -> option_name

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
            
            table_id = table.get('meta', {}).get('id')
            table_name = table.get('meta', {}).get('name')
            
            # 只有当 tableMap 中没有这个表时才使用 meta.name
            if table_id and table_id not in table_map:
                table_map[table_id] = table_name or table_id
                
            if table_id:
                for field_id, field_def in table.get('fieldMap', {}).items():
                    field_name = field_def.get('name') or field_id
                    field_map[(table_id, field_id)] = field_name
                    
                    # 提取选项 - 使用简单的 opt_id 作为键（选项ID全局唯一）
                    for opt in field_def.get('property', {}).get('options', []):
                        opt_id = opt.get('id')
                        opt_name = opt.get('name')
                        if opt_id:
                            option_map[opt_id] = opt_name
                            
    return table_map, field_map, option_map


def resolve_table_id(ref_id, wf_table_map, global_table_map):
    """
    解析工作流中的表引用ID到实际表名。
    工作流中常用 ref_tblXXX 格式，需要通过 Extra.TableMap 映射到实际 ID。
    """
    if not ref_id:
        return "未知表"
    
    # 去除可能的引号
    if isinstance(ref_id, str):
        ref_id = ref_id.strip('"').strip('\\"')
    
    # 先检查工作流的映射表
    if wf_table_map and ref_id in wf_table_map:
        real_id = wf_table_map[ref_id].get('TableID', '').strip('"')
        if real_id in global_table_map:
            return global_table_map[real_id]
        return real_id if real_id else ref_id
    
    # 再检查全局表
    if ref_id in global_table_map:
        return global_table_map[ref_id]
    
    return f"[已删除的表:{ref_id}]"


def resolve_field_id(ref_fid, wf_table_map, field_map):
    """解析工作流中的字段引用ID到实际字段名"""
    if not ref_fid:
        return "未知字段"
    
    if isinstance(ref_fid, str):
        ref_fid = ref_fid.strip('"')
    
    # 处理 ref_ref_tblXXXX_fldYYYY 或 ref_tblXXXX_fldYYYY 格式
    if isinstance(ref_fid, str) and (ref_fid.startswith('ref_ref_tbl') or ref_fid.startswith('ref_tbl')):
        # 提取 tblXXXXX 和 fldYYYYY
        import re
        # 匹配 ref_tbl 或 ref_ref_tbl
        match = re.search(r'(tbl[^_]+)_(fld.+)', ref_fid)
        if match:
            real_tid = match.group(1)
            real_fid = match.group(2)
            
            # 1. 尝试从 wf_table_map 查找真实表ID (如果是 ref_tbl 引用)
            # 构造 ref_tblXXX key
            ref_key = f"ref_{real_tid}"
            if wf_table_map and ref_key in wf_table_map:
                mapped_tid = wf_table_map[ref_key].get('TableID', '').strip('"')
                fname = field_map.get((mapped_tid, real_fid))
                if fname:
                    return fname
            
            # 2. 直接尝试全局查找 (假设 real_tid 就是真实 ID)
            fname = field_map.get((real_tid, real_fid))
            if fname:
                return fname
            
            if fname:
                return fname
            
            # 3. 忽略表ID，只匹配字段ID (兜底)
            for (tid, fid), name in field_map.items():
                if fid == real_fid:
                    return name
    
    # 尝试从映射表中解析 (原有逻辑)
    for ref_tid, info in (wf_table_map or {}).items():
        field_mapping = info.get('FieldMap', {})
        if ref_fid in field_mapping:
            real_fid = field_mapping[ref_fid]
            real_tid = info.get('TableID', '').strip('"')
            fname = field_map.get((real_tid, real_fid))
            if fname:
                return fname
    
    # 直接查找
    for (tid, fid), name in field_map.items():
        if fid == ref_fid:
            return name
    
    # 找不到时返回友好标记但包含ID
    return f"[已删除的字段:{ref_fid}]"


def parse_condition(condition, wf_table_map, table_map, field_map, option_map):
    """解析条件对象，返回可读的条件描述"""
    if not isinstance(condition, dict):
        return str(condition)
    
    field_id = condition.get('fieldId', '')
    operator = condition.get('operator', '')
    value = condition.get('value') or condition.get('matchValue', {}).get('value')
    
    field_name = resolve_field_id(field_id, wf_table_map, field_map)
    op_name = OPERATORS.get(operator, operator)
    
    # 处理值
    if isinstance(value, dict) and value.get('type') == 'ref':
        # 处理引用类型的值 (例如引用步骤结果)
        value_str = format_value(value, option_map, 0, wf_table_map, field_map)
    else:
        value_str = format_value(value, option_map, 0, wf_table_map, field_map)
    
    # 对于 is_empty / is_not_empty 操作符，不需要显示值
    if operator in ['is_empty', 'is_not_empty']:
        return f"「{field_name}」{op_name}"
    
    return f"「{field_name}」{op_name} \"{value_str}\""


def parse_trigger_filter_condition(condition_obj, wf_table_map, field_map, option_map):
    """解析触发器的筛选条件 (step.next[0].condition 结构)"""
    if not condition_obj:
        return ""
    
    conjunction = condition_obj.get('conjunction', 'and')
    conditions = condition_obj.get('conditions', [])
    
    if not conditions:
        return ""
    
    parsed_parts = []
    for cond in conditions:
        # 可能是嵌套的条件组
        if 'conditions' in cond:
            nested = parse_trigger_filter_condition(cond, wf_table_map, field_map, option_map)
            if nested:
                parsed_parts.append(f"({nested})")
        else:
            # 单个条件
            field_id = cond.get('fieldId', '')
            operator = cond.get('operator', '')
            value = cond.get('value', [])
            
            # 解析字段名
            field_name = resolve_field_id(field_id, wf_table_map, field_map)
            
            # 翻译操作符
            op_name = OPERATORS.get(operator, operator)
            
            # 处理值
            if isinstance(value, list):
                translated_vals = []
                for v in value:
                    if isinstance(v, str) and v.startswith('opt'):
                        translated_vals.append(option_map.get(v, v))
                    else:
                        translated_vals.append(str(v))
                value_str = ', '.join(translated_vals) if translated_vals else "[空]"
            else:
                value_str = str(value) if value else "[空]"
            
            # 对于空/非空操作符，不显示值
            if operator in ['isEmpty', 'isNotEmpty', 'is_empty', 'is_not_empty']:
                parsed_parts.append(f"「{field_name}」{op_name}")
            else:
                parsed_parts.append(f"「{field_name}」{op_name} \"{value_str}\"")
    
    # 连接条件
    connector = " 且 " if conjunction == "and" else " 或 "
    return connector.join(parsed_parts)



def parse_conditions_list(conditions, wf_table_map, table_map, field_map, option_map, conjunction="and"):
    """解析条件列表，返回可读的条件组合描述"""
    if not conditions:
        return "无条件"
    
    parsed = []
    for cond in conditions:
        parsed.append(parse_condition(cond, wf_table_map, table_map, field_map, option_map))
    
    connector = " 且 " if conjunction == "and" else " 或 "
    return connector.join(parsed)


def parse_field_values(values, wf_table_map, field_map, option_map):
    """解析字段值设置列表，并将选项ID翻译为中文名称"""
    if not values:
        return []
    
    result = []
    for v in values:
        if not isinstance(v, dict):
            continue
        field_id = v.get('fieldId', '')
        field_name = resolve_field_id(field_id, wf_table_map, field_map)
        
        value_type = v.get('valueType', '')
        value = v.get('value', '')
        
        # 简化值的显示
        if isinstance(value, list):
            if len(value) > 0 and isinstance(value[0], dict):
                # 可能是公式引用
                if value[0].get('type') == 'ref' and value[0].get('tagType') == 'formula':
                    value_str = f"[公式计算: {value[0].get('title', '未知')}]"
                elif value[0].get('type') == 'ref' and value[0].get('tagType') == 'step':
                    step_num = value[0].get('stepNum', '?')
                    # 尝试提取具体引用的字段名
                    ref_fields = value[0].get('fields', [])
                    if ref_fields and isinstance(ref_fields, list) and len(ref_fields) > 0:
                        ref_field_id = ref_fields[0].get('fieldId', '') if isinstance(ref_fields[0], dict) else ''
                        if ref_field_id:
                            ref_field_name = resolve_field_id(ref_field_id, wf_table_map, field_map)
                            value_str = f"[步骤{step_num}的「{ref_field_name}」]"
                        else:
                            value_str = f"[步骤{step_num}的结果]"
                    else:
                        value_str = f"[步骤{step_num}的结果]"
                elif value[0].get('type') == 'ref' and value[0].get('tagType') == 'loop':
                    # 循环引用
                    step_num = value[0].get('stepNum', '?')
                    ref_fields = value[0].get('fields', [])
                    if ref_fields and isinstance(ref_fields, list) and len(ref_fields) > 0:
                        ref_field_id = ref_fields[0].get('fieldId', '') if isinstance(ref_fields[0], dict) else ''
                        if ref_field_id:
                            ref_field_name = resolve_field_id(ref_field_id, wf_table_map, field_map)
                            value_str = f"[步骤{step_num}循环的「{ref_field_name}」]"
                        else:
                             value_str = f"[步骤{step_num}的循环当前记录]"
                    else:
                        value_str = f"[步骤{step_num}的循环当前记录]"
                else:
                    value_str = str(value)
            else:
                # 可能是选项ID列表
                translated = []
                for item in value:
                    if isinstance(item, str) and item.startswith('opt'):
                        # 尝试翻译选项ID
                        opt_name = option_map.get(item)
                        if opt_name:
                            translated.append(opt_name)
                        else:
                            translated.append(item)
                    else:
                        translated.append(str(item))
                value_str = ', '.join(translated) if translated else str(value)
        elif isinstance(value, str) and value.startswith('opt'):
            # 单个选项ID
            value_str = option_map.get(value, value)
        elif isinstance(value, dict):
            value_str = str(value)
        else:
            value_str = str(value) if value else "[空]"
        
        result.append(f"- 「{field_name}」= {value_str}")
    
    return result


def format_value(value, option_map=None, depth=0, wf_table_map=None, field_map=None):
    """格式化任意值，处理空值、选项翻译和递归结构"""
    if value == "":
        return "[空值]"
    if value is None:
        return "[空]"
    
    if isinstance(value, str):
        if value.startswith('opt') and option_map:
            return option_map.get(value, value)
        return value
        
    if isinstance(value, list):
        if not value:
            return "[空列表]"
        
        # 预先格式化所有项
        formatted_items = [format_value(v, option_map, depth+1, wf_table_map, field_map) for v in value]
        
        # 如果所有项都是简短的（不包含换行且长度适中），则使用行内显示
        if all('\n' not in item and len(item) < 50 for item in formatted_items):
            return ", ".join(formatted_items)
        
        # 否则使用列表显示
        indent = "  " * depth
        lines = []
        for item in formatted_items:
            lines.append(f"\n{indent}- {item}")
        return "".join(lines)
        
    if isinstance(value, dict):
        if not value:
            return "{}"
        
        # 特殊结构处理
        if value.get('type') == 'ref':
            tag = value.get('tagType', '未知')
            step = value.get('stepNum', '?')
            fields = value.get('fields', [])
            
            # 尝试提取具体引用的字段名
            field_name_desc = ""
            if fields and isinstance(fields, list) and len(fields) > 0:
                field_info = fields[0]
                if isinstance(field_info, dict):
                    field_id = field_info.get('fieldId', '')
                    if field_id:
                        if field_map:
                            fn = resolve_field_id(field_id, wf_table_map, field_map)
                            field_name_desc = f"的「{fn}」"
                        else:
                            field_name_desc = f"的[未知字段:{field_id}]"

            # 尝试从 path 中提取字段 (用于 Loop 等场景)
            if not field_name_desc:
                path = value.get('path', [])
                if path and isinstance(path, list):
                    for p in path:
                        if isinstance(p, dict) and p.get('type') == 'Field':
                            fid = p.get('value', '')
                            if fid:
                                if field_map:
                                    fn = resolve_field_id(fid, wf_table_map, field_map)
                                    field_name_desc = f"的「{fn}」"
                                else:
                                    field_name_desc = f"的[未知字段:{fid}]"
                                break
                        elif isinstance(p, dict) and p.get('type') == 'RecordAttr':
                            attr = p.get('value', '')
                            attr_map = {'recordId': '记录ID', 'record': '记录'}
                            field_name_desc = f"的{attr_map.get(attr, attr)}"
                            break
            
            # 特殊处理 formula
            if tag == 'formula':
                 return f"[公式计算: {value.get('title', '未知')}]"
            
            # 特殊处理 system (系统变量)
            if tag == 'system':
                sys_type = value.get('systemType', 'unknown')
                sys_map = {'viewUrl': '视图链接', 'recordUrl': '记录链接'}
                return f"[系统变量:{sys_map.get(sys_type, sys_type)}]"
            
            # 特殊处理 RecordAttribute (记录属性)
            if tag == 'RecordAttribute':
                attr = value.get('attribute', 'unknown')
                attr_map = {'recordId': '记录ID', 'record': '记录'}
                return f"[步骤{step}的{attr_map.get(attr, attr)}]"

            # 根据 tagType 生成更友好的描述
            tag_map = {
                'loop': '循环当前记录',
                'step': '结果',
                'trigger': '触发记录',
                'RecordAttribute': '记录属性'
            }
            tag_desc = tag_map.get(tag, tag)
            
            if tag == 'loop':
                if field_name_desc:
                    return f"[步骤{step}循环{field_name_desc}]"
                return f"[步骤{step}的循环当前记录]"
            
            if field_name_desc:
                return f"[步骤{step}{field_name_desc}]"
                
            return f"[步骤{step}的{tag_desc}]"
            
        items = []
        for k, v in value.items():
            items.append(f"{k}: {format_value(v, option_map, depth+1, wf_table_map, field_map)}")
        return "{ " + ", ".join(items) + " }"
        
    return str(value)


def parse_step(step, wf_table_map, table_map, field_map, option_map, step_id_map, step_index=0, depth=0):
    """解析单个工作流步骤，返回 Markdown 格式的描述"""
    indent = "  " * depth
    lines = []
    
    step_type = step.get('type', '未知类型')
    step_title = step.get('stepTitle') or ACTION_TYPES.get(step_type, step_type)
    step_data = step.get('data', {})
    
    # 记录已处理的键，以便最后显示未处理的配置
    processed_keys = set()
    
    # 显示步骤序号
    idx_str = f" {step_index}" if step_index > 0 else ""
    lines.append(f"{indent}- **步骤{idx_str}: {step_title}**")
    
    # 涉及的表
    table_id = step_data.get('tableId')
    if table_id:
        table_name = resolve_table_id(table_id, wf_table_map, table_map)
        lines.append(f"{indent}  - 涉及表: 「{table_name}」")
        processed_keys.add('tableId')
    
    # ============ 触发器处理 ============
    
    # ChangeRecordTrigger - 有字段条件
    if step_type == 'ChangeRecordTrigger':
        fields = step_data.get('fields', [])
        processed_keys.add('fields')
        if fields:
            cond_parts = []
            for f in fields:
                fid = f.get('fieldId', '')
                fname = resolve_field_id(fid, wf_table_map, field_map)
                op = f.get('operator', '')
                value = f.get('value', [])
                op_name = OPERATORS.get(op, op)  # 使用全局操作符翻译表
                if op in ['isEmpty', 'isNotEmpty']:
                    cond_parts.append(f"「{fname}」{op_name}")
                else:
                    # 翻译选项ID
                    if isinstance(value, list):
                        translated_vals = []
                        for v in value:
                            if isinstance(v, str) and v.startswith('opt'):
                                translated_vals.append(option_map.get(v, v))
                            else:
                                translated_vals.append(str(v))
                        val_str = ', '.join(translated_vals)
                    else:
                        val_str = option_map.get(value, value) if isinstance(value, str) and value.startswith('opt') else str(value)
                    
                    if val_str == "": val_str = "[空值]"
                    cond_parts.append(f"「{fname}」{op_name} \"{val_str}\"")
            lines.append(f"{indent}  - 触发条件: {' 且 '.join(cond_parts)}")
        
        trigger_list = step_data.get('triggerControlList', [])
        processed_keys.add('triggerControlList')
        if trigger_list:
            trigger_map = {
                'pasteUpdate': '粘贴更新',
                'automationBatchUpdate': '自动化批量更新',
                'appendImport': '追加导入',
                'openAPIBatchUpdate': 'API批量更新'
            }
            triggers = [trigger_map.get(t, t) for t in trigger_list]
            lines.append(f"{indent}  - 触发来源: {', '.join(triggers)}")
    
    # AddRecordTrigger
    if step_type == 'AddRecordTrigger':
        trigger_list = step_data.get('triggerControlList', [])
        processed_keys.add('triggerControlList')
        
        watched_fid = step_data.get('watchedFieldId')
        processed_keys.add('watchedFieldId')
        
        if watched_fid:
            fname = resolve_field_id(watched_fid, wf_table_map, field_map)
            lines.append(f"{indent}  - 监听字段: 「{fname}」")
            
        if trigger_list:
            trigger_map = {
                'pasteUpdate': '粘贴更新',
                'automationBatchUpdate': '自动化批量更新',
                'appendImport': '追加导入',
                'openAPIBatchUpdate': 'API批量更新'
            }
            triggers = [trigger_map.get(t, t) for t in trigger_list]
            lines.append(f"{indent}  - 触发来源: {', '.join(triggers)}")
    
    # ============ 通用触发条件处理 (next.condition) ============
    # 触发器的过滤条件存储在 step.next[0].condition 中
    next_list = step.get('next', [])
    if next_list and isinstance(next_list, list) and len(next_list) > 0:
        first_next = next_list[0]
        if isinstance(first_next, dict):
            next_condition = first_next.get('condition')
            if next_condition and isinstance(next_condition, dict):
                cond_desc = parse_trigger_filter_condition(next_condition, wf_table_map, field_map, option_map)
                if cond_desc:
                    lines.append(f"{indent}  - **触发筛选条件**: {cond_desc}")
    
    # SetRecordTrigger
    if step_type == 'SetRecordTrigger':
        fields = step_data.get('fields', [])
        processed_keys.add('fields')
        processed_keys.add('fieldIds')
        processed_keys.add('filterInfo') # 可能存在
        if fields:
            field_names = [resolve_field_id(f.get('fieldId', ''), wf_table_map, field_map) for f in fields]
            lines.append(f"{indent}  - 监听字段: {', '.join([f'「{n}」' for n in field_names])}")
        # 也检查直接的 fieldIds
        field_ids = step_data.get('fieldIds', [])
        if field_ids:
            field_names = [resolve_field_id(fid, wf_table_map, field_map) for fid in field_ids]
            lines.append(f"{indent}  - 监听字段(ID): {', '.join([f'「{n}」' for n in field_names])}")
    
    # TimerTrigger
    if step_type == 'TimerTrigger':
        rule = step_data.get('rule', '')
        processed_keys.add('rule')
        start_time = step_data.get('startTime')
        processed_keys.add('startTime')
        if start_time:
            try:
                dt = datetime.datetime.fromtimestamp(start_time / 1000)
                lines.append(f"{indent}  - 开始时间: {dt.strftime('%Y-%m-%d %H:%M')}")
            except:
                pass
        rule_map = {'MONTHLY': '每月', 'WEEKLY': '每周', 'DAILY': '每天', 'HOURLY': '每小时'}
        lines.append(f"{indent}  - 重复规则: {rule_map.get(rule, rule)}")
    
    # ============ 查找记录 ============
    if step_type in ['FindRecordAction', 'FindRecord']:
        record_info = step_data.get('recordInfo', {})
        processed_keys.add('recordInfo')
        processed_keys.add('fieldsMap') # 可能是输出字段映射
        
        # 显式处理 fieldIds (返回的字段)
        field_ids = step_data.get('fieldIds')
        processed_keys.add('fieldIds')
        if field_ids:
            field_names = [resolve_field_id(fid, wf_table_map, field_map) for fid in field_ids]
            lines.append(f"{indent}  - 返回字段: {', '.join([f'「{n}」' for n in field_names])}")
        
        # 记录类型处理
        record_type = step_data.get('recordType')
        processed_keys.add('recordType')
        
        if record_type == 'Ref' and isinstance(record_info, dict):
            # 引用之前的步骤
            ref_step_id = record_info.get('stepId')
            ref_step_num = step_id_map.get(ref_step_id, '?')
            lines.append(f"{indent}  - 查找方式: 基于步骤{ref_step_num}返回的记录进行筛选")
        elif isinstance(record_info, dict):
            conditions = record_info.get('conditions', [])
            if conditions:
                cond_str = parse_conditions_list(conditions, wf_table_map, table_map, field_map, option_map)
                lines.append(f"{indent}  - 查找条件: {cond_str}")
            else:
                lines.append(f"{indent}  - 查找条件: 无（返回所有记录）")
        
        # 是否在无结果时继续
        should_proceed = step_data.get('shouldProceedWithNoResults', False)
        processed_keys.add('shouldProceedWithNoResults')
        if should_proceed:
            lines.append(f"{indent}  - 无结果时: 继续执行")
            
    # ============ 按钮触发 ============
    if step_type in ['ButtonTrigger']:
        button_type = step_data.get('buttonType')
        processed_keys.add('buttonType')
        type_map = {'buttonField': '字段按钮触发', 'recordMenu': '记录菜单触发'}
        lines.append(f"{indent}  - 按钮类型: {type_map.get(button_type, button_type)}")

    # ... (other step types)


    
    # ============ 新增记录 ============
    if step_type in ['AddRecordAction', 'AddRecord']:
        values = step_data.get('values', [])
        processed_keys.add('values')
        if values:
            field_values = parse_field_values(values, wf_table_map, field_map, option_map)
            if field_values:
                lines.append(f"{indent}  - 设置字段:")
                for fv in field_values:
                    lines.append(f"{indent}    {fv}")
    
    # ============ 修改记录 ============
    if step_type in ['SetRecordAction', 'UpdateRecordAction', 'UpdateRecord']:
        # 记录来源
        record_type = step_data.get('recordType', '')
        processed_keys.add('recordType')
        record_info = step_data.get('recordInfo', {})
        processed_keys.add('recordInfo')
        processed_keys.add('maxSetRecordNum') # 可能存在
        
        if record_type == 'stepRecord' or (isinstance(record_info, dict) and record_info.get('type') == 'ref'):
            # 引用步骤结果
            step_num = record_info.get('stepNum', '?') if isinstance(record_info, dict) else '?'
            lines.append(f"{indent}  - 修改对象: [步骤{step_num}找到的记录]")
        elif isinstance(record_info, dict) and record_info.get('conditions'):
            # 有查找条件
            cond_str = parse_conditions_list(record_info.get('conditions', []), wf_table_map, table_map, field_map, option_map)
            lines.append(f"{indent}  - 修改条件: {cond_str}")
        
        # 设置的字段值
        values = step_data.get('values', [])
        processed_keys.add('values')
        if values:
            field_values = parse_field_values(values, wf_table_map, field_map, option_map)
            if field_values:
                lines.append(f"{indent}  - 设置字段:")
                for fv in field_values:
                    lines.append(f"{indent}    {fv}")
    
    # ============ 循环 ============
    if step_type == 'Loop':
        loop_type = step_data.get('loopType', '')
        processed_keys.add('loopType')
        loop_data = step_data.get('loopData', {})
        processed_keys.add('loopData')
        max_times = step_data.get('maxLoopTimes', 0)
        processed_keys.add('maxLoopTimes')
        processed_keys.add('loopMode')
        
        start_child_id = step_data.get('startChildStepId')
        processed_keys.add('startChildStepId')
        
        loop_type_map = {'forEach': '遍历每条记录', 'times': '固定次数'}
        lines.append(f"{indent}  - 循环类型: {loop_type_map.get(loop_type, loop_type)}")
        
        if isinstance(loop_data, dict) and loop_data.get('type') == 'ref':
            step_num = loop_data.get('stepNum', '?')
            lines.append(f"{indent}  - 循环数据: [步骤{step_num}找到的记录]")
        
        if max_times:
            lines.append(f"{indent}  - 最大循环次数: {max_times}")
            
        if start_child_id:
            child_step_num = step_id_map.get(start_child_id, '?')
            lines.append(f"{indent}  - 循环体开始: 跳转至步骤 {child_step_num}")
    
    # ============ 条件判断 ============
    if step_type == 'IfElseBranch':
        condition_obj = step_data.get('condition', {})
        processed_keys.add('condition')
        
        meet_id = step_data.get('meetConditionStepId')
        processed_keys.add('meetConditionStepId')
        
        not_meet_id = step_data.get('notMeetConditionStepId')
        processed_keys.add('notMeetConditionStepId')
        
        if condition_obj:
            cond_desc = parse_if_else_condition(condition_obj, wf_table_map, table_map, field_map, option_map)
            lines.append(f"{indent}  - **判断条件**: {cond_desc}")
            
        if meet_id:
            meet_num = step_id_map.get(meet_id, '?')
            lines.append(f"{indent}  - ✅ 满足时: 跳转至步骤 {meet_num}")
        else:
            lines.append(f"{indent}  - ✅ 满足时: 继续执行")
            
        if not_meet_id:
            not_meet_num = step_id_map.get(not_meet_id, '?')
            lines.append(f"{indent}  - ❌ 不满足: 跳转至步骤 {not_meet_num}")
        else:
            lines.append(f"{indent}  - ❌ 不满足: (无动作)")
    
    # ============ 自定义动作 ============
    if step_type == 'CustomAction':
        pack_id = step_data.get('packId', '')
        processed_keys.add('packId')
        form_data = step_data.get('formData', {})
        processed_keys.add('formData')
        version = step_data.get('version', '')
        processed_keys.add('version')
        processed_keys.add('endpointId')
        processed_keys.add('resultTypeInfo')
        processed_keys.add('packType')
        
        lines.append(f"{indent}  - 动作类型: 自定义动作 (packId: {pack_id})")
        if form_data:
            lines.append(f"{indent}  - 配置详情:")
            
            # 尝试通过 key/label 解析配置
            if isinstance(form_data, list):
                for idx, item in enumerate(form_data):
                    if isinstance(item, dict):
                        label = item.get('label', item.get('key', f'配置{idx+1}'))
                        val = item.get('value', '')
                        
                        # 解析值
                        val_text = ""
                        if isinstance(val, list):
                            # 处理富文本列表 (Rich Text List)
                            parts = []
                            for v in val:
                                if isinstance(v, dict):
                                    # 优先取 text，其次处理引用 ref
                                    if 'text' in v:
                                        parts.append(v['text'])
                                    else:
                                        # 统一使用增强后的 format_value 解析引用 (ref/system/formula/RecordAttr)
                                        ref_desc = format_value(v, option_map, 0, wf_table_map, field_map)
                                        parts.append(ref_desc)
                                else:
                                    parts.append(str(v))
                            val_text = "".join(parts)
                            
                            # 用户要求完整展示，移除截断
                            # if len(val_text) > 2000: 
                            #     val_text = val_text[:2000] + "...(过长截断)"
                        else:
                            val_text = format_value(val, option_map, 0, wf_table_map, field_map)
                            
                        lines.append(f"{indent}    - {label}: {val_text}")
            else:
                form_str = str(form_data)
                if len(form_str) > 500: form_str = form_str[:500] + "..."
                lines.append(f"{indent}    {form_str}")
    
    # ============ 兜底机制：显示未处理的配置，并尝试解析 ID ============
    remaining_keys = set(step_data.keys()) - processed_keys
    if remaining_keys:
        lines.append(f"{indent}  - 其他配置:")
        for k in sorted(remaining_keys):
            val = step_data[k]
            # 忽略空字典或 None
            if val in [None, {}, [], ""]:
                continue
            
            # 智能解析值中的 ID
            val_fmt = format_value(val, option_map, 0, wf_table_map, field_map)
            
            # 如果是 ID 列表或包含 ID 的字符串，尝试解析出名称补充在后面
            resolved_names = []
            if isinstance(val, list):
                for v in val:
                    if isinstance(v, str) and 'fld' in v:
                        fname = resolve_field_id(v, wf_table_map, field_map)
                        if fname != v: resolved_names.append(fname)
            elif isinstance(val, str) and 'fld' in val:
                # 可能是单个 ID
                fname = resolve_field_id(val, wf_table_map, field_map)
                if fname != val: resolved_names.append(fname)
                
            if resolved_names:
                val_fmt += f" (解析: {', '.join(resolved_names)})"

            if len(val_fmt) > 300: val_fmt = val_fmt[:300] + "..."
            lines.append(f"{indent}    - {k}: {val_fmt}")
            
    return lines


def parse_if_else_condition(condition_obj, wf_table_map, table_map, field_map, option_map):
    """解析 IfElseBranch 的条件对象，返回可读描述"""
    if not condition_obj:
        return "无条件"
    
    conjunction = condition_obj.get('conjunction', 'And')
    conditions = condition_obj.get('conditions', [])
    
    if not conditions:
        return "无条件"
    
    parsed = []
    for cond in conditions:
        # 可能是嵌套的条件组
        if 'conditions' in cond:
            nested = parse_if_else_condition(cond, wf_table_map, table_map, field_map, option_map)
            parsed.append(f"({nested})")
        else:
            # 单个条件: leftValue, operator, rightValue
            left = cond.get('leftValue', {})
            op = cond.get('operator', '')
            right = cond.get('rightValue', [])
            
            # 解析左值
            left_desc = parse_value_ref(left, wf_table_map, field_map)
            
            # 解析操作符 (使用全局 OPERATORS 字典)
            op_desc = OPERATORS.get(op, op)
            
            # 解析右值
            right_desc = parse_right_value(right)
            
            if op in ['isEmpty', 'isNotEmpty']:
                parsed.append(f"{left_desc} {op_desc}")
            else:
                parsed.append(f"{left_desc} {op_desc} \"{right_desc}\"")
    
    connector = " 或 " if conjunction.lower() == 'or' else " 且 "
    return connector.join(parsed)


def parse_value_ref(value_obj, wf_table_map, field_map):
    """解析值引用对象（leftValue 或类似结构）"""
    if not value_obj:
        return "未知"
    
    if isinstance(value_obj, str):
        return value_obj
    
    # RecordAttribute 步骤引用 (来自查找记录的结果)
    if value_obj.get('type') == 'ref' and value_obj.get('tagType') == 'RecordAttribute':
        step_num = value_obj.get('stepNum', '?')
        attribute = value_obj.get('attribute', '')
        step_type = value_obj.get('stepType', '')
        
        # 翻译属性名
        attr_map = {
            'recordNum': '记录数',
            'recordId': '记录ID',
            'record': '记录',
            'value': '值'
        }
        attr_name = attr_map.get(attribute, attribute)
        
        # 翻译步骤类型
        step_type_map = {
            'FindRecordAction': '查找记录',
            'AddRecordAction': '新增记录'
        }
        step_type_name = step_type_map.get(step_type, step_type)
        
        return f"[步骤{step_num}({step_type_name})的{attr_name}]"
    
    # 步骤引用
    if value_obj.get('type') == 'ref' and value_obj.get('tagType') == 'step':
        step_num = value_obj.get('stepNum', '?')
        fields = value_obj.get('fields', [])
        if fields:
            field_id = fields[0].get('fieldId', '')
            field_name = resolve_field_id(field_id, wf_table_map, field_map)
            return f"[步骤{step_num}的「{field_name}」]"
        return f"[步骤{step_num}的结果]"
    
    # 直接字段引用
    fields = value_obj.get('fields', [])
    if fields:
        field_id = fields[0].get('fieldId', '')
        field_name = resolve_field_id(field_id, wf_table_map, field_map)
        return f"「{field_name}」"
    
    return str(value_obj)


def parse_right_value(right_value):
    """解析右值（通常是数组）"""
    if not right_value:
        return ""
    
    if isinstance(right_value, list):
        values = []
        for item in right_value:
            if isinstance(item, dict):
                text = item.get('text', item.get('value', str(item)))
                values.append(str(text))
            else:
                values.append(str(item))
        return ", ".join(values)
    
    return str(right_value)


def parse_workflow(wf_item, table_map, field_map, option_map, block_map):
    """解析单个工作流，返回 Markdown 格式的描述"""
    lines = []
    
    # 获取 WorkflowExtra
    extra = wf_item.get('WorkflowExtra', {})
    draft_str = extra.get('Draft', '{}')
    
    try:
        draft = json.loads(draft_str) if isinstance(draft_str, str) else draft_str
    except:
        draft = {}
    
    
    if not isinstance(draft, dict):
        return lines
    
    # 获取工作流的表映射
    wf_table_map = extra.get('Extra', {}).get('TableMap', {})
    
    # 工作流基本信息
    wf_id = wf_item.get('id', '未知')
    title = draft.get('title')
    
    # 优先使用侧边栏名称 (block_map)
    if not title and block_map:
        title = block_map.get(str(wf_id))

    # 如果还没有标题，尝试根据触发器生成描述性标题
    if not title:
        steps = draft.get('steps', [])
        if steps:
            first_step = steps[0]
            stype = first_step.get('type')
            sdata = first_step.get('data', {})
            
            # 尝试获取表名
            tid = sdata.get('tableId') or sdata.get('watchedCustomTableId') # TimerTrigger uses watchedCustomTableId
            tname = resolve_table_id(tid, wf_table_map, table_map) if tid else "未知表"
            
            if stype == 'ChangeRecordTrigger':
                title = f"当「{tname}」记录变更时"
            elif stype == 'AddRecordTrigger':
                title = f"当「{tname}」新增记录时"
            elif stype == 'SetRecordTrigger':
                title = f"当「{tname}」记录满足条件时"
            elif stype == 'TimerTrigger':
                title = f"定时触发 (基于「{tname}」)"
            elif stype == 'ButtonTrigger':
                title = f"按钮触发 (「{tname}」)"
            else:
                title = f"{ACTION_TYPES.get(stype, stype)} (「{tname}」)"
        else:
            title = "未命名工作流"
    
    status = wf_item.get('status', 0)
    # 飞书中 status=1 表示启用
    status_str = "✅ 已启用" if status == 1 else "⚪ 已禁用"
    
    lines.append(f"## {title}")
    lines.append(f"- **工作流 ID**: `{wf_id}`")
    lines.append(f"- **状态**: {status_str}")
    

    
    # 解析步骤
    steps = draft.get('steps', [])
    if steps:
        # 建立步骤ID到序号的映射
        step_id_map = {}
        for i, step in enumerate(steps):
             if step.get('id'):
                 step_id_map[step.get('id')] = i + 1
        
        lines.append("- **执行逻辑**:")
        for i, step in enumerate(steps):
            step_lines = parse_step(step, wf_table_map, table_map, field_map, option_map, step_id_map, step_index=i+1)
            lines.extend(step_lines)
    
    lines.append("\n---\n")
    return lines


def generate_document(workflows, table_map, field_map, option_map, block_map):
    """生成自动化地图 Markdown 文档"""
    # print(f"DEBUG: generate_document -> field_map size: {len(field_map)}")
    document = []
    document.append("# 自动化地图\n")
    document.append(f"> 生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    document.append(f"> 工作流总数: {len(workflows)}\n\n")
    
    # 飞书中 status=1 表示启用
    enabled_count = sum(1 for wf in workflows if wf.get('status') == 1)
    disabled_count = len(workflows) - enabled_count
    document.append(f"- 已启用: {enabled_count} 个\n")
    document.append(f"- 已禁用: {disabled_count} 个\n")
    document.append("\n---\n")
    
    document.append("\n> **🔍 如何对应飞书界面？**")
    document.append("> 1. **看名字**：文档已读取飞书侧边栏的真实名称，与界面完全一致。")
    document.append("> 2. **看 ID**：如果需要精确排查，可参考自动化 ID。")
    
    for wf in workflows:
        wf_lines = parse_workflow(wf, table_map, field_map, option_map, block_map)
        document.extend(wf_lines)
    
    return "\n".join(document)


def build_block_map(snapshot):
    """构建 Automation ID -> SideBar Name 的映射"""
    block_map = {}
    for item in snapshot:
        if 'schema' in item:
            block_infos = item['schema'].get('base', {}).get('blockInfos', {})
            for bid, info in block_infos.items():
                # blockType 86 似乎是自动化工作流
                token = info.get('blockToken')
                name = info.get('name')
                if token and name:
                    block_map[token] = name
    return block_map


def main():
    print("=" * 50)
    print("自动化地图生成器")
    print("=" * 50)
    
    # 读取文件
    FILE_PATH = find_base_file()
    print(f"\n[1/5] 读取文件: {FILE_PATH}")
    try:
        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ 文件读取失败: {e}")
        return
    
    # 解压快照
    print("[2/5] 解压快照数据...")
    snapshot = decompress_content(data.get('gzipSnapshot'))
    if not snapshot:
        print("❌ 快照解压失败")
        return
    
    # 构建名称映射
    print("[3/5] 构建名称映射...")
    table_map, field_map, option_map = build_name_registry(snapshot)
    block_map = build_block_map(snapshot)
    print(f"    - 发现 {len(table_map)} 张表")
    print(f"    - 发现 {len(field_map)} 个字段")
    print(f"    - 发现 {len(block_map)} 个侧边栏名称")
    
    # 解压自动化数据
    print("[4/5] 解压自动化数据...")
    workflows = decompress_content(data.get('gzipAutomation'))
    if not workflows or not isinstance(workflows, list):
        print("❌ 自动化数据解压失败或为空")
        return
    print(f"    - 发现 {len(workflows)} 个工作流")
    
    # 生成文档
    print("[5/5] 生成文档...")
    
    # print(f"DEBUG: main -> field_map size: {len(field_map)}")
    document = generate_document(workflows, table_map, field_map, option_map, block_map)
    
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(document)
    
    print(f"\n✅ 成功生成: {OUTPUT_PATH}")
    print("=" * 50)


if __name__ == "__main__":
    main()
