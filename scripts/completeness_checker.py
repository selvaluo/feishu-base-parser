#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整性校验器 (Completeness Checker)
====================================
功能：检查飞书多维表格 .base 文件中的所有数据字段，
      找出解析器可能遗漏的字段，生成校验报告。
      
输出：完整性校验报告.md
"""

import json
import base64
import gzip
import io
from collections import defaultdict

import glob
import sys
import os

# ========== 配置 ==========
OUTPUT_PATH = "完整性校验报告.md"

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

# 已知的、已被解析器处理的字段（根据 generate_自动化地图.py 的逻辑）
KNOWN_STEP_KEYS = {
    # 通用
    'type', 'id', 'data', 'stepTitle',
    # 触发器
    'tableId', 'fields', 'triggerControlList', 'watchedFieldId', 'rule', 'startTime',
    'buttonType',  # ButtonTrigger
    # 查找记录
    'recordInfo', 'fieldsMap', 'fieldIds', 'recordType', 'shouldProceedWithNoResults',
    # 修改/新增记录
    'recordList', 'updateFields', 'values', 'maxSetRecordNum',
    # 条件分支
    'condition', 'ifStepId', 'elseStepId', 'meetConditionStepId', 'notMeetConditionStepId',
    # 循环
    'loopType', 'loopData', 'loopStartStepId', 'maxLoopCount', 'maxLoopTimes', 'loopMode', 'startChildStepId',
    # CustomAction
    'packId', 'formData', 'version', 'endpointId', 'resultTypeInfo', 'packType',
    # 其他常见字段
    'filterInfo', 'isEnabled', 'stepNum'
}

KNOWN_WORKFLOW_KEYS = {
    'id', 'base_id', 'trigger_name', 'creator', 'editor', 'status', 'delete_flag',
    'created_time', 'updated_time', 'source', 'access_mode', 'webhook_token',
    'biz_type', 'nodeSchema', 'WorkflowExtra'
}

KNOWN_DRAFT_KEYS = {
    'title', 'steps', 'version'
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


def analyze_unknown_keys(data, known_keys, context=""):
    """分析数据中的未知键"""
    unknown = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if k not in known_keys:
                unknown[k] = {
                    'context': context,
                    'value_type': type(v).__name__,
                    'sample': str(v)[:200] if v else "[空]"
                }
    return unknown


def main():
    print("=" * 50)
    print("完整性校验器")
    print("=" * 50)
    
    # 读取文件
    FILE_PATH = find_base_file()
    print(f"\n[1/4] 读取文件: {FILE_PATH}")
    try:
        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ 文件读取失败: {e}")
        return
    
    # 检查顶层结构
    print("[2/4] 检查顶层数据块...")
    top_level_keys = set(data.keys())
    known_top_keys = {'gzipSnapshot', 'gzipExtraInfo', 'gzipBaseRole', 'gzipAccessConfig', 
                      'gzipDashboard', 'gzipAutomation', 'gzipAutomationButtonRule', 'sign'}
    unknown_top = top_level_keys - known_top_keys
    
    # 解压自动化数据
    print("[3/4] 解压并分析自动化数据...")
    workflows = decompress_content(data.get('gzipAutomation'))
    if not workflows or not isinstance(workflows, list):
        print("❌ 自动化数据解压失败")
        return
    
    # 收集所有未知字段
    all_unknown = defaultdict(list)
    step_type_fields = defaultdict(lambda: defaultdict(int))  # step_type -> {field: count}
    
    for wf in workflows:
        # 检查工作流级别
        wf_unknown = analyze_unknown_keys(wf, KNOWN_WORKFLOW_KEYS, f"工作流 {wf.get('id', '?')}")
        for k, v in wf_unknown.items():
            all_unknown[f"工作流级别.{k}"].append(v)
        
        # 解析 Draft
        extra = wf.get('WorkflowExtra', {})
        draft_str = extra.get('Draft', '{}')
        try:
            draft = json.loads(draft_str) if isinstance(draft_str, str) else draft_str
        except:
            continue
        
        if not isinstance(draft, dict):
            continue
            
        # 检查 Draft 级别
        draft_unknown = analyze_unknown_keys(draft, KNOWN_DRAFT_KEYS, f"Draft")
        for k, v in draft_unknown.items():
            all_unknown[f"Draft级别.{k}"].append(v)
        
        # 检查每个步骤
        for step in draft.get('steps', []):
            step_type = step.get('type', 'Unknown')
            step_data = step.get('data', {})
            
            # 记录步骤级别的未知字段
            step_unknown = analyze_unknown_keys(step, {'type', 'id', 'data', 'stepTitle'}, f"步骤 {step_type}")
            for k, v in step_unknown.items():
                all_unknown[f"步骤级别.{k}"].append(v)
            
            # 记录步骤数据中的所有字段（用于统计）
            for k in step_data.keys():
                step_type_fields[step_type][k] += 1
    
    # 生成报告
    print("[4/4] 生成校验报告...")
    
    # 统计数据
    table_count = len(set(wf.get('base_id', '') for wf in workflows))
    workflow_count = len(workflows)
    unknown_count = sum(1 for fields in step_type_fields.values() 
                        for f in fields if f not in KNOWN_STEP_KEYS)
    
    # 收集具体问题
    problems = []
    
    # 检查未知步骤类型字段
    for step_type, fields in step_type_fields.items():
        for field in fields:
            if field not in KNOWN_STEP_KEYS:
                problems.append({
                    'type': '未解析的步骤字段',
                    'location': f'{step_type} 类型的步骤',
                    'detail': f'字段 `{field}` 未被解析',
                    'suggestion': f'告诉 AI："{step_type} 步骤中的 {field} 字段没有被解析"'
                })
    
    # ========== 扫描生成的文档，检查未翻译的 ID ==========
    import re
    import os
    
    # 0. 提取源文件中所有的有效 ID (用于诊断)
    valid_ids = set()
    
    # 提取表 ID 和字段 ID
    if isinstance(data, dict):
        extra = data.get('gzipExtraInfo', {})
        if isinstance(extra, str): # 如果还没解压
             extra = decompress_content(extra)
        
        if isinstance(extra, dict):
            tables = extra.get('tables', [])
            for tbl in tables:
                tid = tbl.get('tableId')
                if tid: valid_ids.add(tid)
                
                for fld in tbl.get('fields', []):
                    fid = fld.get('fieldId')
                    if fid: valid_ids.add(fid)
    
    doc_files = [
        "全量字段表.md",
        "字段关联关系图.md",
        "自动化工作流.md"
    ]
    
    # 匹配模式：(正则, 类型名称, 是否故意显示)
    # [未知字段:fldXXX]
    id_patterns = [
        (r'\[未知(?:字段|表|选项|引用)[^:\]]*:([^\]]+)\]', '显式未知项', '未解析'),
        (r'\[已删除的(?:字段|表)[^:\]]*:([^\]]+)\]', '已删除引用', '未解析'),
        (r'\[步骤\d+的(?:字段|formula|结果)\]', '模糊引用', '可读性差'),
        (r'\[步骤\d+的循环当前记录\]', '模糊循环', '可读性差'),
        (r'default_url":\s*"{引用}"', '模糊动作配置', '信息丢失'),
        (r'\b(is|isNot|contains|doesNotContain|isEmpty|isNotEmpty)\b', '未翻译操作符', '英文残留')
    ]
    
    untranslated_items = []
    
    for doc_path in doc_files:
        if not os.path.exists(doc_path):
            continue
        
        with open(doc_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        doc_name = os.path.basename(doc_path)
        
        for pattern, issue_type, category in id_patterns:
            for match in re.finditer(pattern, content):
                match_text = match.group(0) # 完整标签
                # 对于某些正则，可能没有 group(1)
                match_id = match.group(1) if match.lastindex and match.lastindex >= 1 else match_text
                match_start = match.start()
                
                # 找到行号
                line_num = content[:match_start].count('\n') + 1

                # 获取该行内容
                line_start = content.rfind('\n', 0, match_start) + 1
                line_end = content.find('\n', match_start)
                if line_end == -1: line_end = len(content)
                line_content = content[line_start:line_end]

                # 尝试获取上下文信息 (所属表名 / 字段名)
                context_info = "未知位置"
                
                # 1. 向上查找最近的二级标题 (## 表名)
                header_match = None
                for m in re.finditer(r'^##\s+(.*?)$', content[:match_start], re.MULTILINE):
                    header_match = m
                
                table_name = header_match.group(1).strip() if header_match else "未知表"
                
                # 2. 尝试从当前行提取第一个单元格 (字段名)
                field_name = "未知行"
                row_match = re.match(r'^\|?\s*\*{0,2}(.*?)\*{0,2}\s*\|', line_content.strip())
                if row_match:
                    field_name = row_match.group(1).strip()
                
                context_str = f"表: {table_name} / 行: {field_name}"
                
                # 诊断原因
                diagnosis = ""
                action = ""
                
                if category == '未解析':
                    if match_id in valid_ids:
                        reason = "解析器缺陷"
                        diagnosis = f"ID `{match_id}` 存在于源数据中，但解析器未能识别。"
                        action = "建议：请检查生成脚本的 ID 映射逻辑。"
                        severity = "🔴 高 (可能是 Bug)"
                    else:
                        reason = "数据缺失"
                        diagnosis = f"ID `{match_id}` 在源数据中不存在。"
                        action = (
                            "请执行以下操作：\n"
                            "  1. 打开飞书多维表格\n"
                            f"  2. 定位到 **{table_name}**\n"
                            f"  3. 找到 **{field_name}** (或对应自动化流程)\n"
                            "  4. 检查是否有显示为 **红色错误** 或 **已删除** 的字段引用\n"
                            "  5. 如果该字段确实存在且正常，请**截图**该字段的配置发送给 AI"
                        )
                        severity = "🟡 中 (可能是已删除字段)"
                else:
                    reason = issue_type
                    diagnosis = f"发现 {issue_type}: `{match_text}`"
                    action = "这是脚本生成逻辑不够完善导致的，请告知 AI 优化相关解析函数。"
                    severity = "🔵 低 (可读性问题)"

                untranslated_items.append({
                    'doc': doc_name,
                    'line': line_num,
                    'text': match_text,
                    'id': match_id,
                    'context': context_str,
                    'reason': reason,
                    'diagnosis': diagnosis,
                    'action': action,
                    'severity': severity
                })
    
    lines = []
    lines.append("# 完整性校验报告\n")
    lines.append(f"> 生成时间: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    lines.append("---\n")
    
    # 校验结果摘要
    lines.append("## 📊 校验结果\n")
    lines.append("| 项目 | 结果 |")
    lines.append("|------|------|")
    lines.append(f"| 工作流解析 | ✅ {workflow_count} 个工作流已解析 |")
    
    if unknown_count == 0:
        lines.append("| 字段覆盖率 | ✅ 100% 全部覆盖 |")
    else:
        coverage = 100 - (unknown_count / max(1, sum(len(f) for f in step_type_fields.values())) * 100)
        lines.append(f"| 字段覆盖率 | ⚠️ {coverage:.1f}% (有 {unknown_count} 个字段未解析) |")
    
    # 翻译覆盖率
    if len(untranslated_items) == 0:
        lines.append("| ID翻译 | ✅ 100% 已翻译 |")
    else:
        lines.append(f"| ID翻译 | ⚠️ 发现 {len(untranslated_items)} 个未翻译ID |")
    
    lines.append("")
    
    # 问题列表
    if problems:
        lines.append("---\n")
        lines.append("## ⚠️ 发现的问题 (需人工介入)\n")
        
        for i, p in enumerate(problems[:5], 1):  # 最多显示5个
            lines.append(f"### 问题 {i}: {p['type']}\n")
            lines.append(f"- **位置**: {p['location']}")
            lines.append(f"- **详情**: {p['detail']}")
            lines.append(f"- **如何修复**: {p['suggestion']}\n")
        
        if len(problems) > 5:
            lines.append(f"\n*还有 {len(problems) - 5} 个类似问题...*\n")
    
    # 生成问题列表
    if untranslated_items:
        lines.append("---\n")
        lines.append("## ⚠️ 发现的问题 (需人工介入)\n")
        
        for i, item in enumerate(untranslated_items[:10], 1):
            # 构建可点击链接 (VS Code 友好格式)
            file_link = f"[{item['doc']}:{item['line']}](./{item['doc']}#L{item['line']})"
            
            lines.append(f"### 问题 {i}: {item['reason']}\n")
            lines.append(f"- **错误位置**: {file_link}")
            lines.append(f"- **精确定位**: {item['context']}")
            lines.append(f"- **未解析内容**: `{item['text']}`")
            lines.append(f"- **诊断结果**: {item['diagnosis']}")
            lines.append(f"- **建议操作**: \n{item['action']}\n")
            
        if len(untranslated_items) > 10:
             lines.append(f"\n*还有 {len(untranslated_items) - 10} 个类似问题...*\n")

    else:
        # 如果没有问题
        lines.append("---\n")
        lines.append("## ✅ 解析完成\n")
        lines.append("所有内容均已成功解析，无需额外处理。\n")
        
    # 使用说明
    lines.append("---\n")
    lines.append("## 💬 如果您发现其他问题\n")
    lines.append("在阅读生成的文档时，如果看到以下情况：\n")
    lines.append("- 显示为 `fldXXX` 或 `tblXXX` 格式的内容")
    lines.append("- 显示为 `未知类型(数字)` 的字段类型")
    lines.append("- 显示为英文的操作或字段\n")
    lines.append("**请直接告诉 AI** 问题出现的位置，例如：\n")
    lines.append('> "自动化工作流第 XX 行有个字段显示为原始 ID，帮我翻译一下"\n')
    lines.append("AI 会自动修复并重新生成文档。\n")
    
    # 写入文件
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"\n✅ 校验报告已生成: {OUTPUT_PATH}")
    print("=" * 50)
    
    if untranslated_items:
        print(f"⚠️ 发现 {len(untranslated_items)} 个需要人工介入的问题，请查看报告")
    else:
        print("✅ 所有字段均已被解析器覆盖")


if __name__ == "__main__":
    main()
