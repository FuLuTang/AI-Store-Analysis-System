import datetime
import json
import re
import uuid
from pathlib import Path
from typing import Optional

from openai import OpenAI

from packages.agents.core.file_domains import SERVICE_DOCS_DOMAIN, split_domain_path

RULES_FILE_NAME = "service_docs_access_rules.json"
AUDIT_FILE_NAME = "service_docs_access_audit.jsonl"
PROFILE_FILE_NAME = "profile.json"
IDENTITY_PROFILE_KEY = "identityDescription"
REASONING_PROFILE_KEY = "reasoningEffort"

TOOL_READ_LIKE = {"read_file", "read_document_structure", "get_resource_link"}
TOOL_DISCOVERY_LIKE = {"list_files", "search"}


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _rules_path(account_dir: Path) -> Path:
    return account_dir / RULES_FILE_NAME


def _audit_path(account_dir: Path) -> Path:
    return account_dir / AUDIT_FILE_NAME


def _profile_path(account_dir: Path) -> Path:
    return account_dir / PROFILE_FILE_NAME


def normalize_identity_description(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def load_identity_description(account_dir: Path) -> str:
    return normalize_identity_description(load_account_profile(account_dir).get(IDENTITY_PROFILE_KEY))


def load_account_profile(account_dir: Path) -> dict:
    profile_path = _profile_path(account_dir)
    if not profile_path.exists():
        return {}
    try:
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def save_account_profile(account_dir: Path, profile: dict) -> dict:
    data = dict(profile or {})
    data[IDENTITY_PROFILE_KEY] = normalize_identity_description(data.get(IDENTITY_PROFILE_KEY))
    data[REASONING_PROFILE_KEY] = str(data.get(REASONING_PROFILE_KEY) or "").strip()
    path = _profile_path(account_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def update_identity_description(account_dir: Path, identity_description: str) -> dict:
    current = load_account_profile(account_dir)
    next_profile = dict(current)
    next_profile[IDENTITY_PROFILE_KEY] = normalize_identity_description(identity_description)
    saved = save_account_profile(account_dir, next_profile)
    return saved


def load_rules(account_dir: Path) -> list[dict]:
    path = _rules_path(account_dir)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        source = payload.get("rules")
    else:
        source = payload
    if not isinstance(source, list):
        return []
    result: list[dict] = []
    for item in source:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "active").strip().lower()
        if status != "active":
            continue
        pattern = str(item.get("pattern") or "").strip()
        decision = str(item.get("decision") or "").strip().lower()
        if not pattern or decision not in {"allow", "deny"}:
            continue
        result.append({
            "id": str(item.get("id") or uuid.uuid4().hex),
            "pattern": pattern,
            "decision": decision,
            "status": "active",
            "source": str(item.get("source") or "human").strip() or "human",
            "reason": str(item.get("reason") or "").strip(),
            "confidence": float(item.get("confidence") or 0.0),
            "createdAt": str(item.get("createdAt") or _now_iso()),
            "identitySnapshot": str(item.get("identitySnapshot") or "").strip(),
            "toolHint": str(item.get("toolHint") or "").strip(),
        })
    return result


def summarize_rules(account_dir: Path) -> dict:
    rules = load_rules(account_dir)
    return {
        "count": len(rules),
        "rules": rules,
    }


def save_rules(account_dir: Path, rules: list[dict]) -> None:
    path = _rules_path(account_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updatedAt": _now_iso(),
        "rules": rules,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def clear_rules(account_dir: Path) -> None:
    save_rules(account_dir, [])


def append_audit_event(account_dir: Path, event: dict) -> None:
    path = _audit_path(account_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(event or {})
    payload["time"] = payload.get("time") or _now_iso()
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _normalize_service_docs_rel(path: str) -> str:
    domain, rel = split_domain_path(path, allowed_domains=[SERVICE_DOCS_DOMAIN])
    _ = domain
    return rel.strip("/")


def _top_level_label(rel_path: str) -> str:
    parts = [part for part in str(rel_path or "").split("/") if part]
    return parts[0] if parts else ""


def _match_rule(rules: list[dict], rel_path: str) -> dict | None:
    matched_allow = None
    for rule in rules:
        pattern = str(rule.get("pattern") or "").strip()
        if not pattern:
            continue
        try:
            if re.search(pattern, rel_path):
                if str(rule.get("decision")) == "deny":
                    return rule
                if matched_allow is None:
                    matched_allow = rule
        except re.error:
            continue
    return matched_allow


def classify_access_for_path(account_dir: Path, path: str) -> dict:
    rel_path = _normalize_service_docs_rel(path)
    rules = load_rules(account_dir)
    matched = _match_rule(rules, rel_path)
    if matched:
        decision = str(matched.get("decision") or "").strip().lower()
        return {
            "status": "allow" if decision == "allow" else "deny",
            "relPath": rel_path,
            "matchedRule": matched,
        }
    return {
        "status": "unknown",
        "relPath": rel_path,
        "matchedRule": None,
    }


def tool_access_projection(account_dir: Path, tool: str, path: str) -> dict:
    classification = classify_access_for_path(account_dir, path)
    status = classification["status"]
    rel_path = classification["relPath"]
    top_level = _top_level_label(rel_path)
    if tool in TOOL_READ_LIKE:
        if status == "allow":
            classification["effectiveStatus"] = "allow"
            return classification
        classification["effectiveStatus"] = "deny"
        classification["message"] = (
            "当前身份暂无此文件内容访问权限，请使用 request_service_docs_access 工具请求权限。"
        )
        return classification

    classification["effectiveStatus"] = status
    if status == "deny":
        classification["message"] = f"{top_level or rel_path} 属于受限目录，可见文件名但不可读取正文。"
    elif status == "unknown":
        classification["message"] = "当前目录尚未建立明确授权规则；可见文件名，但读取正文前需申请权限。"
    else:
        classification["message"] = "已命中允许规则。"
    return classification


def annotate_list_payload_for_account(payload: dict, account_dir: Path) -> dict:
    if not isinstance(payload, dict):
        return payload
    domains = payload.get("domains")
    if not isinstance(domains, list):
        return payload
    for domain_item in domains:
        if not isinstance(domain_item, dict):
            continue
        if str(domain_item.get("domain") or "") != SERVICE_DOCS_DOMAIN:
            continue
        entries = domain_item.get("entries")
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            raw_path = str(entry.get("path") or "")
            if not raw_path:
                continue
            projected = tool_access_projection(account_dir, "list_files", raw_path)
            entry["accessStatus"] = projected.get("effectiveStatus")
            entry["accessRuleDecision"] = projected.get("status")
            entry["accessHint"] = projected.get("message", "")
    return payload


def _relative_to_search_root(root_path: str, result_path: str) -> str:
    root_rel = _normalize_service_docs_rel(root_path or f"{SERVICE_DOCS_DOMAIN}/")
    result_rel = _normalize_service_docs_rel(result_path)
    if root_rel and result_rel.startswith(root_rel + "/"):
        return result_rel[len(root_rel) + 1:]
    return result_rel


def project_search_payload_for_account(payload: dict, account_dir: Path) -> dict:
    if not isinstance(payload, dict):
        return payload
    domains = payload.get("domains")
    if not isinstance(domains, list):
        return payload
    for domain_item in domains:
        if not isinstance(domain_item, dict):
            continue
        if str(domain_item.get("domain") or "") != SERVICE_DOCS_DOMAIN:
            continue
        results = domain_item.get("results")
        if not isinstance(results, list):
            continue
        root_path = str(domain_item.get("root_path") or f"{SERVICE_DOCS_DOMAIN}/")
        allowed_results: list[dict] = []
        denied_groups: dict[str, dict] = {}
        unknown_groups: dict[str, dict] = {}
        for result in results:
            if not isinstance(result, dict):
                continue
            raw_path = str(result.get("path") or "")
            if not raw_path:
                continue
            projected = tool_access_projection(account_dir, "search", raw_path)
            effective = projected.get("effectiveStatus")
            rel_under_root = _relative_to_search_root(root_path, raw_path)
            child_name = rel_under_root.split("/", 1)[0] if rel_under_root else _top_level_label(projected.get("relPath", ""))
            if effective == "allow":
                result["accessStatus"] = "allow"
                allowed_results.append(result)
                continue
            if projected.get("status") == "deny":
                key = _top_level_label(projected.get("relPath", "")) or child_name or "受限目录"
                group = denied_groups.setdefault(key, {
                    "path": f"{SERVICE_DOCS_DOMAIN}/{key}" if key else root_path,
                    "type": "restricted_summary",
                    "total_matches": 0,
                    "matches": [],
                })
                group["total_matches"] += int(result.get("total_matches") or len(result.get("matches") or []) or 1)
                continue
            key = child_name or "当前目录"
            group = unknown_groups.setdefault(key, {
                "path": f"{root_path.rstrip('/')}/{key}" if key != "当前目录" else root_path,
                "type": "hint_summary",
                "total_matches": 0,
                "matches": [],
            })
            group["total_matches"] += int(result.get("total_matches") or len(result.get("matches") or []) or 1)

        projected_results = list(allowed_results)
        for key, item in denied_groups.items():
            item["matches"] = [{
                "match_type": "restricted_summary",
                "text": f"{key}（命中条数 {item['total_matches']}，无权限访问）",
            }]
            projected_results.append(item)
        for key, item in unknown_groups.items():
            item["matches"] = [{
                "match_type": "hint_summary",
                "text": f"{key}（命中条数 {item['total_matches']}）",
            }, {
                "match_type": "hint",
                "text": "若需访问子文件夹内容，需使用 request_service_docs_access 工具请求权限。",
            }]
            projected_results.append(item)
        domain_item["results"] = projected_results
    return payload


def _identity_rules_brief(rules: list[dict]) -> str:
    if not rules:
        return "暂无现有规则。"
    lines = []
    for rule in rules[:12]:
        lines.append(
            f"- [{rule.get('decision')}] pattern={rule.get('pattern')} reason={str(rule.get('reason') or '')[:80]}"
        )
    return "\n".join(lines)


def _call_permission_ai(identity_description: str, path: str, tool: str, reason: str, rules: list[dict], llm_preset: dict | None) -> dict:
    preset = llm_preset or {}
    call_cfg = preset.get("call", {}) if isinstance(preset, dict) else {}
    model = call_cfg.get("model") or preset.get("model") or ""
    api_key = call_cfg.get("apiKey") or preset.get("apiKey") or ""
    base_url = call_cfg.get("baseUrl") or preset.get("baseUrl") or ""
    if not model or not api_key or not base_url:
        return {
            "decision": "deny",
            "regex_pattern": "",
            "reason": "权限 AI 未配置可用模型，默认拒绝。",
            "confidence": 0.0,
        }
    client = OpenAI(api_key=api_key, base_url=base_url, max_retries=0)
    system_prompt = (
        "你是 service_docs 权限判断器。你不是 Agent，不要闲聊。"
        "你只有一次调用机会，只能输出 JSON 对象。"
        "目标：根据用户身份描述、目标路径、目标工具、已有规则，给出 allow/deny 判定，"
        "并在合适时给出可复用的 regex_pattern。"
        "文件名本身风险较低，但正文读取、结构读取和资源链接要谨慎。"
        "若信息不足，优先 deny。"
    )
    user_prompt = (
        f"用户身份描述:\n{identity_description or '(空)'}\n\n"
        f"目标工具: {tool}\n"
        f"目标路径: {path}\n"
        f"申请原因: {reason or '(空)'}\n\n"
        f"现有规则摘要:\n{_identity_rules_brief(rules)}\n\n"
        "请严格输出 JSON："
        '{"decision":"allow|deny","regex_pattern":"可为空","reason":"简短原因","confidence":0到1之间数字}'
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
        timeout=120,
    )
    content = ""
    try:
        content = resp.choices[0].message.content or ""
    except Exception:
        content = ""
    try:
        payload = json.loads(content)
    except Exception:
        payload = {}
    decision = str(payload.get("decision") or "deny").strip().lower()
    if decision not in {"allow", "deny"}:
        decision = "deny"
    regex_pattern = str(payload.get("regex_pattern") or "").strip()
    reason_text = str(payload.get("reason") or "").strip() or "权限 AI 未提供原因。"
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    return {
        "decision": decision,
        "regex_pattern": regex_pattern,
        "reason": reason_text,
        "confidence": max(0.0, min(1.0, confidence)),
    }


def request_access_via_ai(account_dir: Path, path: str, tool: str, reason: str, llm_preset: dict | None) -> dict:
    identity_description = load_identity_description(account_dir)
    rules = load_rules(account_dir)
    ai_result = _call_permission_ai(identity_description, path, tool, reason, rules, llm_preset)
    rule_saved = False
    regex_pattern = ai_result["regex_pattern"]
    if ai_result["decision"] == "allow" and not regex_pattern:
        try:
            rel = _normalize_service_docs_rel(path)
            regex_pattern = rf"^{re.escape(rel)}(?:/.*)?$"
        except Exception:
            regex_pattern = ""
    if regex_pattern:
        next_rules = list(rules)
        next_rules.append({
            "id": uuid.uuid4().hex,
            "pattern": regex_pattern,
            "decision": ai_result["decision"],
            "status": "active",
            "source": "ai",
            "reason": ai_result["reason"],
            "confidence": ai_result["confidence"],
            "createdAt": _now_iso(),
            "identitySnapshot": identity_description,
            "toolHint": tool,
        })
        save_rules(account_dir, next_rules)
        rule_saved = True
    append_audit_event(account_dir, {
        "kind": "service_docs_access_request",
        "path": path,
        "tool": tool,
        "reason": reason,
        "identityDescription": identity_description,
        "aiResult": ai_result,
        "ruleSaved": rule_saved,
    })
    return {
        "status": "ok",
        "decision": ai_result["decision"],
        "ruleSaved": rule_saved,
        "reason": ai_result["reason"],
        "confidence": ai_result["confidence"],
        "regexPattern": regex_pattern,
    }
