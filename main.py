from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

BASE_URL = "https://static.gamecenter.qq.com/xgame/roco-kingdom/compendium/"
DATA_URL = urljoin(BASE_URL, "d.json")
EGG_PAGE_URL = urljoin(BASE_URL, "egg.html")

ELEMENT_COLORS = {
    "光": "#FFB833",
    "草": "#6BC44F",
    "火": "#FF6B3D",
    "水": "#4DA6FF",
    "普通": "#A8A878",
    "幽": "#7B62A3",
    "电": "#F9CF30",
    "冰": "#98D8D8",
    "地": "#C7A054",
    "龙": "#7866D5",
    "翼": "#9DB7F5",
    "虫": "#A8B820",
    "机械": "#B8B8D0",
    "武": "#C03028",
    "毒": "#A040A0",
    "恶": "#705848",
    "萌": "#EE99AC",
    "幻": "#F85888",
}

SKILL_BUCKETS = {
    "s": ("spirit", "精灵技能"),
    "b": ("bloodline", "血脉技能"),
    "t": ("tm", "可学技能石"),
}

SPIRIT_NO_PATTERN = re.compile(r"(NO\.\d+)")
NUMBERED_FILENAME_PATTERN = re.compile(r"^NO\.\d+_")
DISPLAY_NAME_PATTERN = re.compile(r"^(.*?)[（(](.*?)[）)]$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="抓取洛克王国精灵图鉴并导出数据库友好的 JSON 分表。"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="输出目录，默认写入 ./output",
    )
    parser.add_argument(
        "--save-source",
        action="store_true",
        help="额外保存抓取到的原始 d.json",
    )
    parser.add_argument(
        "--write-mysql",
        action="store_true",
        help="在导出 JSON 后，继续把数据写入 MySQL。",
    )
    parser.add_argument(
        "--mysql-host",
        default=os.getenv("MYSQL_HOST", "127.0.0.1"),
        help="MySQL 主机，默认 127.0.0.1",
    )
    parser.add_argument(
        "--mysql-port",
        type=int,
        default=int(os.getenv("MYSQL_PORT", "3306")),
        help="MySQL 端口，默认 3306",
    )
    parser.add_argument(
        "--mysql-user",
        default=os.getenv("MYSQL_USER", "root"),
        help="MySQL 用户名，默认 root",
    )
    parser.add_argument(
        "--mysql-password",
        default=os.getenv("MYSQL_PASSWORD"),
        help="MySQL 密码，也可以通过环境变量 MYSQL_PASSWORD 提供",
    )
    parser.add_argument(
        "--mysql-database",
        default=os.getenv("MYSQL_DATABASE", "rocom"),
        help="MySQL 数据库名，默认 rocom",
    )
    return parser.parse_args()


def fetch_remote_json(url: str) -> dict[str, Any]:
    return json.loads(fetch_remote_text(url))


def fetch_remote_text(url: str) -> str:
    result = subprocess.run(
        ["curl", "-LfsS", url],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout


def parse_embedded_json_object(html_text: str, variable_name: str) -> dict[str, Any]:
    pattern = rf"const\s+{re.escape(variable_name)}\s*=\s*(\{{.*?\}});\n"
    match = re.search(pattern, html_text, re.S)
    if match is None:
        raise ValueError(f"未在页面中找到 JSON 变量: {variable_name}")
    return json.loads(match.group(1))


def parse_embedded_literal_object(
    html_text: str, variable_name: str
) -> dict[str, str]:
    pattern = rf"const\s+{re.escape(variable_name)}\s*=\s*(\{{.*?\}});"
    match = re.search(pattern, html_text, re.S)
    if match is None:
        raise ValueError(f"未在页面中找到字面量变量: {variable_name}")
    return ast.literal_eval(match.group(1))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_int(value: Any) -> int | None:
    text = clean_text(value)
    if text is None:
        return None
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    return None


def to_absolute_url(relative_path: Any) -> str | None:
    path = clean_text(relative_path)
    if path is None:
        return None
    return urljoin(BASE_URL, path)


def extract_spirit_no(*candidates: Any) -> str | None:
    for candidate in candidates:
        text = clean_text(candidate)
        if text is None:
            continue
        match = SPIRIT_NO_PATTERN.search(text)
        if match:
            return match.group(1)
    return None


def extract_spirit_no_number(spirit_no: str | None) -> int | None:
    if spirit_no is None:
        return None
    digits = re.sub(r"\D", "", spirit_no)
    return int(digits) if digits else None


def display_name_from_image(relative_path: Any) -> str | None:
    path = clean_text(relative_path)
    if path is None:
        return None
    file_name = Path(path).stem
    file_name = NUMBERED_FILENAME_PATTERN.sub("", file_name)
    return clean_text(file_name)


def split_display_name(display_name: Any) -> tuple[str | None, str | None]:
    text = clean_text(display_name)
    if text is None:
        return None, None
    match = DISPLAY_NAME_PATTERN.match(text)
    if match:
        base_name = clean_text(match.group(1))
        form_name = clean_text(match.group(2))
        return base_name, form_name
    return text, None


def split_locations(location_text: Any) -> list[str]:
    text = clean_text(location_text)
    if text is None:
        return []
    return [part.strip() for part in re.split(r"[\/、，,]", text) if part.strip()]


def pick_first(values: list[str]) -> str | None:
    seen: set[str] = set()
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.add(text)
            return text
    return None


def build_reference_index(details: dict[str, Any]) -> dict[int, dict[str, str | None]]:
    raw_index: dict[int, dict[str, list[str]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for detail in details.values():
        for evo in detail.get("evo", []):
            spirit_id = int(evo["i"])
            raw_index[spirit_id]["display_name"].append(evo.get("fn", ""))
            raw_index[spirit_id]["base_name"].append(evo.get("nm", ""))
            raw_index[spirit_id]["stage_name"].append(evo.get("s", ""))
            raw_index[spirit_id]["image_relative_path"].append(evo.get("img", ""))

        for form in detail.get("forms", []):
            spirit_id = int(form["i"])
            raw_index[spirit_id]["display_name"].append(form.get("fn", ""))
            raw_index[spirit_id]["form_name"].append(form.get("f", ""))
            raw_index[spirit_id]["image_relative_path"].append(form.get("img", ""))

    collapsed: dict[int, dict[str, str | None]] = {}
    for spirit_id, fields in raw_index.items():
        display_name = pick_first(fields["display_name"])
        base_name = pick_first(fields["base_name"])
        form_name = pick_first(fields["form_name"])
        image_relative_path = pick_first(fields["image_relative_path"])
        stage_name = pick_first(fields["stage_name"])
        inferred_base_name, inferred_form_name = split_display_name(display_name)

        collapsed[spirit_id] = {
            "display_name": display_name,
            "base_name": base_name or inferred_base_name,
            "form_name": form_name or inferred_form_name,
            "stage_name": stage_name,
            "image_relative_path": image_relative_path,
            "spirit_no": extract_spirit_no(image_relative_path),
        }

    return collapsed


def build_list_summary(raw_data: dict[str, Any]) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_id: dict[int, dict[str, Any]] = {}
    by_no: dict[str, dict[str, Any]] = {}

    for item in raw_data["l"]:
        spirit_id = int(item["i"])
        row = {
            "spirit_id": spirit_id,
            "spirit_no": clean_text(item.get("n")),
            "spirit_no_number": extract_spirit_no_number(clean_text(item.get("n"))),
            "base_name": clean_text(item.get("nm")),
            "display_name": clean_text(item.get("fn")),
            "form_name": clean_text(item.get("f")),
            "primary_attribute": clean_text(item.get("e")),
            "secondary_attribute": clean_text(item.get("e2")),
            "stage_name": clean_text(item.get("s")),
            "race_total": parse_int(item.get("rt")),
            "has_shiny_flag": parse_int(item.get("sh")) == 1,
            "image_relative_path": clean_text(item.get("img")),
        }
        by_id[spirit_id] = row
        if row["spirit_no"]:
            by_no[row["spirit_no"]] = row

    return by_id, by_no


def infer_self_stage(detail: dict[str, Any], spirit_id: int) -> str | None:
    for evo in detail.get("evo", []):
        if int(evo["i"]) == spirit_id:
            return clean_text(evo.get("s"))
    return None


def resolve_reference_name(
    spirit_id: int,
    display_name: str | None,
    list_summary_by_id: dict[int, dict[str, Any]],
    reference_index: dict[int, dict[str, str | None]],
) -> tuple[str | None, str | None]:
    list_summary = list_summary_by_id.get(spirit_id)
    reference = reference_index.get(spirit_id, {})

    base_name = None
    form_name = None

    if list_summary:
        base_name = clean_text(list_summary.get("base_name"))
        form_name = clean_text(list_summary.get("form_name"))

    if base_name is None:
        base_name = clean_text(reference.get("base_name"))
    if form_name is None:
        form_name = clean_text(reference.get("form_name"))

    inferred_base_name, inferred_form_name = split_display_name(display_name)

    return base_name or inferred_base_name, form_name or inferred_form_name


def resolve_evolution_chain(
    detail: dict[str, Any],
    list_summary_by_id: dict[int, dict[str, Any]],
    reference_index: dict[int, dict[str, str | None]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for evo in detail.get("evo", []):
        spirit_id = int(evo["i"])
        display_name = clean_text(evo.get("fn")) or clean_text(
            reference_index.get(spirit_id, {}).get("display_name")
        )
        base_name, form_name = resolve_reference_name(
            spirit_id,
            display_name,
            list_summary_by_id,
            reference_index,
        )
        image_relative_path = clean_text(evo.get("img"))
        spirit_no = extract_spirit_no(
            clean_text(list_summary_by_id.get(spirit_id, {}).get("spirit_no")),
            image_relative_path,
        )
        rows.append(
            {
                "spirit_id": spirit_id,
                "spirit_no": spirit_no,
                "spirit_no_number": extract_spirit_no_number(spirit_no),
                "base_name": base_name,
                "display_name": display_name or display_name_from_image(image_relative_path),
                "form_name": form_name,
                "stage_name": clean_text(evo.get("s"))
                or clean_text(list_summary_by_id.get(spirit_id, {}).get("stage_name")),
                "evolution_level": parse_int(evo.get("lv")),
                "evolution_level_text": clean_text(evo.get("lv")),
                "image_relative_path": image_relative_path,
                "image_url": to_absolute_url(image_relative_path),
            }
        )
    return rows


def resolve_forms(
    detail: dict[str, Any],
    list_summary_by_id: dict[int, dict[str, Any]],
    reference_index: dict[int, dict[str, str | None]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for form in detail.get("forms", []):
        spirit_id = int(form["i"])
        display_name = clean_text(form.get("fn")) or clean_text(
            reference_index.get(spirit_id, {}).get("display_name")
        )
        base_name, inferred_form_name = resolve_reference_name(
            spirit_id,
            display_name,
            list_summary_by_id,
            reference_index,
        )
        image_relative_path = clean_text(form.get("img"))
        spirit_no = extract_spirit_no(
            clean_text(list_summary_by_id.get(spirit_id, {}).get("spirit_no")),
            image_relative_path,
        )
        rows.append(
            {
                "spirit_id": spirit_id,
                "spirit_no": spirit_no,
                "spirit_no_number": extract_spirit_no_number(spirit_no),
                "base_name": base_name,
                "display_name": display_name or display_name_from_image(image_relative_path),
                "form_name": clean_text(form.get("f")) or inferred_form_name,
                "stage_name": clean_text(list_summary_by_id.get(spirit_id, {}).get("stage_name"))
                or clean_text(reference_index.get(spirit_id, {}).get("stage_name")),
                "image_relative_path": image_relative_path,
                "image_url": to_absolute_url(image_relative_path),
            }
        )
    return rows


def build_attribute_rows(raw_data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    element_icons = raw_data.get("_em", {})

    for index, element_name in enumerate(raw_data.get("e", []), start=1):
        rows.append(
            {
                "attribute_id": index,
                "attribute_name": element_name,
                "attribute_color": ELEMENT_COLORS.get(element_name),
                "icon_relative_path": clean_text(element_icons.get(element_name)),
                "icon_url": to_absolute_url(element_icons.get(element_name)),
                "sort_order": index,
            }
        )

    return rows


def build_egg_group_rows(
    egg_page_data: dict[str, Any],
    egg_group_icons: dict[str, str],
    egg_group_descriptions: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for index, egg_group_name in enumerate(egg_page_data.get("g", []), start=1):
        rows.append(
            {
                "egg_group_id": index,
                "egg_group_name": egg_group_name,
                "egg_group_icon": clean_text(egg_group_icons.get(egg_group_name)),
                "egg_group_description": clean_text(
                    egg_group_descriptions.get(egg_group_name)
                ),
                "is_unbreedable_group": egg_group_name == "无法孵蛋",
                "sort_order": index,
            }
        )

    return rows


def build_egg_lookup(
    egg_page_data: dict[str, Any],
) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_id: dict[int, dict[str, Any]] = {}
    by_no: dict[str, dict[str, Any]] = {}

    for item in egg_page_data.get("s", []):
        spirit_id = int(item["i"])
        egg_groups = [group for group in item.get("eg", []) if clean_text(group)]
        row = {
            "spirit_id": spirit_id,
            "spirit_no": clean_text(item.get("n")),
            "base_name": clean_text(item.get("nm")),
            "display_name": clean_text(item.get("fn")),
            "form_name": clean_text(item.get("f")),
            "egg_groups": egg_groups,
            "egg_group_count": len(egg_groups),
            "has_shiny_flag": parse_int(item.get("sh")) == 1,
        }
        by_id[spirit_id] = row

        spirit_no = row["spirit_no"]
        if spirit_no is None:
            continue

        if spirit_no not in by_no:
            by_no[spirit_no] = row
            continue

        if by_no[spirit_no]["egg_groups"] != egg_groups:
            raise ValueError(f"蛋组页中同编号蛋组不一致: {spirit_no}")

    return by_id, by_no


def build_egg_group_details(
    egg_groups: list[str],
    egg_group_meta_by_name: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for egg_group_name in egg_groups:
        meta = egg_group_meta_by_name.get(egg_group_name, {})
        rows.append(
            {
                "egg_group_name": egg_group_name,
                "egg_group_icon": clean_text(meta.get("egg_group_icon")),
                "egg_group_description": clean_text(
                    meta.get("egg_group_description")
                ),
                "is_unbreedable_group": bool(meta.get("is_unbreedable_group")),
            }
        )
    return rows


def build_spirit_rows(
    raw_data: dict[str, Any],
    list_summary_by_id: dict[int, dict[str, Any]],
    list_summary_by_no: dict[str, dict[str, Any]],
    reference_index: dict[int, dict[str, str | None]],
    egg_lookup_by_id: dict[int, dict[str, Any]],
    egg_lookup_by_no: dict[str, dict[str, Any]],
    egg_group_meta_by_name: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    trait_icons = raw_data.get("_tm", {})
    attribute_icons = raw_data.get("_em", {})

    rows: list[dict[str, Any]] = []
    for spirit_id in sorted(int(key) for key in raw_data["d"].keys()):
        detail = raw_data["d"][str(spirit_id)]
        list_summary = list_summary_by_id.get(spirit_id, {})
        reference = reference_index.get(spirit_id, {})

        image_relative_path = (
            clean_text(list_summary.get("image_relative_path"))
            or clean_text(reference.get("image_relative_path"))
        )
        spirit_no = (
            clean_text(list_summary.get("spirit_no"))
            or clean_text(reference.get("spirit_no"))
            or extract_spirit_no(image_relative_path)
        )
        list_summary_for_no = list_summary_by_no.get(spirit_no or "", {})

        display_name = (
            clean_text(list_summary.get("display_name"))
            or clean_text(reference.get("display_name"))
            or display_name_from_image(image_relative_path)
        )
        base_name, form_name = resolve_reference_name(
            spirit_id,
            display_name,
            list_summary_by_id,
            reference_index,
        )
        stage_name = (
            clean_text(list_summary.get("stage_name"))
            or clean_text(reference.get("stage_name"))
            or infer_self_stage(detail, spirit_id)
            or clean_text(list_summary_for_no.get("stage_name"))
        )

        primary_attribute = clean_text(list_summary.get("primary_attribute")) or clean_text(
            list_summary_for_no.get("primary_attribute")
        )
        secondary_attribute = clean_text(
            list_summary.get("secondary_attribute")
        ) or clean_text(list_summary_for_no.get("secondary_attribute"))
        egg_entry = egg_lookup_by_id.get(spirit_id)
        egg_data_source = "egg_page_exact"
        if egg_entry is None:
            egg_entry = egg_lookup_by_no.get(spirit_no or "")
            egg_data_source = (
                "egg_page_same_no_inferred" if egg_entry is not None else "egg_page_missing"
            )

        egg_groups = list(egg_entry["egg_groups"]) if egg_entry is not None else []
        is_unbreedable = "无法孵蛋" in egg_groups

        rows.append(
            {
                "spirit_id": spirit_id,
                "spirit_no": spirit_no,
                "spirit_no_number": extract_spirit_no_number(spirit_no),
                "base_name": base_name,
                "display_name": display_name,
                "form_name": form_name,
                "stage_name": stage_name,
                "primary_attribute": primary_attribute,
                "primary_attribute_icon_url": to_absolute_url(
                    attribute_icons.get(primary_attribute)
                ),
                "secondary_attribute": secondary_attribute,
                "secondary_attribute_icon_url": to_absolute_url(
                    attribute_icons.get(secondary_attribute)
                ),
                "trait_name": clean_text(detail.get("tn")),
                "trait_icon_url": to_absolute_url(trait_icons.get(detail.get("tn"))),
                "trait_effect": clean_text(detail.get("te")),
                "nickname": clean_text(detail.get("nick")),
                "description": clean_text(detail.get("desc")),
                "height_text": clean_text(detail.get("h")),
                "weight_text": clean_text(detail.get("w")),
                "location_text": clean_text(detail.get("loc")),
                "locations": split_locations(detail.get("loc")),
                "egg_groups": egg_groups,
                "egg_group_count": len(egg_groups),
                "egg_group_details": build_egg_group_details(
                    egg_groups,
                    egg_group_meta_by_name,
                ),
                "can_breed": bool(egg_groups) and not is_unbreedable,
                "is_unbreedable": is_unbreedable,
                "egg_data_source": egg_data_source,
                "race_total": parse_int(detail.get("rt")),
                "hp": parse_int(detail.get("hp")),
                "attack": parse_int(detail.get("atk")),
                "magic_attack": parse_int(detail.get("matk")),
                "defense": parse_int(detail.get("df")),
                "magic_defense": parse_int(detail.get("mdf")),
                "speed": parse_int(detail.get("spd")),
                "image_relative_path": image_relative_path,
                "image_url": to_absolute_url(image_relative_path),
                "shiny_image_relative_path": clean_text(detail.get("si")),
                "shiny_image_url": to_absolute_url(detail.get("si")),
                "has_shiny_variant": bool(clean_text(detail.get("si")))
                or bool(list_summary.get("has_shiny_flag")),
                "source_in_list": spirit_id in list_summary_by_id,
                "is_hidden_detail": spirit_id not in list_summary_by_id,
                "name_source": "list"
                if spirit_id in list_summary_by_id
                else "reference_inferred",
                "attribute_source": "list"
                if clean_text(list_summary.get("primary_attribute"))
                else "same_no_inferred",
                "stage_source": "list"
                if clean_text(list_summary.get("stage_name"))
                else "reference_or_same_no_inferred",
                "evolution_chain": resolve_evolution_chain(
                    detail,
                    list_summary_by_id,
                    reference_index,
                ),
                "forms": resolve_forms(
                    detail,
                    list_summary_by_id,
                    reference_index,
                ),
            }
        )

    return rows


def build_skill_tables(
    raw_data: dict[str, Any],
    spirit_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    spirit_lookup = {row["spirit_id"]: row for row in spirit_rows}
    skill_icons = raw_data.get("_skm", {})
    attribute_icons = raw_data.get("_em", {})
    skill_type_icons = raw_data.get("_stm", {})

    skill_key_to_id: dict[tuple[Any, ...], int] = {}
    skill_definitions: list[dict[str, Any]] = []
    spirit_skill_rows: list[dict[str, Any]] = []
    flat_skill_rows: list[dict[str, Any]] = []

    relation_id = 1
    for spirit_id in sorted(int(key) for key in raw_data["d"].keys()):
        detail = raw_data["d"][str(spirit_id)]
        spirit = spirit_lookup[spirit_id]

        for bucket_key, (bucket_name, bucket_label) in SKILL_BUCKETS.items():
            for skill in detail.get("sk", {}).get(bucket_key, []):
                key = (
                    clean_text(skill.get("nm")),
                    clean_text(skill.get("tp")),
                    clean_text(skill.get("el")),
                    parse_int(skill.get("ec")),
                    parse_int(skill.get("pw")),
                    clean_text(skill.get("ef")),
                )

                skill_id = skill_key_to_id.get(key)
                if skill_id is None:
                    skill_id = len(skill_definitions) + 1
                    skill_key_to_id[key] = skill_id
                    skill_definitions.append(
                        {
                            "skill_id": skill_id,
                            "skill_name": clean_text(skill.get("nm")),
                            "skill_type": clean_text(skill.get("tp")),
                            "skill_type_icon_url": to_absolute_url(
                                skill_type_icons.get(skill.get("tp"))
                            ),
                            "attribute_name": clean_text(skill.get("el")),
                            "attribute_icon_url": to_absolute_url(
                                attribute_icons.get(skill.get("el"))
                            ),
                            "energy_cost": parse_int(skill.get("ec")),
                            "power": parse_int(skill.get("pw")),
                            "effect_text": clean_text(skill.get("ef")),
                            "skill_icon_url": to_absolute_url(
                                skill_icons.get(skill.get("nm"))
                            ),
                        }
                    )

                relation_row = {
                    "relation_id": relation_id,
                    "spirit_id": spirit_id,
                    "skill_id": skill_id,
                    "learnset_type": bucket_name,
                    "learnset_type_label": bucket_label,
                    "learn_level": parse_int(skill.get("lv")),
                    "learn_level_text": clean_text(skill.get("lv")),
                }
                spirit_skill_rows.append(relation_row)

                flat_skill_rows.append(
                    {
                        "relation_id": relation_id,
                        "spirit_id": spirit_id,
                        "spirit_no": spirit["spirit_no"],
                        "spirit_no_number": spirit["spirit_no_number"],
                        "spirit_base_name": spirit["base_name"],
                        "spirit_display_name": spirit["display_name"],
                        "spirit_form_name": spirit["form_name"],
                        "skill_id": skill_id,
                        "skill_name": clean_text(skill.get("nm")),
                        "skill_type": clean_text(skill.get("tp")),
                        "skill_type_icon_url": to_absolute_url(
                            skill_type_icons.get(skill.get("tp"))
                        ),
                        "attribute_name": clean_text(skill.get("el")),
                        "attribute_icon_url": to_absolute_url(
                            attribute_icons.get(skill.get("el"))
                        ),
                        "energy_cost": parse_int(skill.get("ec")),
                        "power": parse_int(skill.get("pw")),
                        "effect_text": clean_text(skill.get("ef")),
                        "skill_icon_url": to_absolute_url(skill_icons.get(skill.get("nm"))),
                        "learnset_type": bucket_name,
                        "learnset_type_label": bucket_label,
                        "learn_level": parse_int(skill.get("lv")),
                        "learn_level_text": clean_text(skill.get("lv")),
                    }
                )

                relation_id += 1

    return skill_definitions, spirit_skill_rows, flat_skill_rows


def build_manifest(
    attribute_rows: list[dict[str, Any]],
    egg_group_rows: list[dict[str, Any]],
    spirit_rows: list[dict[str, Any]],
    skill_definitions: list[dict[str, Any]],
    spirit_skill_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    hidden_spirit_count = sum(1 for row in spirit_rows if row["is_hidden_detail"])
    exact_egg_count = sum(
        1 for row in spirit_rows if row["egg_data_source"] == "egg_page_exact"
    )
    inferred_egg_count = sum(
        1
        for row in spirit_rows
        if row["egg_data_source"] == "egg_page_same_no_inferred"
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_urls": {
            "compendium_page_url": urljoin(BASE_URL, "index.html"),
            "compendium_data_url": DATA_URL,
            "egg_group_page_url": EGG_PAGE_URL,
        },
        "counts": {
            "attributes": len(attribute_rows),
            "egg_groups": len(egg_group_rows),
            "spirits": len(spirit_rows),
            "hidden_spirits_only_in_detail": hidden_spirit_count,
            "spirits_with_exact_egg_groups": exact_egg_count,
            "spirits_with_same_no_inferred_egg_groups": inferred_egg_count,
            "skill_definitions": len(skill_definitions),
            "spirit_skill_relations": len(spirit_skill_rows),
        },
        "tables": {
            "attributes.json": "属性表",
            "egg_groups.json": "蛋组表",
            "spirits.json": "精灵表",
            "skills.json": "技能表（扁平版，直接带精灵和技能字段）",
            "skill_definitions.json": "技能定义表（规范化附加表）",
            "spirit_skill_relations.json": "精灵-技能关系表（规范化附加表）",
        },
        "notes": [
            "精灵表使用全部详情 ID，共 468 条，不只限于首页列表中的 347 条。",
            "部分隐藏形态的名称、阶段、属性来自详情引用或同编号 NO 分组推断。",
            "图片链接均已展开为完整可访问 URL。",
            "蛋组页原始精灵记录共 429 条，其余 39 条通过同编号 NO.xxx 自动补齐蛋组。",
        ],
    }


def write_mysql_from_output(args: argparse.Namespace, output_dir: Path) -> dict[str, int]:
    try:
        from load_mysql import import_output_to_mysql
    except ImportError as exc:
        raise SystemExit(
            "缺少依赖 pymysql，无法写入 MySQL。请先执行 `python3 -m pip install -r requirements.txt`。"
        ) from exc

    try:
        return import_output_to_mysql(
            output_dir=output_dir,
            host=args.mysql_host,
            port=args.mysql_port,
            user=args.mysql_user,
            password=args.mysql_password,
            database=args.mysql_database,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()

    raw_data = fetch_remote_json(DATA_URL)
    egg_page_html = fetch_remote_text(EGG_PAGE_URL)
    egg_page_data = parse_embedded_json_object(egg_page_html, "data")
    egg_group_icons = parse_embedded_literal_object(egg_page_html, "EGG_ICONS")
    egg_group_descriptions = parse_embedded_literal_object(egg_page_html, "EGG_DESC")
    list_summary_by_id, list_summary_by_no = build_list_summary(raw_data)
    reference_index = build_reference_index(raw_data["d"])
    egg_group_rows = build_egg_group_rows(
        egg_page_data,
        egg_group_icons,
        egg_group_descriptions,
    )
    egg_lookup_by_id, egg_lookup_by_no = build_egg_lookup(egg_page_data)
    egg_group_meta_by_name = {
        row["egg_group_name"]: row for row in egg_group_rows
    }

    attribute_rows = build_attribute_rows(raw_data)
    spirit_rows = build_spirit_rows(
        raw_data,
        list_summary_by_id,
        list_summary_by_no,
        reference_index,
        egg_lookup_by_id,
        egg_lookup_by_no,
        egg_group_meta_by_name,
    )
    skill_definitions, spirit_skill_rows, flat_skill_rows = build_skill_tables(
        raw_data,
        spirit_rows,
    )
    manifest = build_manifest(
        attribute_rows,
        egg_group_rows,
        spirit_rows,
        skill_definitions,
        spirit_skill_rows,
    )

    write_json(output_dir / "attributes.json", attribute_rows)
    write_json(output_dir / "egg_groups.json", egg_group_rows)
    write_json(output_dir / "spirits.json", spirit_rows)
    write_json(output_dir / "skills.json", flat_skill_rows)
    write_json(output_dir / "skill_definitions.json", skill_definitions)
    write_json(output_dir / "spirit_skill_relations.json", spirit_skill_rows)
    write_json(output_dir / "manifest.json", manifest)

    if args.save_source:
        write_json(output_dir / "source_d.json", raw_data)
        write_json(output_dir / "source_egg_data.json", egg_page_data)

    print(f"输出目录: {output_dir}")
    print(f"属性表: {len(attribute_rows)} 条")
    print(f"蛋组表: {len(egg_group_rows)} 条")
    print(f"精灵表: {len(spirit_rows)} 条")
    print(f"技能表(扁平): {len(flat_skill_rows)} 条")
    print(f"技能定义表: {len(skill_definitions)} 条")
    print(f"精灵技能关系表: {len(spirit_skill_rows)} 条")

    if args.write_mysql:
        counts = write_mysql_from_output(args, output_dir)
        print(
            f"MySQL 导入完成: {args.mysql_user}@{args.mysql_host}:{args.mysql_port}/{args.mysql_database}"
        )
        for table_name, count in counts.items():
            print(f"{table_name}: {count}")


if __name__ == "__main__":
    main()
