from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import pymysql


TABLES_IN_TRUNCATE_ORDER = [
    "spirit_form",
    "spirit_evolution",
    "spirit_skill",
    "spirit_egg_group",
    "spirit_location",
    "skill",
    "spirit",
    "egg_group",
    "attribute",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="将导出的 JSON 数据写入 MySQL。")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MYSQL_PASSWORD"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "rocom"))
    parser.add_argument("--output-dir", default="output")
    return parser.parse_args()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def connect_mysql(
    host: str,
    port: int,
    user: str,
    password: str | None,
    database: str | None = None,
) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.Cursor,
    )


def ensure_database(
    host: str, port: int, user: str, password: str | None, database: str
) -> None:
    conn = connect_mysql(host, port, user, password)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.commit()
    finally:
        conn.close()


def create_tables(conn: pymysql.connections.Connection) -> None:
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS `attribute` (
          `id` INT NOT NULL,
          `name` VARCHAR(64) NOT NULL,
          `color` VARCHAR(32) NULL,
          `icon_relative_path` VARCHAR(255) NULL,
          `icon_url` VARCHAR(512) NULL,
          `sort_order` INT NOT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `uk_attribute_name` (`name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS `egg_group` (
          `id` INT NOT NULL,
          `name` VARCHAR(64) NOT NULL,
          `icon` VARCHAR(32) NULL,
          `description` VARCHAR(255) NULL,
          `is_unbreedable_group` TINYINT(1) NOT NULL DEFAULT 0,
          `sort_order` INT NOT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `uk_egg_group_name` (`name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS `spirit` (
          `id` INT NOT NULL,
          `spirit_no` VARCHAR(32) NULL,
          `spirit_no_number` INT NULL,
          `base_name` VARCHAR(255) NULL,
          `display_name` VARCHAR(255) NOT NULL,
          `form_name` VARCHAR(255) NULL,
          `stage_name` VARCHAR(64) NULL,
          `primary_attribute_id` INT NULL,
          `secondary_attribute_id` INT NULL,
          `trait_name` VARCHAR(255) NULL,
          `trait_icon_url` VARCHAR(512) NULL,
          `trait_effect` TEXT NULL,
          `nickname` VARCHAR(255) NULL,
          `description` TEXT NULL,
          `height_text` VARCHAR(64) NULL,
          `weight_text` VARCHAR(64) NULL,
          `location_text` TEXT NULL,
          `race_total` INT NULL,
          `hp` INT NULL,
          `attack` INT NULL,
          `magic_attack` INT NULL,
          `defense` INT NULL,
          `magic_defense` INT NULL,
          `speed` INT NULL,
          `image_relative_path` VARCHAR(255) NULL,
          `image_url` VARCHAR(512) NULL,
          `shiny_image_relative_path` VARCHAR(255) NULL,
          `shiny_image_url` VARCHAR(512) NULL,
          `has_shiny_variant` TINYINT(1) NOT NULL DEFAULT 0,
          `can_breed` TINYINT(1) NOT NULL DEFAULT 0,
          `is_unbreedable` TINYINT(1) NOT NULL DEFAULT 0,
          `egg_data_source` VARCHAR(64) NULL,
          `source_in_list` TINYINT(1) NOT NULL DEFAULT 0,
          `is_hidden_detail` TINYINT(1) NOT NULL DEFAULT 0,
          `name_source` VARCHAR(64) NULL,
          `attribute_source` VARCHAR(64) NULL,
          `stage_source` VARCHAR(64) NULL,
          PRIMARY KEY (`id`),
          KEY `idx_spirit_no` (`spirit_no`),
          KEY `idx_spirit_display_name` (`display_name`),
          KEY `idx_spirit_base_name` (`base_name`),
          KEY `idx_spirit_primary_attribute_id` (`primary_attribute_id`),
          KEY `idx_spirit_secondary_attribute_id` (`secondary_attribute_id`),
          CONSTRAINT `fk_spirit_primary_attribute`
            FOREIGN KEY (`primary_attribute_id`) REFERENCES `attribute` (`id`),
          CONSTRAINT `fk_spirit_secondary_attribute`
            FOREIGN KEY (`secondary_attribute_id`) REFERENCES `attribute` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS `spirit_location` (
          `spirit_id` INT NOT NULL,
          `sort_order` INT NOT NULL,
          `location_name` VARCHAR(255) NOT NULL,
          PRIMARY KEY (`spirit_id`, `sort_order`),
          KEY `idx_spirit_location_name` (`location_name`),
          CONSTRAINT `fk_spirit_location_spirit`
            FOREIGN KEY (`spirit_id`) REFERENCES `spirit` (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS `spirit_egg_group` (
          `spirit_id` INT NOT NULL,
          `egg_group_id` INT NOT NULL,
          `sort_order` INT NOT NULL,
          PRIMARY KEY (`spirit_id`, `egg_group_id`),
          KEY `idx_spirit_egg_group_group_id` (`egg_group_id`),
          CONSTRAINT `fk_spirit_egg_group_spirit`
            FOREIGN KEY (`spirit_id`) REFERENCES `spirit` (`id`) ON DELETE CASCADE,
          CONSTRAINT `fk_spirit_egg_group_group`
            FOREIGN KEY (`egg_group_id`) REFERENCES `egg_group` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS `skill` (
          `id` INT NOT NULL,
          `name` VARCHAR(255) NOT NULL,
          `skill_type` VARCHAR(64) NULL,
          `skill_type_icon_url` VARCHAR(512) NULL,
          `attribute_id` INT NULL,
          `energy_cost` INT NULL,
          `power` INT NULL,
          `effect_text` TEXT NULL,
          `skill_icon_url` VARCHAR(512) NULL,
          PRIMARY KEY (`id`),
          KEY `idx_skill_name` (`name`),
          KEY `idx_skill_attribute_id` (`attribute_id`),
          CONSTRAINT `fk_skill_attribute`
            FOREIGN KEY (`attribute_id`) REFERENCES `attribute` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS `spirit_skill` (
          `id` INT NOT NULL,
          `spirit_id` INT NOT NULL,
          `skill_id` INT NOT NULL,
          `learnset_type` VARCHAR(32) NOT NULL,
          `learnset_type_label` VARCHAR(64) NOT NULL,
          `learn_level` INT NULL,
          `learn_level_text` VARCHAR(32) NULL,
          PRIMARY KEY (`id`),
          KEY `idx_spirit_skill_spirit_id` (`spirit_id`),
          KEY `idx_spirit_skill_skill_id` (`skill_id`),
          CONSTRAINT `fk_spirit_skill_spirit`
            FOREIGN KEY (`spirit_id`) REFERENCES `spirit` (`id`) ON DELETE CASCADE,
          CONSTRAINT `fk_spirit_skill_skill`
            FOREIGN KEY (`skill_id`) REFERENCES `skill` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS `spirit_evolution` (
          `spirit_id` INT NOT NULL,
          `chain_order` INT NOT NULL,
          `evolution_spirit_id` INT NOT NULL,
          `evolution_level` INT NULL,
          `evolution_level_text` VARCHAR(32) NULL,
          PRIMARY KEY (`spirit_id`, `chain_order`),
          KEY `idx_spirit_evolution_target` (`evolution_spirit_id`),
          CONSTRAINT `fk_spirit_evolution_spirit`
            FOREIGN KEY (`spirit_id`) REFERENCES `spirit` (`id`) ON DELETE CASCADE,
          CONSTRAINT `fk_spirit_evolution_target`
            FOREIGN KEY (`evolution_spirit_id`) REFERENCES `spirit` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS `spirit_form` (
          `spirit_id` INT NOT NULL,
          `form_order` INT NOT NULL,
          `form_spirit_id` INT NOT NULL,
          PRIMARY KEY (`spirit_id`, `form_order`),
          KEY `idx_spirit_form_target` (`form_spirit_id`),
          CONSTRAINT `fk_spirit_form_spirit`
            FOREIGN KEY (`spirit_id`) REFERENCES `spirit` (`id`) ON DELETE CASCADE,
          CONSTRAINT `fk_spirit_form_target`
            FOREIGN KEY (`form_spirit_id`) REFERENCES `spirit` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    ]

    with conn.cursor() as cur:
        for ddl in ddl_statements:
            cur.execute(ddl)
    conn.commit()


def truncate_tables(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table_name in TABLES_IN_TRUNCATE_ORDER:
            cur.execute(f"TRUNCATE TABLE `{table_name}`")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()


def insert_attributes(
    conn: pymysql.connections.Connection, attributes: list[dict[str, Any]]
) -> dict[str, int]:
    rows = [
        (
            row["attribute_id"],
            row["attribute_name"],
            row.get("attribute_color"),
            row.get("icon_relative_path"),
            row.get("icon_url"),
            row["sort_order"],
        )
        for row in attributes
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO `attribute`
            (`id`, `name`, `color`, `icon_relative_path`, `icon_url`, `sort_order`)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()
    return {row["attribute_name"]: row["attribute_id"] for row in attributes}


def insert_egg_groups(
    conn: pymysql.connections.Connection, egg_groups: list[dict[str, Any]]
) -> dict[str, int]:
    rows = [
        (
            row["egg_group_id"],
            row["egg_group_name"],
            row.get("egg_group_icon"),
            row.get("egg_group_description"),
            int(bool(row.get("is_unbreedable_group"))),
            row["sort_order"],
        )
        for row in egg_groups
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO `egg_group`
            (`id`, `name`, `icon`, `description`, `is_unbreedable_group`, `sort_order`)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()
    return {row["egg_group_name"]: row["egg_group_id"] for row in egg_groups}


def insert_spirits(
    conn: pymysql.connections.Connection,
    spirits: list[dict[str, Any]],
    attribute_name_to_id: dict[str, int],
) -> None:
    spirit_rows = []
    location_rows = []
    spirit_egg_group_rows = []
    evolution_rows = []
    form_rows = []

    for spirit in spirits:
        spirit_rows.append(
            (
                spirit["spirit_id"],
                spirit.get("spirit_no"),
                spirit.get("spirit_no_number"),
                spirit.get("base_name"),
                spirit["display_name"],
                spirit.get("form_name"),
                spirit.get("stage_name"),
                attribute_name_to_id.get(spirit.get("primary_attribute")),
                attribute_name_to_id.get(spirit.get("secondary_attribute")),
                spirit.get("trait_name"),
                spirit.get("trait_icon_url"),
                spirit.get("trait_effect"),
                spirit.get("nickname"),
                spirit.get("description"),
                spirit.get("height_text"),
                spirit.get("weight_text"),
                spirit.get("location_text"),
                spirit.get("race_total"),
                spirit.get("hp"),
                spirit.get("attack"),
                spirit.get("magic_attack"),
                spirit.get("defense"),
                spirit.get("magic_defense"),
                spirit.get("speed"),
                spirit.get("image_relative_path"),
                spirit.get("image_url"),
                spirit.get("shiny_image_relative_path"),
                spirit.get("shiny_image_url"),
                int(bool(spirit.get("has_shiny_variant"))),
                int(bool(spirit.get("can_breed"))),
                int(bool(spirit.get("is_unbreedable"))),
                spirit.get("egg_data_source"),
                int(bool(spirit.get("source_in_list"))),
                int(bool(spirit.get("is_hidden_detail"))),
                spirit.get("name_source"),
                spirit.get("attribute_source"),
                spirit.get("stage_source"),
            )
        )

        for index, location_name in enumerate(spirit.get("locations", []), start=1):
            location_rows.append((spirit["spirit_id"], index, location_name))

        for index, egg_group in enumerate(spirit.get("egg_groups", []), start=1):
            spirit_egg_group_rows.append((spirit["spirit_id"], egg_group, index))

        for index, evo in enumerate(spirit.get("evolution_chain", []), start=1):
            evolution_rows.append(
                (
                    spirit["spirit_id"],
                    index,
                    evo["spirit_id"],
                    evo.get("evolution_level"),
                    evo.get("evolution_level_text"),
                )
            )

        for index, form in enumerate(spirit.get("forms", []), start=1):
            form_rows.append((spirit["spirit_id"], index, form["spirit_id"]))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO `spirit` (
              `id`, `spirit_no`, `spirit_no_number`, `base_name`, `display_name`,
              `form_name`, `stage_name`, `primary_attribute_id`, `secondary_attribute_id`,
              `trait_name`, `trait_icon_url`, `trait_effect`, `nickname`, `description`,
              `height_text`, `weight_text`, `location_text`, `race_total`, `hp`, `attack`,
              `magic_attack`, `defense`, `magic_defense`, `speed`, `image_relative_path`,
              `image_url`, `shiny_image_relative_path`, `shiny_image_url`,
              `has_shiny_variant`, `can_breed`, `is_unbreedable`, `egg_data_source`,
              `source_in_list`, `is_hidden_detail`, `name_source`, `attribute_source`,
              `stage_source`
            ) VALUES (
              %s, %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s,
              %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s
            )
            """,
            spirit_rows,
        )

        if location_rows:
            cur.executemany(
                """
                INSERT INTO `spirit_location`
                (`spirit_id`, `sort_order`, `location_name`)
                VALUES (%s, %s, %s)
                """,
                location_rows,
            )

        if spirit_egg_group_rows:
            cur.executemany(
                """
                INSERT INTO `spirit_egg_group`
                (`spirit_id`, `egg_group_id`, `sort_order`)
                VALUES (%s, %s, %s)
                """,
                spirit_egg_group_rows,
            )

        if evolution_rows:
            cur.executemany(
                """
                INSERT INTO `spirit_evolution`
                (`spirit_id`, `chain_order`, `evolution_spirit_id`, `evolution_level`, `evolution_level_text`)
                VALUES (%s, %s, %s, %s, %s)
                """,
                evolution_rows,
            )

        if form_rows:
            cur.executemany(
                """
                INSERT INTO `spirit_form`
                (`spirit_id`, `form_order`, `form_spirit_id`)
                VALUES (%s, %s, %s)
                """,
                form_rows,
            )
    conn.commit()


def insert_skills(
    conn: pymysql.connections.Connection,
    skills: list[dict[str, Any]],
    spirit_skill_relations: list[dict[str, Any]],
    attribute_name_to_id: dict[str, int],
) -> None:
    skill_rows = [
        (
            row["skill_id"],
            row["skill_name"],
            row.get("skill_type"),
            row.get("skill_type_icon_url"),
            attribute_name_to_id.get(row.get("attribute_name")),
            row.get("energy_cost"),
            row.get("power"),
            row.get("effect_text"),
            row.get("skill_icon_url"),
        )
        for row in skills
    ]
    relation_rows = [
        (
            row["relation_id"],
            row["spirit_id"],
            row["skill_id"],
            row["learnset_type"],
            row["learnset_type_label"],
            row.get("learn_level"),
            row.get("learn_level_text"),
        )
        for row in spirit_skill_relations
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO `skill`
            (`id`, `name`, `skill_type`, `skill_type_icon_url`, `attribute_id`,
             `energy_cost`, `power`, `effect_text`, `skill_icon_url`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            skill_rows,
        )

        cur.executemany(
            """
            INSERT INTO `spirit_skill`
            (`id`, `spirit_id`, `skill_id`, `learnset_type`, `learnset_type_label`, `learn_level`, `learn_level_text`)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            relation_rows,
        )
    conn.commit()


def fetch_table_counts(conn: pymysql.connections.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for table_name in [
            "attribute",
            "egg_group",
            "spirit",
            "spirit_location",
            "spirit_egg_group",
            "skill",
            "spirit_skill",
            "spirit_evolution",
            "spirit_form",
        ]:
            cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            counts[table_name] = int(cur.fetchone()[0])
    return counts


def import_output_to_mysql(
    output_dir: str | Path,
    host: str,
    port: int,
    user: str,
    password: str | None,
    database: str,
) -> dict[str, int]:
    output_dir = Path(output_dir).resolve()

    if not password:
        raise ValueError("缺少 MySQL 密码，请通过参数或 MYSQL_PASSWORD 提供。")

    attributes = load_json(output_dir / "attributes.json")
    egg_groups = load_json(output_dir / "egg_groups.json")
    spirits = load_json(output_dir / "spirits.json")
    skills = load_json(output_dir / "skill_definitions.json")
    spirit_skill_relations = load_json(output_dir / "spirit_skill_relations.json")

    ensure_database(host, port, user, password, database)
    conn = connect_mysql(
        host,
        port,
        user,
        password,
        database,
    )

    try:
        create_tables(conn)
        truncate_tables(conn)
        attribute_name_to_id = insert_attributes(conn, attributes)
        egg_group_name_to_id = insert_egg_groups(conn, egg_groups)

        for spirit in spirits:
            spirit["egg_groups"] = [
                egg_group_name_to_id[name]
                for name in spirit.get("egg_groups", [])
                if name in egg_group_name_to_id
            ]

        insert_spirits(conn, spirits, attribute_name_to_id)
        insert_skills(conn, skills, spirit_skill_relations, attribute_name_to_id)
        counts = fetch_table_counts(conn)
    finally:
        conn.close()

    return counts


def main() -> None:
    args = parse_args()
    counts = import_output_to_mysql(
        output_dir=args.output_dir,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    print(f"database: {args.database}")
    for table_name, count in counts.items():
        print(f"{table_name}: {count}")


if __name__ == "__main__":
    main()
