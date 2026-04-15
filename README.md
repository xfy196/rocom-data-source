# Rocom Data Source

抓取洛克王国图鉴数据，导出 JSON，并可直接写入本地 MySQL。

## 1. 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## 2. 只导出 JSON

```bash
python3 main.py --save-source
```

输出目录默认是 `output/`，会生成：

- `attributes.json`
- `egg_groups.json`
- `spirits.json`
- `skills.json`
- `skill_definitions.json`
- `spirit_skill_relations.json`
- `manifest.json`

## 3. 一键导出并写入 MySQL

```bash
python3 main.py \
  --save-source \
  --write-mysql \
  --mysql-host 127.0.0.1 \
  --mysql-port 3306 \
  --mysql-user root \
  --mysql-password 'your_password' \
  --mysql-database rocom
```

执行后会：

1. 抓取图鉴页面和蛋组页面
2. 生成 JSON 文件
3. 自动创建数据库 `rocom`（如果不存在）
4. 自动建表
5. 将数据导入这些表：

- `attribute`
- `egg_group`
- `spirit`
- `spirit_location`
- `spirit_egg_group`
- `skill`
- `spirit_skill`
- `spirit_evolution`
- `spirit_form`

## 4. 使用环境变量

也可以不把账号密码写在命令里：

```bash
export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD='your_password'
export MYSQL_DATABASE=rocom

python3 main.py --save-source --write-mysql
```

## 5. 只把已有 JSON 写入 MySQL

如果你已经生成过 `output/`，可以单独执行：

```bash
python3 load_mysql.py --user root --password 'your_password' --database rocom
```

## 6. 说明

- 表名不会带 `rocom_` 前缀，直接使用业务名，例如 `spirit`。
- `spirits.json` 中已经合并了蛋组信息。
- 一部分详情专属形态的蛋组，是按同编号 `NO.xxx` 自动补齐的。
