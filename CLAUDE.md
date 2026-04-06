# 沪深300ETF数据抓取与展示平台

## 项目概述

一个自动化工具，用于抓取上海证券交易所（SSE）官网的沪深300ETF规模数据，存储到MySQL数据库，并通过网页实时展示。

**数据来源：** https://www.sse.com.cn/market/funddata/volumn/etfvolumn/

---

## 技术栈

| 组件 | 技术 |
|------|------|
| 数据库 | MySQL 127.0.0.1:3306 |
| 后端 | Flask (Python) |
| 前端 | HTML + Bootstrap + Chart.js |
| 数据抓取 | requests + 自定义爬虫 |
| ORM | SQLAlchemy |

---

## 数据库设计

**数据库名：** `etf_data`

### 表结构

#### 1. etf_info（ETF基础信息）
```sql
- id              INT AUTO_INCREMENT PRIMARY KEY
- sec_code        VARCHAR(10) NOT NULL UNIQUE  -- 证券代码
- sec_name        VARCHAR(50) NOT NULL         -- 证券简称
- fund_name       VARCHAR(100) NOT NULL         -- 基金全称
- etf_type        VARCHAR(20) DEFAULT '跨市'
- listing_date    DATE
- created_at      TIMESTAMP
- updated_at      TIMESTAMP
```

#### 2. etf_daily_share（每日份额明细）
```sql
- id              BIGINT AUTO_INCREMENT PRIMARY KEY
- stat_date       DATE NOT NULL                -- 统计日期
- sec_code        VARCHAR(10) NOT NULL         -- 证券代码
- tot_vol         DECIMAL(20,2) NOT NULL      -- 总份额（万份）
- num             INT                          -- 排名序号
- created_at      TIMESTAMP
- UNIQUE KEY uk_date_code (stat_date, sec_code)  -- 防止重复
- FOREIGN KEY (sec_code) REFERENCES etf_info(sec_code)
```

#### 3. etf_daily_summary（每日汇总）
```sql
- id              BIGINT AUTO_INCREMENT PRIMARY KEY
- stat_date       DATE NOT NULL UNIQUE         -- 统计日期
- total_vol       DECIMAL(20,2) NOT NULL      -- 当日总份额（万份）
- etf_count       INT NOT NULL                -- ETF数量
- avg_vol         DECIMAL(20,2)               -- 平均份额
- created_at      TIMESTAMP
```

#### 4. etf_fetch_log（采集日志）
```sql
- id              BIGINT AUTO_INCREMENT PRIMARY KEY
- fetch_date      DATE NOT NULL                -- 采集日期
- status          VARCHAR(20) NOT NULL         -- success/failed
- records_count   INT DEFAULT 0               -- 采集记录数
- error_msg       TEXT                         -- 错误信息
- created_at      TIMESTAMP
```

### 初始ETF列表

| 基金代码 | 基金名称 |
|---------|---------|
| 510300 | 沪深300ETF华泰柏瑞 |
| 510310 | 沪深300ETF易方达 |
| 510320 | 沪深300ETF中金 |
| 510330 | 沪深300ETF华夏 |
| 510350 | 沪深300ETF工银 |
| 510360 | 沪深300ETF广发 |

---

## 配置文件

### config.yaml
```yaml
database:
  host: "127.0.0.1"
  port: 3306
  username: "root"
  password: "12345678"
  name: "etf_data"

etf_targets:
  - code: "510300"
    name: "沪深300ETF华泰柏瑞"
  - code: "510310"
    name: "沪深300ETF易方达"
  # ... 其他ETF

api:
  base_url: "https://query.sse.com.cn/commonQuery.do"
  sql_id: "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L"
  headers:
    Referer: "https://www.sse.com.cn/"
    User-Agent: "Mozilla/5.0 ..."

output:
  data_dir: "./data"
  output_dir: "./output"
```

---

## 核心脚本

### 1. etf300_db_scraper.py
**功能：** 抓取当天数据并存入数据库

**用法：**
```bash
python3 etf300_db_scraper.py
```

**特点：**
- 支持重复运行（ON DUPLICATE KEY UPDATE）
- 自动初始化ETF基础信息
- 支持重试机制

### 2. etf300_date_range_scraper.py
**功能：** 按日期范围抓取历史数据

**用法：**
```bash
# 抓取指定日期范围
python3 etf300_date_range_scraper.py 2026-01-01 2026-04-05

# 不带参数默认抓取当年1月1日至今
python3 etf300_date_range_scraper.py
```

**API参数：**
- `STAT_DATE=YYYY-MM-DD` 可指定查询特定日期

### 3. web_app.py
**功能：** Flask网页服务，从数据库读取数据展示

**用法：**
```bash
python3 web_app.py
# 访问 http://127.0.0.1:5001
```

---

## API接口

| 接口 | 方法 | 参数 | 说明 |
|------|------|------|------|
| `/api/etf/daily` | GET | days=N | 最近N天数据 |
| `/api/etf/daily/range` | GET | start, end | 指定日期范围 |
| `/api/etf/latest` | GET | - | 最新交易日数据 |
| `/api/etf/summary` | GET | days=N | 每日汇总数据 |
| `/api/etf/info` | GET | - | ETF基础信息 |
| `/api/health` | GET | - | 健康检查 |

---

## 网页功能

### 主页面 (templates/index.html)

1. **日期选择器**
   - 可选择开始/结束日期
   - 默认显示当月1日至今日
   - 查询按钮、重置按钮

2. **统计卡片**
   - 总份额
   - ETF数量
   - 各ETF最新份额

3. **柱状图**
   - 分组柱状图：横轴为日期，每组6个ETF柱子
   - 支持点击柱子查看对应日期的明细

4. **趋势折线图**
   - 显示总份额随时间变化

5. **数据表格**
   - 显示选中日期的各ETF明细
   - 点击柱状图日期更新表格数据

---

## 使用方法

```bash
# 安装依赖
make install

# 抓取当天数据
make run

# 抓取历史数据（示例）
python3 etf300_date_range_scraper.py 2026-01-01 2026-04-05

# 启动网页
make web

# 设置定时任务（每天9点自动抓取）
make schedule

# 查看定时任务
make show-schedule

# 取消定时任务
make unschedule
```

---

## 目录结构

```
sse-funddata/
├── CLAUDE.md              # 本文件
├── config.yaml            # 配置文件
├── Makefile               # 快捷命令
├── requirements.txt       # Python依赖
├── pytest.ini             # pytest配置
├── run.sh                 # 运行脚本
├── web_app.py             # Flask网页服务
├── etf300_db_scraper.py   # 当天数据爬虫
├── etf300_date_range_scraper.py  # 历史数据爬虫
├── database/
│   └── schema.sql         # 数据库表结构
├── templates/
│   └── index.html         # 网页模板
├── tests/                # 测试目录
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_api.py       # API测试
│   ├── test_database.py  # 数据库测试
│   └── test_scraper.py   # 爬虫测试
└── output/              # 图表输出目录
```

---

## 测试

### 运行测试
```bash
# 运行所有测试
python3 -m pytest tests/ -v

# 运行指定测试文件
python3 -m pytest tests/test_api.py -v

# 运行指定类别测试
python3 -m pytest tests/test_database.py -v
```

### 测试覆盖

| 测试文件 | 测试内容 | 测试数量 |
|---------|---------|---------|
| `tests/test_config.py` | 配置加载测试 | 5个 |
| `tests/test_scraper.py` | 爬虫功能测试 | 9个 |
| `tests/test_database.py` | 数据库操作测试 | 12个 |
| `tests/test_api.py` | API接口测试 | 10个 |

### 当前测试状态
```
34 passed, 1 skipped
```

### 添加新测试
在 `tests/` 目录下创建 `test_*.py` 文件，遵循现有测试命名规范。

---

## 扩展指南

### 添加新的ETF

1. 修改 `config.yaml` 中的 `etf_targets` 列表
2. 修改 `database/schema.sql` 中的初始数据
3. 重新初始化数据库：
```bash
mysql -h 127.0.0.1 -u root -p12345678 etf_data < database/schema.sql
```

### 添加新的数据字段

1. 修改 `database/schema.sql` 添加新字段
2. 修改爬虫脚本的字段映射
3. 修改 `web_app.py` 的API查询
4. 修改前端 `templates/index.html` 展示

### 添加新的API接口

在 `web_app.py` 中添加新的路由函数，例如：
```python
@app.route('/api/etf/new_endpoint')
def new_endpoint():
    # 实现逻辑
    return jsonify({...})
```

---

## 注意事项

1. **数据更新：** 上交所API只有当日数据，无历史区间查询。需要每日定时抓取累积历史数据。

2. **休市日：** 周六周日无数据，抓取时会跳过。

3. **API限制：** 请求过于频繁可能导致限流，建议间隔0.5秒以上。

4. **数据库备份：** 重要数据建议定期备份。
