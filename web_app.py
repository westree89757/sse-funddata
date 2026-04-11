#!/usr/bin/env python3
"""
沪深300ETF数据展示平台 - Flask后端
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, render_template, jsonify, request
import pandas as pd
from sqlalchemy import create_engine, text

import yaml

# ==================== 配置加载 ====================

def load_config(config_path: str = "config.yaml") -> dict:
    """加载配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# ==================== Flask应用 ====================

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# 加载配置
config = load_config()

# 数据库连接
db_params = config['database']
connection_str = (
    f"mysql+pymysql://{db_params['username']}:{db_params['password']}"
    f"@{db_params['host']}:{db_params['port']}/{db_params['name']}"
)
engine = create_engine(connection_str, pool_pre_ping=True)


# ==================== 初始化 ====================

def init_index_table():
    """初始化指数表"""
    try:
        sql = text("""
            CREATE TABLE IF NOT EXISTS index_daily_summary (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stat_date DATE NOT NULL UNIQUE COMMENT '统计日期',
                index_code VARCHAR(10) NOT NULL DEFAULT '000001' COMMENT '指数代码',
                index_name VARCHAR(50) NOT NULL DEFAULT '上证指数' COMMENT '指数名称',
                total_amount DECIMAL(20,2) NOT NULL COMMENT '成交额(万元)',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_stat_date (stat_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数每日汇总表'
        """)
        with engine.connect() as conn:
            conn.execute(sql)
        print("[INFO] 指数表初始化完成")
    except Exception as e:
        print(f"[WARN] 指数表初始化异常: {e}")


# ==================== API路由 ====================

@app.route('/')
def index():
    """首页"""
    return render_template('index.html')


@app.route('/api/etf/daily')
def get_daily_data():
    """
    获取每日明细数据
    Query params:
        days: 最近N天的数据（默认30）
    """
    days = request.args.get('days', 30, type=int)

    try:
        sql = text("""
            SELECT
                s.stat_date,
                s.sec_code,
                i.fund_name,
                s.tot_vol,
                s.num
            FROM etf_daily_share s
            JOIN etf_info i ON s.sec_code = i.sec_code
            WHERE s.stat_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
            ORDER BY s.stat_date DESC, s.tot_vol DESC
        """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={'days': days})

        # 转换日期为字符串
        df['stat_date'] = df['stat_date'].astype(str)

        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etf/summary')
def get_summary_data():
    """
    获取每日汇总数据
    Query params:
        days: 最近N天的数据（默认30）
    """
    days = request.args.get('days', 30, type=int)

    try:
        sql = text("""
            SELECT
                stat_date,
                total_vol,
                etf_count,
                avg_vol
            FROM etf_daily_summary
            WHERE stat_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
            ORDER BY stat_date DESC
        """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={'days': days})

        df['stat_date'] = df['stat_date'].astype(str)

        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etf/daily/range')
def get_daily_by_range():
    """
    按日期范围获取数据
    Query params:
        start: 开始日期 (YYYY-MM-DD)
        end: 结束日期 (YYYY-MM-DD)
    """
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    if not start_date or not end_date:
        return jsonify({'success': False, 'error': '请提供start和end参数'}), 400

    try:
        sql = text("""
            SELECT
                s.stat_date,
                s.sec_code,
                i.fund_name,
                s.tot_vol,
                s.num
            FROM etf_daily_share s
            JOIN etf_info i ON s.sec_code = i.sec_code
            WHERE s.stat_date >= :start_date AND s.stat_date <= :end_date
            ORDER BY s.stat_date DESC, s.tot_vol DESC
        """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={'start_date': start_date, 'end_date': end_date})

        df['stat_date'] = df['stat_date'].astype(str)

        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df),
            'etf_count': df['sec_code'].nunique() if len(df) > 0 else 0
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etf/latest')
def get_latest_data():
    """获取最新各ETF数据"""
    try:
        sql = text("""
            SELECT
                s.sec_code,
                i.fund_name,
                s.tot_vol,
                s.stat_date
            FROM etf_daily_share s
            JOIN etf_info i ON s.sec_code = i.sec_code
            WHERE s.stat_date = (
                SELECT MAX(stat_date) FROM etf_daily_share
            )
            ORDER BY s.tot_vol DESC
        """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        df['stat_date'] = df['stat_date'].astype(str)

        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'latest_date': df['stat_date'].iloc[0] if len(df) > 0 else None
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etf/info')
def get_etf_info():
    """获取ETF基本信息"""
    try:
        sql = text("SELECT sec_code, sec_name, fund_name FROM etf_info ORDER BY sec_code")
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        return jsonify({
            'success': True,
            'data': df.to_dict('records')
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/health')
def health_check():
    """健康检查"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


@app.route('/api/index/summary')
def get_index_summary():
    """
    获取上证指数每日成交额
    Query params:
        days: 最近N天的数据（默认30）
    """
    days = request.args.get('days', 30, type=int)

    try:
        sql = text("""
            SELECT
                stat_date,
                total_amount
            FROM index_daily_summary
            WHERE stat_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
            ORDER BY stat_date DESC
        """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={'days': days})

        df['stat_date'] = df['stat_date'].astype(str)

        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== 静态文件 ====================

@app.route('/css/<path:filename>')
def serve_css(filename):
    return app.send_static_file(f'css/{filename}')


@app.route('/js/<path:filename>')
def serve_js(filename):
    return app.send_static_file(f'js/{filename}')


# ==================== 启动 ====================

def get_latest_date_in_db():
    """获取数据库中最新日期"""
    try:
        sql = text("SELECT MAX(stat_date) as max_date FROM etf_daily_share")
        with engine.connect() as conn:
            result = conn.execute(sql)
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except:
        return None


def get_latest_index_date_in_db():
    """获取数据库中上证指数最新日期"""
    try:
        sql = text("SELECT MAX(stat_date) as max_date FROM index_daily_summary")
        with engine.connect() as conn:
            result = conn.execute(sql)
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except:
        return None


def fetch_index_data_by_date(target_date: str) -> dict:
    """从东方财富API获取指定日期的上证指数成交额"""
    try:
        # 东方财富K线API
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": "1.000001",  # 上海证券交易所的上证指数
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",  # 日K线
            "fqt": "0",  # 不复权
            "beg": target_date.replace("-", ""),
            "end": target_date.replace("-", ""),
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Referer": "https://finance.eastmoney.com/"
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()

        data = response.json()
        klines = data.get("data", {}).get("klines", [])

        if klines:
            # 返回格式: "日期,开盘,收盘,最高,最低,成交量,成交额,..."
            fields = klines[0].split(",")
            # fields[6] = 成交额(万元)
            return {
                "stat_date": fields[0],
                "total_amount": float(fields[6]) if len(fields) > 6 else 0
            }
    except Exception as e:
        print(f"[指数抓取] 请求失败: {e}")
    return None


def save_index_data(date: str, total_amount: float):
    """保存上证指数数据到数据库"""
    try:
        sql = text("""
            INSERT INTO index_daily_summary (stat_date, index_code, index_name, total_amount)
            VALUES (:stat_date, '000001', '上证指数', :total_amount)
            ON DUPLICATE KEY UPDATE total_amount = :total_amount
        """)
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(sql, {'stat_date': date, 'total_amount': total_amount})
        return True
    except Exception as e:
        print(f"[指数保存] 保存失败: {e}")
        return False


def auto_fetch_data():
    """自动获取最新数据"""
    from etf300_date_range_scraper import ETFScraper, ETFDatabase

    config = load_config()
    db = ETFDatabase(config)
    scraper = ETFScraper(config)

    latest_date = get_latest_date_in_db()
    today = datetime.now().strftime("%Y-%m-%d")

    if latest_date is None:
        print("[提示] 数据库无数据，跳过自动获取")
        return

    latest_str = latest_date.strftime("%Y-%m-%d") if hasattr(latest_date, 'strftime') else str(latest_date)
    # 从最新日期的下一天开始
    start_date = (datetime.strptime(latest_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    days_gap = (datetime.now() - datetime.strptime(latest_str, "%Y-%m-%d")).days

    print(f"[自动获取] 数据库最新日期: {latest_str}, 距今天数: {days_gap}天")

    if days_gap > 90:
        print("[自动获取] 间隔超过90天，跳过自动获取")
        return

    if days_gap <= 0:
        print("[自动获取] 数据已是最新，无需获取")
        return

    print(f"[自动获取] 开始获取 {start_date} 到 {today} 的数据...")
    all_data = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(today, "%Y-%m-%d")

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        print(f"[自动获取] 抓取 {date_str}...", end=" ")

        df = scraper.fetch_data_by_date(date_str)
        if df is not None and not df.empty:
            all_data.append(df)
            print(f"成功 ({len(df)} 条)")
        else:
            print("无数据（节假日），跳过")

        current += timedelta(days=1)
        import time as time_module
        time_module.sleep(0.5)

    # 保存ETF数据
    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        print(f"[自动获取] 共抓取 {len(df)} 条数据，保存到数据库...")
        saved_count = 0
        for date in df['stat_date'].unique():
            date_df = df[df['stat_date'] == date]
            if db.save_data(date_df, date):
                saved_count += 1
        print(f"[自动获取] 已保存 {saved_count} 天的ETF数据")

    # 获取上证指数数据（独立于ETF数据）
    index_start_date = get_latest_index_date_in_db()
    if index_start_date is None:
        # 如果没有指数数据，从2026-01-01开始
        index_start_date = "2026-01-01"
    else:
        index_start_date = (datetime.strptime(index_start_date.strftime("%Y-%m-%d"), "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    index_end_date = today
    if datetime.strptime(index_start_date, "%Y-%m-%d") <= datetime.strptime(index_end_date, "%Y-%m-%d"):
        print(f"[自动获取] 开始获取上证指数 {index_start_date} 到 {index_end_date} 的数据...")
        index_current = datetime.strptime(index_start_date, "%Y-%m-%d")
        index_end = datetime.strptime(index_end_date, "%Y-%m-%d")

        while index_current <= index_end:
            index_date_str = index_current.strftime("%Y-%m-%d")
            print(f"[自动获取] 抓取上证指数 {index_date_str}...", end=" ")

            index_data = fetch_index_data_by_date(index_date_str)
            if index_data and index_data.get('total_amount', 0) > 0:
                if save_index_data(index_date_str, index_data['total_amount']):
                    print(f"成功 (成交额: {index_data['total_amount']:,.2f}万元)")
                else:
                    print("保存失败")
            else:
                print("无数据")

            index_current += timedelta(days=1)
            import time as time_module
            time_module.sleep(0.3)


if __name__ == '__main__':
    print("=" * 60)
    print("沪深300ETF数据展示平台")
    print("=" * 60)

    # 初始化指数表
    init_index_table()

    # 自动获取最新数据
    auto_fetch_data()

    print(f"访问地址: http://127.0.0.1:5001")
    print("按 Ctrl+C 停止服务")
    print("=" * 60)
    app.run(host='0.0.0.0', port=5001, debug=True)
