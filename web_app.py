#!/usr/bin/env python3
"""
沪深300ETF数据展示平台 - Flask后端
"""

import os
from datetime import datetime
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


# ==================== 静态文件 ====================

@app.route('/css/<path:filename>')
def serve_css(filename):
    return app.send_static_file(f'css/{filename}')


@app.route('/js/<path:filename>')
def serve_js(filename):
    return app.send_static_file(f'js/{filename}')


# ==================== 启动 ====================

if __name__ == '__main__':
    print("=" * 60)
    print("沪深300ETF数据展示平台")
    print("=" * 60)
    print(f"访问地址: http://127.0.0.1:5001")
    print("按 Ctrl+C 停止服务")
    print("=" * 60)
    app.run(host='0.0.0.0', port=5001, debug=True)
