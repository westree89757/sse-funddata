#!/usr/bin/env python3
"""
沪深300ETF专项数据爬虫 - 数据库版
从配置文件读取配置，数据存储到MySQL数据库
"""

import os
import re
import json
import time
from datetime import datetime
from typing import List, Dict, Optional, Any

import pandas as pd
import matplotlib.pyplot as plt
import requests
import yaml

try:
    import pymysql
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
except ImportError:
    print("[WARN] pymysql/sqlalchemy 未安装，正在安装...")
    os.system("pip3 install -q pymysql sqlalchemy")
    import pymysql
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker


# ==================== 配置加载 ====================

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """加载配置文件"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# ==================== 数据库操作 ====================

class ETFDatabase:
    """ETF数据库操作类"""

    def __init__(self, config: Dict[str, Any]):
        db_cfg = config['database']
        self.connection_params = {
            'host': db_cfg['host'],
            'port': db_cfg['port'],
            'user': db_cfg['username'],
            'password': db_cfg['password'],
            'database': db_cfg['name'],
            'charset': 'utf8mb4'
        }
        self.engine = None
        self._connect()

    def _connect(self):
        """建立数据库连接"""
        try:
            connection_str = (
                f"mysql+pymysql://{self.connection_params['user']}:{self.connection_params['password']}"
                f"@{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
            )
            self.engine = create_engine(connection_str, echo=False)
            print(f"[INFO] 数据库连接成功: {self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}")
        except Exception as e:
            print(f"[ERROR] 数据库连接失败: {e}")
            raise

    def init_database(self):
        """初始化数据库表"""
        schema_path = os.path.join(os.path.dirname(__file__), "database", "schema.sql")
        if not os.path.exists(schema_path):
            print("[WARN] schema.sql 不存在，跳过初始化")
            return

        with open(schema_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # 分割SQL语句并执行
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]

        conn = self.engine.connect()
        for stmt in statements:
            if stmt:
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    if "Duplicate" not in str(e):
                        print(f"[WARN] SQL执行警告: {e}")
        conn.commit()
        conn.close()
        print("[INFO] 数据库表初始化完成")

    def save_daily_data(self, df: pd.DataFrame, fetch_date: str) -> bool:
        """
        保存每日ETF数据

        Args:
            df: ETF数据DataFrame
            fetch_date: 采集日期

        Returns:
            是否保存成功
        """
        if df is None or df.empty:
            return False

        try:
            conn = self.engine.connect()

            # 开启事务
            with conn.begin():
                # 1. 保存各ETF每日份额数据
                for _, row in df.iterrows():
                    sql = text("""
                        INSERT INTO etf_daily_share (stat_date, sec_code, tot_vol, num)
                        VALUES (:stat_date, :sec_code, :tot_vol, :num)
                        ON DUPLICATE KEY UPDATE tot_vol = :tot_vol, num = :num
                    """)
                    conn.execute(sql, {
                        'stat_date': row['STAT_DATE'],
                        'sec_code': row['SEC_CODE'],
                        'tot_vol': row['TOT_VOL'],
                        'num': row.get('NUM', 0)
                    })

                # 2. 更新汇总数据
                total_vol = df['TOT_VOL'].sum()
                etf_count = len(df)
                avg_vol = total_vol / etf_count if etf_count > 0 else 0

                summary_sql = text("""
                    INSERT INTO etf_daily_summary (stat_date, total_vol, etf_count, avg_vol)
                    VALUES (:stat_date, :total_vol, :etf_count, :avg_vol)
                    ON DUPLICATE KEY UPDATE total_vol = :total_vol, etf_count = :etf_count, avg_vol = :avg_vol
                """)
                conn.execute(summary_sql, {
                    'stat_date': fetch_date,
                    'total_vol': total_vol,
                    'etf_count': etf_count,
                    'avg_vol': avg_vol
                })

                # 3. 记录采集日志
                log_sql = text("""
                    INSERT INTO etf_fetch_log (fetch_date, status, records_count)
                    VALUES (:fetch_date, 'success', :records_count)
                """)
                conn.execute(log_sql, {
                    'fetch_date': fetch_date,
                    'records_count': len(df)
                })

            conn.close()
            print(f"[INFO] 数据已保存到数据库 ({fetch_date}): {len(df)} 条记录")
            return True

        except Exception as e:
            print(f"[ERROR] 数据库保存失败: {e}")
            self._log_error(fetch_date, str(e))
            return False

    def _log_error(self, fetch_date: str, error_msg: str):
        """记录错误日志"""
        try:
            conn = self.engine.connect()
            sql = text("""
                INSERT INTO etf_fetch_log (fetch_date, status, error_msg)
                VALUES (:fetch_date, 'failed', :error_msg)
            """)
            conn.execute(sql, {'fetch_date': fetch_date, 'error_msg': error_msg[:500]})
            conn.commit()
            conn.close()
        except:
            pass

    def get_historical_data(self, days: int = 30) -> Optional[pd.DataFrame]:
        """
        获取历史数据

        Args:
            days: 获取最近N天的数据

        Returns:
            历史数据DataFrame
        """
        try:
            sql = text("""
                SELECT s.stat_date, s.sec_code, i.fund_name, s.tot_vol, s.num
                FROM etf_daily_share s
                JOIN etf_info i ON s.sec_code = i.sec_code
                WHERE s.stat_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY s.stat_date DESC, s.tot_vol DESC
            """)
            conn = self.engine.connect()
            df = pd.read_sql(sql, conn, params={'days': days})
            conn.close()
            return df
        except Exception as e:
            print(f"[ERROR] 获取历史数据失败: {e}")
            return None

    def get_daily_summary(self, days: int = 30) -> Optional[pd.DataFrame]:
        """获取每日汇总数据"""
        try:
            sql = text("""
                SELECT stat_date, total_vol, etf_count, avg_vol
                FROM etf_daily_summary
                WHERE stat_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY stat_date DESC
            """)
            conn = self.engine.connect()
            df = pd.read_sql(sql, conn, params={'days': days})
            conn.close()
            return df
        except Exception as e:
            print(f"[ERROR] 获取汇总数据失败: {e}")
            return None


# ==================== 数据抓取 ====================

class ETFScraper:
    """ETF数据抓取类"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.api_config = config['api']
        self.etf_targets = {e['code']: e['name'] for e in config['etf_targets']}

    def fetch_data(self) -> Optional[pd.DataFrame]:
        """抓取ETF数据"""
        params = {
            "jsonCallBack": f"jsonpCallback_{int(time.time())}",
            "isPagination": "true",
            "pageHelp.pageSize": "1000",
            "pageHelp.pageNo": "1",
            "pageHelp.beginPage": "1",
            "pageHelp.cacheSize": "1",
            "pageHelp.endPage": "100",
            "sqlId": self.api_config['sql_id'],
            "STAT_DATE": "",
            "_": str(int(time.time() * 1000)),
        }

        for attempt in range(2):
            try:
                print(f"[INFO] 正在抓取数据 (第 {attempt + 1} 次)...")
                response = requests.get(
                    self.api_config['base_url'],
                    params=params,
                    headers=self.api_config['headers'],
                    timeout=30
                )
                response.raise_for_status()

                text = response.text
                match = re.search(r'jsonpCallback_\d+\((.+)\)', text)
                if not match:
                    print("[ERROR] 无法解析JSONP格式")
                    return None

                data = json.loads(match.group(1))
                records = data.get("pageHelp", {}).get("data", [])

                if records:
                    df = pd.DataFrame(records)
                    # 筛选目标ETF
                    target_df = df[df["SEC_CODE"].isin(self.etf_targets.keys())].copy()
                    if not target_df.empty:
                        # 添加基金名称
                        target_df["FUND_NAME"] = target_df["SEC_CODE"].map(self.etf_targets)
                        # 转换数值
                        target_df["TOT_VOL"] = pd.to_numeric(target_df["TOT_VOL"], errors="coerce")
                        print(f"[SUCCESS] 获取到 {len(target_df)} 条目标ETF数据")
                        return target_df

            except requests.RequestException as e:
                print(f"[ERROR] 请求失败: {e}")
                if attempt < 1:
                    time.sleep(2)

        return None

    def get_fetch_date(self) -> str:
        """获取采集日期"""
        return datetime.now().strftime("%Y-%m-%d")


# ==================== 可视化 ====================

def create_visualizations(df: pd.DataFrame, output_dir: str):
    """创建可视化图表"""
    if df is None or df.empty:
        print("[WARN] 无数据可绘图")
        return

    import numpy as np
    os.makedirs(output_dir, exist_ok=True)

    # 图1: 各ETF份额柱状图
    fig, ax = plt.subplots(figsize=(12, 6))
    summary = df.groupby(["SEC_CODE", "FUND_NAME"])["TOT_VOL"].last().reset_index()
    summary = summary.sort_values("TOT_VOL", ascending=True)

    colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(summary)))
    bars = ax.barh(summary["FUND_NAME"], summary["TOT_VOL"], color=colors)

    for bar, val in zip(bars, summary["TOT_VOL"]):
        ax.text(val + summary["TOT_VOL"].max() * 0.01, bar.get_y() + bar.get_height()/2,
                f'{val/10000:,.2f}亿', va='center', fontsize=10)

    ax.set_xlabel('基金份额 (万份)', fontsize=11)
    ax.set_title('沪深300ETF份额对比', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'etf300_bar_comparison.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("[INFO] 柱状图已保存")

    # 图2: 复合图表（当有多天数据时）
    if 'STAT_DATE' in df.columns and df['STAT_DATE'].nunique() > 1:
        create_combined_chart(df, output_dir)


def create_combined_chart(df: pd.DataFrame, output_dir: str):
    """创建复合图表"""
    import numpy as np

    fig, ax1 = plt.subplots(figsize=(14, 7))

    pivot_df = df.pivot_table(index="STAT_DATE", columns="FUND_NAME", values="TOT_VOL", aggfunc='first')
    pivot_df = pivot_df.sort_index()

    dates = pivot_df.index.strftime('%Y-%m-%d') if hasattr(pivot_df.index, 'strftime') else [str(d) for d in pivot_df.index]
    x = np.arange(len(dates))
    width = 0.12

    funds = list(pivot_df.columns)
    n_funds = len(funds)
    colors = plt.cm.Set3(np.linspace(0, 1, n_funds))

    for i, (fund, color) in enumerate(zip(funds, colors)):
        values = pivot_df[fund].fillna(0).values
        offset = (i - n_funds/2 + 0.5) * width
        ax1.bar(x + offset, values, width, label=fund, color=color, edgecolor='white')

    ax1.set_xlabel('日期', fontsize=11)
    ax1.set_ylabel('基金份额 (万份)', fontsize=11, color='steelblue')
    ax1.set_xticks(x)
    ax1.set_xticklabels(dates, rotation=45, ha='right')
    ax1.tick_params(axis='y', labelcolor='steelblue')
    ax1.legend(loc='upper left', bbox_to_anchor=(0, 1.15), ncol=3, fontsize=9)

    # 折线图-总份额
    ax2 = ax1.twinx()
    total_shares = pivot_df.sum(axis=1).values
    ax2.plot(x, total_shares, color='red', linewidth=2.5, marker='o', markersize=6, label='总份额')
    ax2.set_ylabel('总份额 (万份)', fontsize=11, color='red')
    ax2.tick_params(axis='y', labelcolor='red')

    for i, val in enumerate(total_shares):
        ax2.annotate(f'{val/10000:,.1f}亿', (x[i], val), textcoords="offset points",
                     xytext=(0, 10), ha='center', fontsize=8, color='red')

    plt.title('沪深300ETF每日份额：柱状图(各ETF) + 折线图(总份额)', fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'etf300_combined_chart.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("[INFO] 复合图表已保存")


# ==================== Excel导出 ====================

def export_to_excel(df: pd.DataFrame, output_dir: str):
    """导出数据到Excel"""
    if df is None or df.empty:
        return

    os.makedirs(output_dir, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    filepath = os.path.join(output_dir, f"etf300_data_{today}.xlsx")

    # 统一列名
    df = df.copy()
    df.columns = [c.upper() if c.islower() else c for c in df.columns]

    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        # 各ETF明细
        detail_cols = ["STAT_DATE", "SEC_CODE", "FUND_NAME"]
        if "ETF_TYPE" in df.columns:
            detail_cols.append("ETF_TYPE")
        detail_cols.append("TOT_VOL")

        detail = df[[c for c in detail_cols if c in df.columns]].copy()
        detail = detail.sort_values(["SEC_CODE", "STAT_DATE"])
        detail.to_excel(writer, sheet_name="各ETF明细", index=False)

        # 每日汇总
        if "STAT_DATE" in df.columns:
            daily = df.groupby("STAT_DATE").agg({"TOT_VOL": ["sum", "mean", "count"]}).reset_index()
            daily.columns = ["日期", "总份额(万份)", "平均份额(万份)", "ETF数量"]
            daily.to_excel(writer, sheet_name="每日汇总", index=False)

        # 各ETF汇总
        if "FUND_NAME" in df.columns:
            fund_sum = df.groupby(["SEC_CODE", "FUND_NAME"]).agg({"TOT_VOL": ["first", "last", "mean"]}).reset_index()
            fund_sum.columns = ["基金代码", "基金名称", "期初份额", "期末份额", "平均份额"]
            fund_sum.to_excel(writer, sheet_name="各ETF汇总", index=False)

    print(f"[INFO] Excel已保存: {filepath}")


# ==================== 主流程 ====================

def main():
    print("\n" + "#" * 60)
    print("#  沪深300ETF数据爬虫 (数据库版)")
    print("#" * 60)

    # 加载配置
    try:
        config = load_config("config.yaml")
        print("[INFO] 配置文件加载成功")
    except Exception as e:
        print(f"[ERROR] 配置加载失败: {e}")
        return

    # 初始化数据库
    try:
        db = ETFDatabase(config)
        db.init_database()
    except Exception as e:
        print(f"[ERROR] 数据库初始化失败: {e}")
        return

    # 抓取数据
    scraper = ETFScraper(config)
    df = scraper.fetch_data()

    if df is None or df.empty:
        print("[ERROR] 数据抓取失败")
        return

    # 保存到数据库
    fetch_date = scraper.get_fetch_date()
    db.save_daily_data(df, fetch_date)

    # 从数据库获取历史数据（最近30天）
    df_history = db.get_historical_data(days=30)
    if df_history is not None and not df_history.empty:
        print_summary(df_history)
        export_to_excel(df_history, config['output']['output_dir'])
        create_visualizations(df_history, config['output']['output_dir'])
    else:
        # 只有当日数据
        print_summary(df)
        export_to_excel(df, config['output']['output_dir'])
        create_visualizations(df, config['output']['output_dir'])

    print("\n" + "#" * 60)
    print("#  执行完成!")
    print("#" * 60)


def print_summary(df: pd.DataFrame):
    """打印数据摘要"""
    if df is None or df.empty:
        return

    # 统一列名（小写转大写适配）
    df.columns = [c.upper() if c.islower() else c for c in df.columns]

    print("\n" + "=" * 60)
    print("数据摘要")
    print("=" * 60)

    if 'STAT_DATE' in df.columns:
        print(f"日期范围: {df['STAT_DATE'].min()} ~ {df['STAT_DATE'].max()}")
    print(f"ETF数量: {df['SEC_CODE'].nunique()} 只")

    if 'TOT_VOL' in df.columns:
        print(f"总份额: {df['TOT_VOL'].sum():,.2f} 万份")

    print("\n各ETF最新数据:")
    if 'STAT_DATE' in df.columns:
        latest = df.loc[df.groupby('SEC_CODE')['STAT_DATE'].idxmax()]
    else:
        latest = df

    if 'FUND_NAME' in latest.columns:
        for _, row in latest.sort_values('TOT_VOL', ascending=False).iterrows():
            print(f"  {row['SEC_CODE']} {row.get('FUND_NAME', '')}: {row['TOT_VOL']:,.2f}万份")

    print("=" * 60)


if __name__ == "__main__":
    main()
