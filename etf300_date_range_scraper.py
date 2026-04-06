#!/usr/bin/env python3
"""
沪深300ETF历史数据爬虫
支持按日期范围抓取数据
"""

import os
import re
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pandas as pd
import requests
import yaml

try:
    from sqlalchemy import create_engine, text
except ImportError:
    import subprocess
    subprocess.check_call(['pip3', 'install', '-q', 'pymysql', 'sqlalchemy'])
    from sqlalchemy import create_engine, text


# ==================== 配置加载 ====================

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# ==================== 数据库操作 ====================

class ETFDatabase:
    def __init__(self, config: Dict[str, Any]):
        db_cfg = config['database']
        self.connection_str = (
            f"mysql+pymysql://{db_cfg['username']}:{db_cfg['password']}"
            f"@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['name']}"
        )
        self.engine = create_engine(self.connection_str, pool_pre_ping=True)

    def save_data(self, df: pd.DataFrame, fetch_date: str) -> bool:
        """保存数据到数据库"""
        if df is None or df.empty:
            return False

        try:
            conn = self.engine.connect()
            with conn.begin():
                for _, row in df.iterrows():
                    sql = text("""
                        INSERT INTO etf_daily_share (stat_date, sec_code, tot_vol, num)
                        VALUES (:stat_date, :sec_code, :tot_vol, :num)
                        ON DUPLICATE KEY UPDATE tot_vol = :tot_vol, num = :num
                    """)
                    conn.execute(sql, {
                        'stat_date': row['stat_date'],
                        'sec_code': row['sec_code'],
                        'tot_vol': row['tot_vol'],
                        'num': row.get('num', 0)
                    })

                # 更新汇总表
                total_vol = df['tot_vol'].sum()
                etf_count = len(df)

                summary_sql = text("""
                    INSERT INTO etf_daily_summary (stat_date, total_vol, etf_count, avg_vol)
                    VALUES (:stat_date, :total_vol, :etf_count, :avg_vol)
                    ON DUPLICATE KEY UPDATE total_vol = :total_vol, etf_count = :etf_count, avg_vol = :avg_vol
                """)
                conn.execute(summary_sql, {
                    'stat_date': fetch_date,
                    'total_vol': total_vol,
                    'etf_count': etf_count,
                    'avg_vol': total_vol / etf_count if etf_count > 0 else 0
                })

            conn.close()
            return True
        except Exception as e:
            print(f"[ERROR] 保存失败: {e}")
            return False


# ==================== 数据抓取 ====================

class ETFScraper:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.api_config = config['api']
        self.etf_targets = {e['code']: e['name'] for e in config['etf_targets']}

    def fetch_data_by_date(self, target_date: str) -> Optional[pd.DataFrame]:
        """
        按指定日期抓取数据
        """
        params = {
            "jsonCallBack": f"jsonpCallback_{int(time.time())}",
            "isPagination": "true",
            "pageHelp.pageSize": "1000",
            "pageHelp.pageNo": "1",
            "pageHelp.beginPage": "1",
            "pageHelp.cacheSize": "1",
            "pageHelp.endPage": "100",
            "sqlId": self.api_config['sql_id'],
            "STAT_DATE": target_date,  # 指定日期
            "_": str(int(time.time() * 1000)),
        }

        try:
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
                return None

            data = json.loads(match.group(1))
            records = data.get("pageHelp", {}).get("data", [])

            if records:
                df = pd.DataFrame(records)
                # 筛选目标ETF
                target_df = df[df["SEC_CODE"].isin(self.etf_targets.keys())].copy()
                if not target_df.empty:
                    target_df["stat_date"] = target_date
                    target_df["sec_code"] = target_df["SEC_CODE"]
                    target_df["sec_name"] = target_df["SEC_NAME"]
                    target_df["fund_name"] = target_df["SEC_CODE"].map(self.etf_targets)
                    target_df["tot_vol"] = pd.to_numeric(target_df["TOT_VOL"], errors="coerce")
                    target_df["num"] = pd.to_numeric(target_df["NUM"], errors="coerce")
                    return target_df

        except Exception as e:
            print(f"[ERROR] 请求失败: {e}")

        return None

    def fetch_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        抓取日期范围内的所有数据
        """
        all_data = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        total_days = (end - current).days + 1
        print(f"\n开始抓取 {start_date} 到 {end_date}，共 {total_days} 天")

        day_count = 0
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            day_count += 1

            print(f"[{day_count}/{total_days}] 抓取 {date_str}...", end=" ")

            df = self.fetch_data_by_date(date_str)
            if df is not None and not df.empty:
                all_data.append(df)
                print(f"成功 ({len(df)} 条)")
            else:
                print("无数据或失败")

            current += timedelta(days=1)
            time.sleep(0.5)  # 避免请求过快

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()


# ==================== 主流程 ====================

def main():
    import sys

    print("\n" + "=" * 60)
    print("沪深300ETF历史数据爬虫")
    print("=" * 60)

    # 加载配置
    config = load_config()

    # 初始化数据库
    db = ETFDatabase(config)

    # 初始化爬虫
    scraper = ETFScraper(config)

    # 解析日期参数
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        # 默认：2026-04-01 到今天
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = "2026-04-01"

    print(f"日期范围: {start_date} ~ {end_date}")

    # 抓取数据
    df = scraper.fetch_date_range(start_date, end_date)

    if df is None or df.empty:
        print("\n[ERROR] 没有抓取到任何数据")
        return

    print(f"\n共抓取 {len(df)} 条数据")

    # 保存到数据库
    print("\n保存到数据库...")
    saved_count = 0
    for date in df['stat_date'].unique():
        date_df = df[df['stat_date'] == date]
        if db.save_data(date_df, date):
            saved_count += 1

    print(f"已保存 {saved_count} 天的数据到数据库")

    # 显示汇总
    print("\n" + "=" * 60)
    print("数据汇总")
    print("=" * 60)
    summary = df.groupby('stat_date').agg({
        'sec_code': 'count',
        'tot_vol': 'sum'
    }).reset_index()
    summary.columns = ['日期', 'ETF数量', '总份额(万份)']

    for _, row in summary.iterrows():
        print(f"{row['日期']}: {row['ETF数量']}只ETF, 总份额 {row['总份额(万份)']:,.2f}万份")

    print("\n" + "=" * 60)
    print("完成!")
    print("=" * 60)


if __name__ == "__main__":
    from typing import Optional
    main()
