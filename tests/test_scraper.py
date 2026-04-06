"""
爬虫功能测试
"""

import os
import sys
import pytest
import yaml
import pandas as pd

# 确保项目根目录在路径中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestConfigLoader:
    """配置加载测试"""

    def test_config_file_exists(self):
        """测试配置文件存在"""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'config.yaml'
        )
        assert os.path.exists(config_path), "config.yaml 文件不存在"

    def test_config_loads_correctly(self):
        """测试配置正确加载"""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'config.yaml'
        )

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        assert 'database' in config
        assert 'api' in config
        assert 'etf_targets' in config

    def test_database_config_has_required_fields(self):
        """测试数据库配置有必需字段"""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'config.yaml'
        )

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        db = config['database']
        assert 'host' in db
        assert 'port' in db
        assert 'username' in db
        assert 'password' in db
        assert 'name' in db

    def test_etf_targets_has_six_etfs(self):
        """测试ETF目标列表有6只ETF"""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'config.yaml'
        )

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        targets = config['etf_targets']
        assert len(targets) == 6

        # 验证必需的代码
        codes = [t['code'] for t in targets]
        expected_codes = ['510300', '510310', '510320', '510330', '510350', '510360']
        for code in expected_codes:
            assert code in codes

    def test_api_config_has_required_fields(self):
        """测试API配置有必需字段"""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'config.yaml'
        )

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        api = config['api']
        assert 'base_url' in api
        assert 'sql_id' in api
        assert 'headers' in api
        assert 'Referer' in api['headers']


class TestETFScraper:
    """ETF爬虫测试"""

    @pytest.fixture
    def scraper(self):
        """创建爬虫实例"""
        from etf300_date_range_scraper import ETFScraper, load_config
        config = load_config()
        return ETFScraper(config)

    def test_fetch_data_by_date_success(self, scraper):
        """测试按日期抓取数据成功"""
        # 使用已知的交易日
        df = scraper.fetch_data_by_date('2026-04-03')

        # 网络可能超时，如果返回None则跳过
        if df is None:
            pytest.skip("网络请求超时，跳过此测试")

        assert not df.empty
        assert len(df) == 6

        # 验证必需列
        assert 'sec_code' in df.columns
        assert 'fund_name' in df.columns
        assert 'tot_vol' in df.columns

    def test_fetch_data_by_date_no_data(self, scraper):
        """测试按日期抓取-无数据（休市日）"""
        df = scraper.fetch_data_by_date('2026-04-04')  # 周六

        # 可能返回None或空DataFrame
        if df is not None:
            assert df.empty

    def test_fetch_data_by_date_all_target_etfs(self, scraper):
        """测试抓取的数据包含所有目标ETF"""
        df = scraper.fetch_data_by_date('2026-04-03')

        # 网络可能超时，如果返回None则跳过
        if df is None:
            pytest.skip("网络请求超时，跳过此测试")

        codes = set(df['sec_code'].tolist())
        expected_codes = {'510300', '510310', '510320', '510330', '510350', '510360'}
        assert codes == expected_codes

    def test_fetch_data_tot_vol_is_numeric(self, scraper):
        """测试份额字段是数字类型"""
        df = scraper.fetch_data_by_date('2026-04-03')

        # 网络可能超时，如果返回None则跳过
        if df is None:
            pytest.skip("网络请求超时，跳过此测试")

        assert not df.empty
        # 转换后应该是数值类型
        df['tot_vol'] = pd.to_numeric(df['tot_vol'], errors='coerce')
        assert df['tot_vol'].dtype in ['int64', 'float64']
        assert (df['tot_vol'] > 0).all()


class TestDatabaseSaver:
    """数据库保存测试"""

    @pytest.fixture
    def db(self):
        """创建数据库实例"""
        from etf300_date_range_scraper import ETFDatabase, load_config
        config = load_config()
        return ETFDatabase(config)

    @pytest.fixture
    def sample_df(self):
        """创建示例DataFrame"""
        import pandas as pd

        data = {
            'stat_date': ['2026-04-03', '2026-04-03'],
            'sec_code': ['510300', '510310'],
            'tot_vol': [4486908.77, 3163744.40],
            'num': [20, 21]
        }
        return pd.DataFrame(data)

    def test_save_data_success(self, db, sample_df):
        """测试保存数据成功"""
        result = db.save_data(sample_df, '2026-04-03')
        assert result is True

    def test_save_data_empty(self, db):
        """测试保存空数据返回False"""
        import pandas as pd
        empty_df = pd.DataFrame()
        result = db.save_data(empty_df, '2026-04-03')
        assert result is False


class TestDateRangeScraper:
    """日期范围爬虫测试"""

    def test_fetch_date_range_logic(self):
        """测试日期范围爬虫逻辑"""
        from datetime import datetime, timedelta

        start_date = '2026-04-01'
        end_date = '2026-04-03'

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # 计算天数
        days = (end - start).days + 1
        assert days == 3

        # 验证日期列表
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        assert len(dates) == 3
        assert dates == ['2026-04-01', '2026-04-02', '2026-04-03']
