"""
API接口测试
"""

import os
import sys
import pytest
import time
import multiprocessing

# 确保项目根目录在路径中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_flask_app():
    """启动Flask应用（在子进程中）"""
    from web_app import app
    app.run(host='127.0.0.1', port=5555, debug=False, use_reloader=False)


@pytest.fixture(scope='module')
def api_base_url():
    """API基础URL"""
    return "http://127.0.0.1:5555"


@pytest.fixture(scope='module')
def flask_server():
    """启动Flask服务器"""
    process = multiprocessing.Process(target=run_flask_app)
    process.start()
    time.sleep(3)  # 等待服务器启动
    yield
    process.terminate()
    process.join()


class TestAPIEndpoints:
    """API端点测试"""

    def test_health_check(self, api_base_url, flask_server):
        """测试健康检查接口"""
        import requests

        response = requests.get(f"{api_base_url}/api/health", timeout=10)
        assert response.status_code == 200

        data = response.json()
        assert data['status'] == 'healthy'
        assert data['database'] == 'connected'

    def test_api_etf_latest(self, api_base_url, flask_server):
        """测试获取最新数据接口"""
        import requests

        response = requests.get(f"{api_base_url}/api/etf/latest", timeout=10)
        assert response.status_code == 200

        data = response.json()
        assert data['success'] is True
        assert len(data['data']) > 0

        # 验证数据结构
        item = data['data'][0]
        assert 'sec_code' in item
        assert 'fund_name' in item
        assert 'tot_vol' in item
        assert 'stat_date' in item

    def test_api_etf_daily(self, api_base_url, flask_server):
        """测试获取每日数据接口"""
        import requests

        response = requests.get(f"{api_base_url}/api/etf/daily?days=30", timeout=10)
        assert response.status_code == 200

        data = response.json()
        assert data['success'] is True
        assert len(data['data']) > 0

    def test_api_etf_daily_range(self, api_base_url, flask_server):
        """测试按日期范围查询接口"""
        import requests

        response = requests.get(
            f"{api_base_url}/api/etf/daily/range?start=2026-01-01&end=2026-01-31",
            timeout=10
        )
        assert response.status_code == 200

        data = response.json()
        assert data['success'] is True
        assert data['count'] > 0
        assert data['etf_count'] == 6

    def test_api_etf_daily_range_invalid(self, api_base_url, flask_server):
        """测试日期范围查询-无效参数"""
        import requests

        # 缺少参数
        response = requests.get(f"{api_base_url}/api/etf/daily/range", timeout=10)
        assert response.status_code == 400

    def test_api_etf_info(self, api_base_url, flask_server):
        """测试获取ETF信息接口"""
        import requests

        response = requests.get(f"{api_base_url}/api/etf/info", timeout=10)
        assert response.status_code == 200

        data = response.json()
        assert data['success'] is True
        assert len(data['data']) >= 6

    def test_api_etf_summary(self, api_base_url, flask_server):
        """测试获取汇总数据接口"""
        import requests

        response = requests.get(f"{api_base_url}/api/etf/summary?days=30", timeout=10)
        assert response.status_code == 200

        data = response.json()
        assert data['success'] is True
        assert len(data['data']) > 0

    def test_api_index_page(self, api_base_url, flask_server):
        """测试首页"""
        import requests

        response = requests.get(api_base_url, timeout=10)
        assert response.status_code == 200
        assert 'text/html' in response.headers.get('Content-Type', '')
        assert '沪深300ETF' in response.text


class TestAPIDataIntegrity:
    """API数据完整性测试"""

    def test_all_dates_have_all_etfs(self, flask_server):
        """测试所有日期都有6只ETF数据"""
        import requests

        response = requests.get(
            "http://127.0.0.1:5555/api/etf/daily?days=365",
            timeout=10
        )
        data = response.json()

        # 按日期分组
        date_groups = {}
        for item in data['data']:
            date = item['stat_date']
            if date not in date_groups:
                date_groups[date] = []
            date_groups[date].append(item['sec_code'])

        # 检查每个日期都有6只ETF
        for date, codes in date_groups.items():
            assert len(codes) == 6, f"日期 {date} 只有 {len(codes)} 只ETF数据"

    def test_tot_vol_is_numeric(self, flask_server):
        """测试份额字段是有效数字"""
        import requests

        response = requests.get(
            "http://127.0.0.1:5555/api/etf/latest",
            timeout=10
        )
        data = response.json()

        for item in data['data']:
            assert isinstance(item['tot_vol'], (int, float))
            assert item['tot_vol'] > 0

    def test_fund_name_consistency(self, flask_server):
        """测试基金名称一致性"""
        import requests

        latest_response = requests.get(
            "http://127.0.0.1:5555/api/etf/latest",
            timeout=10
        )
        info_response = requests.get(
            "http://127.0.0.1:5555/api/etf/info",
            timeout=10
        )

        latest_data = latest_response.json()['data']
        info_data = info_response.json()['data']

        # 建立code到name的映射
        code_to_name = {item['sec_code']: item['fund_name'] for item in info_data}

        # 验证latest中的名称一致性
        for item in latest_data:
            code = item['sec_code']
            if code in code_to_name:
                assert item['fund_name'] == code_to_name[code], \
                    f"基金 {code} 名称不一致: {item['fund_name']} vs {code_to_name[code]}"
