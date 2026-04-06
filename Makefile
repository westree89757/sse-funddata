.PHONY: run install web schedule clean

# 安装依赖
install:
	pip3 install -q pandas requests pymysql sqlalchemy pyyaml flask openpyxl matplotlib

# 运行爬虫（抓取当天数据）
run:
	@chmod +x run.sh
	@./run.sh

# 启动Web展示平台
web:
	@echo "启动沪深300ETF数据展示平台..."
	@python3 web_app.py

# 设置定时任务（每天9点抓取数据）
schedule:
	@echo "设置定时任务：每天 9:00 自动抓取"
	@(crontab -l 2>/dev/null | grep -v "etf300_db_scraper"; echo "0 9 * * * cd $$(pwd) && /usr/bin/python3 etf300_db_scraper.py >> $$(pwd)/logs/cron.log 2>&1") | crontab -
	@echo "定时任务已设置!"

# 取消定时任务
unschedule:
	@crontab -l 2>/dev/null | grep -v "etf300_db_scraper" | crontab -
	@echo "定时任务已取消"

# 查看定时任务
show-schedule:
	@echo "当前定时任务:"
	@crontab -l 2>/dev/null | grep "etf" || echo "无"

# 清理输出文件
clean:
	rm -rf output/*.png
	@echo "已清理图表文件"

# 查看日志
log:
	@tail -50 logs/cron.log 2>/dev/null || echo "无日志文件"
