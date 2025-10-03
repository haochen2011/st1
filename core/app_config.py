"""
Flask应用配置模块
"""

import os
from datetime import datetime


class Config:
    """Flask应用配置类"""

    # 数据库配置
    DATABASE_HOST = 'localhost'
    DATABASE_PORT = 3306
    DATABASE_USER = 'root'
    DATABASE_PASSWORD = 'your_password'
    DATABASE_NAME = 'stock_analysis'

    # Flask配置
    FLASK_HOST = '0.0.0.0'
    FLASK_PORT = 5000
    FLASK_DEBUG = True

    # 日志配置
    LOG_LEVEL = 'INFO'
    LOG_FILE = './logs/app.log'

    # 数据获取配置
    TIMEOUT_SECONDS = 10
    MAX_RETRIES = 3

    # 业务配置
    UPDATE_INTERVAL_MINUTES = 5
    PRICE_CHANGE_THRESHOLD = 5.0
    VOLUME_RATIO_THRESHOLD = 2.0
    TURNOVER_THRESHOLD = 15.0

    # 数据源配置
    DATA_SOURCES = ['akshare_primary', 'akshare_backup', 'akshare_alternative']

    @classmethod
    def is_market_open(cls):
        """判断市场是否开放"""
        now = datetime.now()
        weekday = now.weekday()

        # 周末不开市
        if weekday >= 5:
            return False

        # 交易时间：9:30-11:30, 13:00-15:00
        time_str = now.strftime('%H:%M')
        morning_open = '09:30' <= time_str <= '11:30'
        afternoon_open = '13:00' <= time_str <= '15:00'

        return morning_open or afternoon_open