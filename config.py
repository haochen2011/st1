"""
配置管理模块
负责读取和管理系统配置
"""

import configparser
import os
from pathlib import Path
from loguru import logger


class Config:
    """配置管理类"""

    def __init__(self, config_file='config.ini'):
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.load_config()

    def load_config(self):
        """加载配置文件"""
        if os.path.exists(self.config_file):
            self.config.read(self.config_file, encoding='utf-8')
            logger.info(f"配置文件 {self.config_file} 加载成功")
        else:
            self.create_default_config()

    def create_default_config(self):
        """创建默认配置文件"""
        self.config['database'] = {
            'host': 'localhost',
            'port': '3306',
            'user': 'root',
            'password': 'your_password',
            'database': 'stock_analysis'
        }

        self.config['data_path'] = {
            'tick_data': './data/tick',
            'basic_data': './data/basic',
            'indicator_data': './data/indicator'
        }

        self.config['api'] = {
            'host': '0.0.0.0',
            'port': '5000',
            'debug': 'True'
        }

        self.config['logging'] = {
            'level': 'INFO',
            'file': './logs/stock_analysis.log'
        }

        self.config['stock'] = {
            'market_codes': 'sh,sz',
            'default_period': 'daily',
            'periods': '1min,5min,10min,15min,30min,1hour,daily,week,month,quarter,half-year,year'
        }

        self.config['data_fetch'] = {
            'timeout': '10',
            'max_retries': '3',
            'retry_delay': '2',
            'enable_backup_sources': 'True'
        }

        self.config['thresholds'] = {
            'price_change_threshold': '5.0',
            'volume_ratio_threshold': '2.0',
            'turnover_threshold': '15.0',
            'update_interval_minutes': '5'
        }

        # 保存配置文件
        with open(self.config_file, 'w', encoding='utf-8') as f:
            self.config.write(f)
        logger.info(f"默认配置文件 {self.config_file} 创建成功")

    def get(self, section, key, fallback=None):
        """获取配置值"""
        return self.config.get(section, key, fallback=fallback)

    def getint(self, section, key, fallback=None):
        """获取整数配置值"""
        return self.config.getint(section, key, fallback=fallback)

    def getboolean(self, section, key, fallback=None):
        """获取布尔配置值"""
        return self.config.getboolean(section, key, fallback=fallback)

    def get_data_path(self, data_type):
        """获取数据路径"""
        path = self.get('data_path', data_type, './')
        Path(path).mkdir(parents=True, exist_ok=True)
        return path

    def get_periods(self):
        """获取支持的周期列表"""
        periods_str = self.get('stock', 'periods', 'daily')
        return [p.strip() for p in periods_str.split(',')]

    def get_market_codes(self):
        """获取支持的市场代码列表"""
        codes_str = self.get('stock', 'market_codes', 'sh,sz')
        return [c.strip() for c in codes_str.split(',')]

    def get_data_fetch_timeout(self):
        """获取数据获取超时时间"""
        return self.getint('data_fetch', 'timeout', 10)

    def get_max_retries(self):
        """获取最大重试次数"""
        return self.getint('data_fetch', 'max_retries', 3)

    def get_retry_delay(self):
        """获取重试延迟时间"""
        return self.getint('data_fetch', 'retry_delay', 2)

    def is_backup_sources_enabled(self):
        """是否启用备用数据源"""
        return self.getboolean('data_fetch', 'enable_backup_sources', True)

    def get_price_change_threshold(self):
        """获取价格变动阈值"""
        return self.getfloat('thresholds', 'price_change_threshold', 5.0)

    def get_volume_ratio_threshold(self):
        """获取成交量比率阈值"""
        return self.getfloat('thresholds', 'volume_ratio_threshold', 2.0)

    def get_turnover_threshold(self):
        """获取换手率阈值"""
        return self.getfloat('thresholds', 'turnover_threshold', 15.0)

    def get_update_interval_minutes(self):
        """获取更新间隔（分钟）"""
        return self.getint('thresholds', 'update_interval_minutes', 5)

    def getfloat(self, section, key, fallback=None):
        """获取浮点数配置值"""
        try:
            return self.config.getfloat(section, key)
        except (ValueError, KeyError):
            return fallback


# 全局配置实例
config = Config()