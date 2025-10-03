#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础数据处理器模块
负责基础数据的高级处理和分析
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from core.config import config

class BasicProcessor:
    """基础数据处理器类"""

    def __init__(self):
        self.periods = config.get_periods()

    def calculate_moving_averages(self, data, windows=[5, 10, 20, 60]):
        """计算移动平均线"""
        try:
            data = data.copy()
            for window in windows:
                data[f'ma_{window}'] = data['close_price'].rolling(window).mean()
            return data
        except Exception as e:
            logger.error(f"计算移动平均线失败: {e}")
            return data

    def calculate_bollinger_bands(self, data, window=20, num_std=2):
        """计算布林带"""
        try:
            data = data.copy()
            data['bb_middle'] = data['close_price'].rolling(window).mean()
            data['bb_std'] = data['close_price'].rolling(window).std()
            data['bb_upper'] = data['bb_middle'] + (data['bb_std'] * num_std)
            data['bb_lower'] = data['bb_middle'] - (data['bb_std'] * num_std)
            return data
        except Exception as e:
            logger.error(f"计算布林带失败: {e}")
            return data


# 创建全局实例
basic_processor = BasicProcessor()
