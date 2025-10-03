"""
技术指标分析模块
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger


class TechnicalAnalyzer:
    """技术指标分析器"""

    def __init__(self):
        pass

    def calculate_all_indicators(self, data):
        """计算所有技术指标"""
        if data.empty:
            return data

        try:
            data = data.copy()
            data = data.sort_values('trade_date')

            # 移动平均线
            data = self._calculate_moving_averages(data)

            # RSI
            data = self._calculate_rsi(data)

            # MACD
            data = self._calculate_macd(data)

            # 布林带
            data = self._calculate_bollinger_bands(data)

            # 成交量指标
            data = self._calculate_volume_indicators(data)

            # 价格指标
            data = self._calculate_price_indicators(data)

            logger.info("技术指标计算完成")
            return data

        except Exception as e:
            logger.error(f"技术指标计算失败: {e}")
            return data

    def _calculate_moving_averages(self, data):
        """计算移动平均线"""
        periods = [5, 10, 20, 30, 60]

        for period in periods:
            data[f'ma_{period}'] = data['close_price'].rolling(window=period).mean()

        return data

    def _calculate_rsi(self, data, period=14):
        """计算RSI相对强弱指标"""
        delta = data['close_price'].diff()

        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        rs = gain / loss
        data['rsi'] = 100 - (100 / (1 + rs))

        return data

    def _calculate_macd(self, data, fast=12, slow=26, signal=9):
        """计算MACD指标"""
        ema_fast = data['close_price'].ewm(span=fast).mean()
        ema_slow = data['close_price'].ewm(span=slow).mean()

        data['macd_dif'] = ema_fast - ema_slow
        data['macd_dea'] = data['macd_dif'].ewm(span=signal).mean()
        data['macd_histogram'] = data['macd_dif'] - data['macd_dea']

        return data

    def _calculate_bollinger_bands(self, data, period=20, std_dev=2):
        """计算布林带"""
        data['bb_middle'] = data['close_price'].rolling(window=period).mean()
        data['bb_std'] = data['close_price'].rolling(window=period).std()

        data['bb_upper'] = data['bb_middle'] + (data['bb_std'] * std_dev)
        data['bb_lower'] = data['bb_middle'] - (data['bb_std'] * std_dev)

        # 布林带位置
        data['bb_position'] = (data['close_price'] - data['bb_lower']) / (data['bb_upper'] - data['bb_lower'])

        return data

    def _calculate_volume_indicators(self, data):
        """计算成交量指标"""
        # 成交量移动平均
        data['volume_ma_5'] = data['volume'].rolling(window=5).mean()
        data['volume_ma_20'] = data['volume'].rolling(window=20).mean()

        # 成交量比率
        data['volume_ratio'] = data['volume'] / data['volume_ma_20']

        # 量价确认
        price_change = data['close_price'].pct_change()
        volume_change = data['volume'].pct_change()
        data['volume_price_trend'] = np.where(
            (price_change > 0) & (volume_change > 0), 1,  # 价涨量增
            np.where((price_change < 0) & (volume_change > 0), -1, 0)  # 价跌量增
        )

        return data

    def _calculate_price_indicators(self, data):
        """计算价格指标"""
        # 价格变化
        if 'change_pct' not in data.columns:
            data['change_pct'] = data['close_price'].pct_change() * 100

        # 振幅
        if 'amplitude' not in data.columns:
            data['amplitude'] = ((data['high_price'] - data['low_price']) / data['close_price'].shift(1)) * 100

        # 价格位置（在当日高低点中的位置）
        data['price_position'] = (data['close_price'] - data['low_price']) / (data['high_price'] - data['low_price'])

        # 突破标记
        data['ma5_break'] = (data['close_price'] > data['ma_5']) & (
                    data['close_price'].shift(1) <= data['ma_5'].shift(1))
        data['ma20_break'] = (data['close_price'] > data['ma_20']) & (
                    data['close_price'].shift(1) <= data['ma_20'].shift(1))

        # 高亮蜡烛图标记（用于前端显示）
        data['highlight_candle'] = (
                (data['change_pct'] > 5) |  # 大涨
                (data['change_pct'] < -5) |  # 大跌
                (data['volume_ratio'] > 2) |  # 放量
                (data['ma5_break']) |  # 突破5日线
                (data['ma20_break'])  # 突破20日线
        )

        return data

    def generate_trading_signals(self, data):
        """生成交易信号"""
        if data.empty or len(data) < 20:
            return []

        signals = []
        latest = data.iloc[-1]

        # RSI信号
        if 'rsi' in latest:
            rsi_value = latest['rsi']
            if rsi_value > 70:
                signals.append({
                    'type': 'sell',
                    'indicator': 'RSI',
                    'description': f'RSI超买({rsi_value:.1f})',
                    'strength': 'medium'
                })
            elif rsi_value < 30:
                signals.append({
                    'type': 'buy',
                    'indicator': 'RSI',
                    'description': f'RSI超卖({rsi_value:.1f})',
                    'strength': 'medium'
                })

        # MACD信号
        if 'macd_dif' in latest and 'macd_dea' in latest:
            if latest['macd_dif'] > latest['macd_dea'] and data.iloc[-2]['macd_dif'] <= data.iloc[-2]['macd_dea']:
                signals.append({
                    'type': 'buy',
                    'indicator': 'MACD',
                    'description': 'MACD金叉',
                    'strength': 'strong'
                })
            elif latest['macd_dif'] < latest['macd_dea'] and data.iloc[-2]['macd_dif'] >= data.iloc[-2]['macd_dea']:
                signals.append({
                    'type': 'sell',
                    'indicator': 'MACD',
                    'description': 'MACD死叉',
                    'strength': 'strong'
                })

        # 布林带信号
        if 'bb_position' in latest:
            bb_pos = latest['bb_position']
            if bb_pos > 1:
                signals.append({
                    'type': 'sell',
                    'indicator': 'Bollinger',
                    'description': '价格突破布林带上轨',
                    'strength': 'medium'
                })
            elif bb_pos < 0:
                signals.append({
                    'type': 'buy',
                    'indicator': 'Bollinger',
                    'description': '价格跌破布林带下轨',
                    'strength': 'medium'
                })

        # 均线信号
        if 'ma_5' in latest and 'ma_20' in latest:
            if latest['ma5_break']:
                signals.append({
                    'type': 'buy',
                    'indicator': 'MA',
                    'description': '价格突破5日均线',
                    'strength': 'medium'
                })
            elif latest['ma20_break']:
                signals.append({
                    'type': 'buy',
                    'indicator': 'MA',
                    'description': '价格突破20日均线',
                    'strength': 'strong'
                })

        # 成交量信号
        if 'volume_ratio' in latest and latest['volume_ratio'] > 2:
            change_pct = latest.get('change_pct', 0)
            if change_pct > 3:
                signals.append({
                    'type': 'buy',
                    'indicator': 'Volume',
                    'description': f'放量上涨({latest["volume_ratio"]:.1f}倍量)',
                    'strength': 'strong'
                })
            elif change_pct < -3:
                signals.append({
                    'type': 'sell',
                    'indicator': 'Volume',
                    'description': f'放量下跌({latest["volume_ratio"]:.1f}倍量)',
                    'strength': 'strong'
                })

        return signals


# 创建全局实例
technical_analyzer = TechnicalAnalyzer()