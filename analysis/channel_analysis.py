"""
多空通道分析模块
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger


class ChannelAnalyzer:
    """多空通道分析器"""

    def __init__(self):
        pass

    def perform_full_channel_analysis(self, stock_data):
        """执行完整的通道分析"""
        try:
            if stock_data.empty or len(stock_data) < 20:
                return {
                    'error': '数据不足，无法进行通道分析',
                    'channel_info': {}
                }

            # 计算通道线
            channel_data = self._calculate_channel_lines(stock_data)

            # 分析通道状态
            channel_status = self._analyze_channel_status(channel_data)

            # 生成交易建议
            trading_suggestions = self._generate_trading_suggestions(channel_data, channel_status)

            result = {
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'data_period': f"{stock_data.iloc[0]['trade_date']} 至 {stock_data.iloc[-1]['trade_date']}",
                'channel_info': {
                    'upper_line': channel_status['upper_line'],
                    'lower_line': channel_status['lower_line'],
                    'middle_line': channel_status['middle_line'],
                    'current_price': channel_status['current_price'],
                    'channel_width': channel_status['channel_width']
                },
                'channel_status': channel_status,
                'trading_suggestions': trading_suggestions,
                'support_resistance': self._find_support_resistance(stock_data)
            }

            logger.info("多空通道分析完成")
            return result

        except Exception as e:
            logger.error(f"多空通道分析失败: {e}")
            return {
                'error': str(e),
                'channel_info': {}
            }

    def _calculate_channel_lines(self, data):
        """计算通道线"""
        data = data.copy()

        # 计算移动平均线作为中轨
        data['middle_line'] = data['close_price'].rolling(window=20).mean()

        # 计算标准差
        data['std'] = data['close_price'].rolling(window=20).std()

        # 计算上轨和下轨（布林带方式）
        data['upper_line'] = data['middle_line'] + (data['std'] * 2)
        data['lower_line'] = data['middle_line'] - (data['std'] * 2)

        # 计算高点和低点通道（线性回归方式）
        if len(data) >= 20:
            # 最近20天的高点连线
            recent_highs = data['high_price'].tail(20)
            high_trend = np.polyfit(range(len(recent_highs)), recent_highs, 1)
            data['high_trend_line'] = np.poly1d(high_trend)(range(len(data)))

            # 最近20天的低点连线
            recent_lows = data['low_price'].tail(20)
            low_trend = np.polyfit(range(len(recent_lows)), recent_lows, 1)
            data['low_trend_line'] = np.poly1d(low_trend)(range(len(data)))

        return data

    def _analyze_channel_status(self, data):
        """分析通道状态"""
        if data.empty:
            return {}

        latest = data.iloc[-1]

        current_price = latest['close_price']
        upper_line = latest.get('upper_line', 0)
        lower_line = latest.get('lower_line', 0)
        middle_line = latest.get('middle_line', 0)

        # 计算通道宽度
        channel_width = (upper_line - lower_line) / middle_line * 100 if middle_line > 0 else 0

        # 判断价格在通道中的位置
        if upper_line > 0 and lower_line > 0:
            position_ratio = (current_price - lower_line) / (upper_line - lower_line)
        else:
            position_ratio = 0.5

        # 判断通道方向
        if len(data) >= 10:
            middle_trend = (data['middle_line'].iloc[-1] - data['middle_line'].iloc[-10]) / data['middle_line'].iloc[
                -10] * 100
        else:
            middle_trend = 0

        # 通道状态判断
        if position_ratio > 0.8:
            position_status = '接近上轨'
        elif position_ratio < 0.2:
            position_status = '接近下轨'
        else:
            position_status = '通道中部'

        return {
            'current_price': current_price,
            'upper_line': upper_line,
            'lower_line': lower_line,
            'middle_line': middle_line,
            'channel_width': round(channel_width, 2),
            'position_ratio': round(position_ratio, 2),
            'position_status': position_status,
            'channel_trend': '上升' if middle_trend > 1 else '下降' if middle_trend < -1 else '横盘',
            'channel_trend_value': round(middle_trend, 2)
        }

    def _generate_trading_suggestions(self, data, status):
        """生成交易建议"""
        suggestions = []

        position_ratio = status.get('position_ratio', 0.5)
        channel_trend = status.get('channel_trend', '横盘')

        # 基于通道位置的建议
        if position_ratio < 0.2:  # 接近下轨
            if channel_trend == '上升':
                suggestions.append({
                    'type': 'buy',
                    'reason': '价格接近上升通道下轨，支撑较强',
                    'confidence': 'high'
                })
            else:
                suggestions.append({
                    'type': 'wait',
                    'reason': '价格接近下轨，但通道趋势不明，建议观望',
                    'confidence': 'medium'
                })

        elif position_ratio > 0.8:  # 接近上轨
            if channel_trend == '下降':
                suggestions.append({
                    'type': 'sell',
                    'reason': '价格接近下降通道上轨，阻力较大',
                    'confidence': 'high'
                })
            else:
                suggestions.append({
                    'type': 'wait',
                    'reason': '价格接近上轨，建议观察是否突破',
                    'confidence': 'medium'
                })

        else:  # 通道中部
            if channel_trend == '上升':
                suggestions.append({
                    'type': 'hold',
                    'reason': '价格在上升通道中部，趋势良好',
                    'confidence': 'medium'
                })
            else:
                suggestions.append({
                    'type': 'wait',
                    'reason': '价格在通道中部，等待方向明确',
                    'confidence': 'low'
                })

        return suggestions

    def _find_support_resistance(self, data):
        """寻找支撑阻力位"""
        if data.empty or len(data) < 10:
            return {
                'support_levels': [],
                'resistance_levels': []
            }

        # 简化的支撑阻力位计算
        recent_data = data.tail(30)  # 最近30天

        # 寻找局部高点作为阻力位
        resistance_levels = []
        for i in range(2, len(recent_data) - 2):
            current_high = recent_data.iloc[i]['high_price']
            if (current_high > recent_data.iloc[i - 1]['high_price'] and
                    current_high > recent_data.iloc[i - 2]['high_price'] and
                    current_high > recent_data.iloc[i + 1]['high_price'] and
                    current_high > recent_data.iloc[i + 2]['high_price']):
                resistance_levels.append(current_high)

        # 寻找局部低点作为支撑位
        support_levels = []
        for i in range(2, len(recent_data) - 2):
            current_low = recent_data.iloc[i]['low_price']
            if (current_low < recent_data.iloc[i - 1]['low_price'] and
                    current_low < recent_data.iloc[i - 2]['low_price'] and
                    current_low < recent_data.iloc[i + 1]['low_price'] and
                    current_low < recent_data.iloc[i + 2]['low_price']):
                support_levels.append(current_low)

        # 去重并排序
        resistance_levels = sorted(list(set(resistance_levels)), reverse=True)[:3]
        support_levels = sorted(list(set(support_levels)), reverse=True)[:3]

        return {
            'support_levels': support_levels,
            'resistance_levels': resistance_levels
        }

    def analyze_channels(self):
        """分析所有股票的多空通道状态"""
        try:
            from data.enhanced_database import enhanced_db_manager
            from data.basic_data import basic_data

            # 获取股票列表
            stock_sql = "SELECT stock_code, stock_name FROM stock_info LIMIT 30"  # 限制30只股票进行测试
            stock_list = enhanced_db_manager.safe_query_to_dataframe(
                stock_sql, {}, required_tables=['stock_info']
            )

            if stock_list.empty:
                logger.warning("未找到股票数据")
                return []

            channel_results = []

            for _, stock in stock_list.iterrows():
                stock_code = stock['stock_code']
                stock_name = stock['stock_name']

                try:
                    # 获取股票历史数据
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

                    stock_data = basic_data.get_stock_data(stock_code, 'daily', start_date, end_date)

                    if stock_data.empty:
                        continue

                    # 分析通道
                    result = self.perform_full_channel_analysis(stock_data)

                    if 'error' not in result:
                        channel_status = result.get('channel_status', {})

                        # 简化通道状态判断
                        position_status = channel_status.get('position_status', '通道中部')
                        channel_trend = channel_status.get('channel_trend', '横盘')

                        if channel_trend == '上升' and position_status in ['接近下轨', '通道中部']:
                            status = 'bullish'
                        elif channel_trend == '下降' and position_status in ['接近上轨', '通道中部']:
                            status = 'bearish'
                        else:
                            status = 'neutral'

                        channel_results.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'channel_status': status,
                            'channel_trend': channel_trend,
                            'position_status': position_status,
                            'channel_width': channel_status.get('channel_width', 0)
                        })

                except Exception as e:
                    logger.warning(f"分析股票 {stock_code} 通道失败: {e}")
                    continue

            logger.info(f"多空通道分析完成，共分析 {len(channel_results)} 只股票")
            return channel_results

        except Exception as e:
            logger.error(f"批量通道分析失败: {e}")
            return []


# 创建全局实例
channel_analyzer = ChannelAnalyzer()
