"""
三层共振分析模块
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from data.basic_data import basic_data


class ResonanceAnalyzer:
    """三层共振分析器"""

    def __init__(self):
        self.basic_data = basic_data

    def perform_full_analysis(self, stock_code):
        """执行完整的三层共振分析"""
        try:
            # 获取历史数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')

            stock_data = self.basic_data.get_stock_data(stock_code, 'daily', start_date, end_date)

            if stock_data.empty:
                return {
                    'error': '无法获取股票数据',
                    'resonance_score': 0,
                    'signals': []
                }

            # 计算技术指标
            stock_data = self.basic_data.calculate_technical_indicators(stock_data)

            # 分析结果
            result = {
                'stock_code': stock_code,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'resonance_score': self._calculate_resonance_score(stock_data),
                'trend_analysis': self._analyze_trend(stock_data),
                'volume_analysis': self._analyze_volume(stock_data),
                'technical_analysis': self._analyze_technical(stock_data),
                'signals': self._generate_signals(stock_data)
            }

            logger.info(f"股票 {stock_code} 三层共振分析完成")
            return result

        except Exception as e:
            logger.error(f"三层共振分析失败: {e}")
            return {
                'error': str(e),
                'resonance_score': 0,
                'signals': []
            }

    def _calculate_resonance_score(self, data):
        """计算共振评分"""
        if data.empty or len(data) < 20:
            return 0

        latest = data.iloc[-1]
        score = 0

        # 价格趋势评分
        if 'ma_5' in latest and 'ma_20' in latest:
            if latest['close_price'] > latest['ma_5'] > latest['ma_20']:
                score += 30

        # 成交量评分
        recent_volume = data['volume'].tail(5).mean()
        historical_volume = data['volume'].tail(20).mean()
        if recent_volume > historical_volume * 1.2:
            score += 30

        # 技术指标评分
        if 'change_pct' in latest and latest['change_pct'] > 0:
            score += 20

        # 振幅评分
        if 'amplitude' in latest and latest['amplitude'] > 3:
            score += 20

        return min(score, 100)

    def _analyze_trend(self, data):
        """趋势分析"""
        if data.empty:
            return {'trend': 'unknown', 'strength': 0}

        latest = data.iloc[-1]
        trend = 'sideways'
        strength = 0

        if 'ma_5' in latest and 'ma_20' in latest:
            if latest['close_price'] > latest['ma_5'] > latest['ma_20']:
                trend = 'upward'
                strength = 80
            elif latest['close_price'] < latest['ma_5'] < latest['ma_20']:
                trend = 'downward'
                strength = 80

        return {
            'trend': trend,
            'strength': strength,
            'current_price': latest['close_price'],
            'ma5': latest.get('ma_5', 0),
            'ma20': latest.get('ma_20', 0)
        }

    def _analyze_volume(self, data):
        """成交量分析"""
        if data.empty or len(data) < 10:
            return {'volume_trend': 'unknown', 'volume_ratio': 0}

        recent_volume = data['volume'].tail(5).mean()
        historical_volume = data['volume'].tail(20).mean()
        volume_ratio = recent_volume / historical_volume if historical_volume > 0 else 0

        volume_trend = 'normal'
        if volume_ratio > 1.5:
            volume_trend = 'high'
        elif volume_ratio < 0.7:
            volume_trend = 'low'

        return {
            'volume_trend': volume_trend,
            'volume_ratio': round(volume_ratio, 2),
            'recent_avg_volume': int(recent_volume),
            'historical_avg_volume': int(historical_volume)
        }

    def _analyze_technical(self, data):
        """技术指标分析"""
        if data.empty:
            return {'signals': []}

        latest = data.iloc[-1]
        signals = []

        # 均线信号
        if 'ma_5' in latest and 'ma_20' in latest:
            if latest['close_price'] > latest['ma_5'] > latest['ma_20']:
                signals.append('多头排列')
            elif latest['close_price'] < latest['ma_5'] < latest['ma_20']:
                signals.append('空头排列')

        # 涨跌幅信号
        if 'change_pct' in latest:
            if latest['change_pct'] > 5:
                signals.append('强势上涨')
            elif latest['change_pct'] < -5:
                signals.append('大幅下跌')

        return {
            'signals': signals,
            'change_pct': latest.get('change_pct', 0),
            'amplitude': latest.get('amplitude', 0)
        }

    def _generate_signals(self, data):
        """生成交易信号"""
        if data.empty:
            return []

        signals = []
        latest = data.iloc[-1]

        # 买入信号
        if 'ma_5' in latest and 'ma_20' in latest:
            if (latest['close_price'] > latest['ma_5'] > latest['ma_20'] and
                    latest.get('change_pct', 0) > 2):
                signals.append({
                    'type': 'buy',
                    'signal': '多头排列且上涨',
                    'strength': 'strong'
                })

        # 卖出信号
        if 'change_pct' in latest and latest['change_pct'] < -5:
            signals.append({
                'type': 'sell',
                'signal': '大幅下跌',
                'strength': 'strong'
            })

        return signals

    def analyze_all_stocks(self):
        """分析所有股票的三层共振情况"""
        try:
            from data.enhanced_database import enhanced_db_manager

            # 获取所有股票代码
            stock_sql = "SELECT stock_code, stock_name FROM stock_info LIMIT 100"  # 限制100只股票进行测试
            stock_list = enhanced_db_manager.safe_query_to_dataframe(
                stock_sql, {}, required_tables=['stock_info']
            )

            if stock_list.empty:
                logger.warning("未找到股票数据")
                return []

            resonance_stocks = []

            for _, stock in stock_list.iterrows():
                stock_code = stock['stock_code']
                stock_name = stock['stock_name']

                try:
                    # 分析单个股票
                    result = self.perform_full_analysis(stock_code)

                    # 如果共振评分大于80，认为符合条件
                    if result.get('resonance_score', 0) >= 80:
                        resonance_stocks.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'resonance_score': result['resonance_score'],
                            'signals': result.get('signals', [])
                        })

                except Exception as e:
                    logger.warning(f"分析股票 {stock_code} 失败: {e}")
                    continue

            # 按共振评分排序
            resonance_stocks.sort(key=lambda x: x['resonance_score'], reverse=True)

            logger.info(f"三层共振分析完成，发现 {len(resonance_stocks)} 只符合条件的股票")
            return resonance_stocks

        except Exception as e:
            logger.error(f"批量分析失败: {e}")
            return []


# 创建全局实例
resonance_analyzer = ResonanceAnalyzer()
