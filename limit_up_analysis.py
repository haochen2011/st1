"""
涨停板分析模块
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from data_fetcher import data_fetcher


class LimitUpAnalyzer:
    """涨停板分析器"""

    def __init__(self):
        self.data_fetcher = data_fetcher

    def generate_limit_up_report(self, trade_date):
        """生成涨停板报告"""
        try:
            # 获取股票列表
            stock_list = self.data_fetcher.get_stock_list()

            if stock_list.empty:
                return {
                    'error': '无法获取股票列表',
                    'limit_up_stocks': [],
                    'total_count': 0
                }

            limit_up_stocks = []

            # 检查前100只股票的涨停情况（实际应用中可以检查所有股票）
            sample_stocks = stock_list.head(100)

            for _, stock in sample_stocks.iterrows():
                try:
                    stock_code = stock.get('code', '')
                    if not stock_code:
                        continue

                    # 获取当日数据
                    historical_data = self.data_fetcher.get_historical_data(
                        stock_code,
                        trade_date.replace('-', ''),
                        trade_date.replace('-', '')
                    )

                    if not historical_data.empty:
                        latest = historical_data.iloc[-1]
                        change_pct = latest.get('change_pct', 0)

                        # 判断是否涨停（A股一般涨幅限制为10%，ST股为5%）
                        is_st = stock.get('name', '').startswith('ST')
                        limit_threshold = 4.5 if is_st else 9.5  # 考虑到实际交易中的微小差异

                        if change_pct >= limit_threshold:
                            limit_up_stocks.append({
                                'stock_code': stock_code,
                                'stock_name': stock.get('name', ''),
                                'current_price': latest.get('close_price', 0),
                                'change_pct': round(change_pct, 2),
                                'volume': latest.get('volume', 0),
                                'amount': latest.get('amount', 0),
                                'first_limit_time': '09:30:00',  # 简化处理
                                'limit_up_type': 'ST涨停' if is_st else '普通涨停'
                            })

                except Exception as e:
                    logger.warning(f"检查股票 {stock_code} 涨停情况失败: {e}")
                    continue

            # 按涨幅排序
            limit_up_stocks.sort(key=lambda x: x['change_pct'], reverse=True)

            # 分析统计
            analysis = self._analyze_limit_up_pattern(limit_up_stocks)

            result = {
                'trade_date': trade_date,
                'total_count': len(limit_up_stocks),
                'limit_up_stocks': limit_up_stocks,
                'analysis': analysis
            }

            logger.info(f"{trade_date} 涨停板分析完成，共发现 {len(limit_up_stocks)} 只涨停股")
            return result

        except Exception as e:
            logger.error(f"涨停板分析失败: {e}")
            return {
                'error': str(e),
                'limit_up_stocks': [],
                'total_count': 0
            }

    def _analyze_limit_up_pattern(self, limit_up_stocks):
        """分析涨停板模式"""
        if not limit_up_stocks:
            return {
                'st_count': 0,
                'normal_count': 0,
                'avg_volume': 0,
                'high_volume_count': 0
            }

        st_count = sum(1 for stock in limit_up_stocks if stock['limit_up_type'] == 'ST涨停')
        normal_count = len(limit_up_stocks) - st_count

        volumes = [stock['volume'] for stock in limit_up_stocks if stock['volume'] > 0]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0

        # 成交量较大的涨停（简化判断）
        high_volume_count = sum(1 for vol in volumes if vol > avg_volume * 1.5)

        return {
            'st_count': st_count,
            'normal_count': normal_count,
            'avg_volume': int(avg_volume),
            'high_volume_count': high_volume_count,
            'market_sentiment': self._judge_market_sentiment(len(limit_up_stocks))
        }

    def _judge_market_sentiment(self, limit_up_count):
        """判断市场情绪"""
        if limit_up_count >= 100:
            return '极度活跃'
        elif limit_up_count >= 50:
            return '活跃'
        elif limit_up_count >= 20:
            return '一般'
        else:
            return '低迷'


# 创建全局实例
limit_up_analyzer = LimitUpAnalyzer()