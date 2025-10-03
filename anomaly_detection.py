"""
异动检测模块
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from data_fetcher import data_fetcher


class AnomalyDetector:
    """异动检测器"""

    def __init__(self):
        self.data_fetcher = data_fetcher

    def monitor_stock_list(self, stock_codes):
        """监控股票列表的异动"""
        try:
            anomaly_results = {
                'monitor_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_monitored': len(stock_codes),
                'anomalies': [],
                'summary': {
                    'price_anomalies': 0,
                    'volume_anomalies': 0,
                    'turnover_anomalies': 0
                }
            }

            for stock_code in stock_codes:
                try:
                    # 获取实时数据
                    realtime_data = self.data_fetcher.get_realtime_data([stock_code])

                    if realtime_data.empty:
                        continue

                    # 获取历史数据用于对比
                    end_date = datetime.now().strftime('%Y-%m-%d')
                    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                    historical_data = self.data_fetcher.get_historical_data(stock_code, start_date, end_date)

                    if historical_data.empty:
                        continue

                    # 检测各种异动
                    price_anomalies = self.detect_price_anomaly(historical_data)
                    volume_anomalies = self.detect_volume_anomaly(historical_data)
                    turnover_anomalies = self.detect_turnover_anomaly(historical_data)

                    all_anomalies = price_anomalies + volume_anomalies + turnover_anomalies

                    if all_anomalies:
                        anomaly_results['anomalies'].extend([
                            {
                                'stock_code': stock_code,
                                'anomaly': anomaly
                            }
                            for anomaly in all_anomalies
                        ])

                        # 更新统计
                        anomaly_results['summary']['price_anomalies'] += len(price_anomalies)
                        anomaly_results['summary']['volume_anomalies'] += len(volume_anomalies)
                        anomaly_results['summary']['turnover_anomalies'] += len(turnover_anomalies)

                except Exception as e:
                    logger.warning(f"监控股票 {stock_code} 异动失败: {e}")
                    continue

            logger.info(f"异动监控完成，共发现 {len(anomaly_results['anomalies'])} 个异动")
            return anomaly_results

        except Exception as e:
            logger.error(f"异动监控失败: {e}")
            return {
                'error': str(e),
                'anomalies': [],
                'total_monitored': 0
            }

    def detect_price_anomaly(self, data):
        """检测价格异动"""
        if data.empty or len(data) < 5:
            return []

        anomalies = []
        latest = data.iloc[-1]

        # 涨跌幅异动
        change_pct = latest.get('change_pct', 0)
        if abs(change_pct) > 7:  # 涨跌幅超过7%
            anomalies.append({
                'type': 'price_change',
                'description': f"价格异动：{'上涨' if change_pct > 0 else '下跌'}{abs(change_pct):.2f}%",
                'value': change_pct,
                'severity': 'high' if abs(change_pct) > 9 else 'medium'
            })

        # 价格突破异动
        if 'ma_20' in latest:
            ma20 = latest.get('ma_20', 0)
            current_price = latest.get('close_price', 0)
            if ma20 > 0:
                deviation = (current_price - ma20) / ma20 * 100
                if abs(deviation) > 10:  # 偏离20日均线超过10%
                    anomalies.append({
                        'type': 'price_deviation',
                        'description': f"价格偏离20日均线{abs(deviation):.2f}%",
                        'value': deviation,
                        'severity': 'medium'
                    })

        return anomalies

    def detect_volume_anomaly(self, data):
        """检测成交量异动"""
        if data.empty or len(data) < 10:
            return []

        anomalies = []
        latest = data.iloc[-1]

        # 成交量异动
        recent_avg_volume = data['volume'].tail(5).mean()
        historical_avg_volume = data['volume'].tail(20).mean()

        if historical_avg_volume > 0:
            volume_ratio = recent_avg_volume / historical_avg_volume
            if volume_ratio > 2:  # 成交量放大2倍以上
                anomalies.append({
                    'type': 'volume_surge',
                    'description': f"成交量异动：较历史平均放大{volume_ratio:.2f}倍",
                    'value': volume_ratio,
                    'severity': 'high' if volume_ratio > 5 else 'medium'
                })

        return anomalies

    def detect_turnover_anomaly(self, data):
        """检测换手率异动"""
        if data.empty:
            return []

        anomalies = []
        latest = data.iloc[-1]

        # 换手率异动
        turnover_rate = latest.get('turnover_rate', 0)
        if turnover_rate > 15:  # 换手率超过15%
            anomalies.append({
                'type': 'high_turnover',
                'description': f"换手率异动：{turnover_rate:.2f}%",
                'value': turnover_rate,
                'severity': 'high' if turnover_rate > 25 else 'medium'
            })

        return anomalies


# 创建全局实例
anomaly_detector = AnomalyDetector()