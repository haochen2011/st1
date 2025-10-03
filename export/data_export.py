#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据导出模块
支持多种格式的数据导出功能
"""

import pandas as pd
import numpy as np
import json
import os
import zipfile
from datetime import datetime, date, timedelta
from pathlib import Path
from loguru import logger
from data.database import db_manager
from data.enhanced_database import enhanced_db_manager
from core.config import config
from utils.stock_info import stock_info
from data.basic_data import basic_data
from data.tick_data import tick_data


class DataExporter:
    """数据导出器类"""

    def __init__(self):
        self.export_path = config.get_data_path('exports') if hasattr(config, 'get_data_path') else './exports'
        Path(self.export_path).mkdir(parents=True, exist_ok=True)

    def export_stock_list(self, format='excel', market='all'):
        """导出股票列表"""
        try:
            # 获取股票列表数据
            sql = "SELECT * FROM stock_info"
            params = {}

            if market != 'all':
                sql += " WHERE market = :market"
                params['market'] = market

            sql += " ORDER BY stock_code"

            stock_df = db_manager.query_to_dataframe(sql, params)

            if stock_df.empty:
                logger.warning("没有股票数据可导出")
                return None

            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"stock_list_{market}_{timestamp}"

            return self._export_dataframe(stock_df, filename, format)

        except Exception as e:
            logger.error(f"导出股票列表失败: {e}")
            return None

    def export_basic_data(self, stock_code, period='daily', start_date=None, end_date=None, format='excel'):
        """导出基础数据

        Args:
            stock_code: 股票代码
            period: 数据周期，默认'daily'
            start_date: 开始日期
            end_date: 结束日期
            format: 导出格式
        """
        try:
            # 如果没有指定结束日期，使用当前日期
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')

            # 如果没有指定开始日期，默认导出最近一年的数据
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

            # 获取基础数据
            basic_df = basic_data.get_basic_data_from_db(stock_code, period, start_date, end_date)

            if basic_df.empty:
                logger.warning(f"股票 {stock_code} 没有基础数据可导出")
                return None

            # 获取股票名称
            stock_name = self._get_stock_name(stock_code)

            # 生成文件名 - 根据周期类型生成不同的文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if period == 'daily':
                filename = f"basic_data_daily_{stock_name}_{stock_code}_{timestamp}"
            else:
                filename = f"basic_data_{period}_{stock_name}_{stock_code}_{timestamp}"

            return self._export_dataframe(basic_df, filename, format)

        except Exception as e:
            logger.error(f"导出基础数据失败: {e}")
            return None

    def export_tick_data(self, stock_code, trade_date=None, format='excel'):
        """导出分笔数据

        Args:
            stock_code: 股票代码
            trade_date: 交易日期，如果为None则导出最新一天的数据
            format: 导出格式
        """
        try:
            # 如果没有指定日期，获取最新的交易日期
            if trade_date is None:
                trade_date = self._get_latest_tick_date(stock_code)
                if not trade_date:
                    logger.warning(f"股票 {stock_code} 没有分笔数据")
                    return None

            # 标准化日期格式
            if isinstance(trade_date, str):
                if len(trade_date) == 8:  # YYYYMMDD
                    trade_date = datetime.strptime(trade_date, '%Y%m%d').date()
                elif len(trade_date) == 10:  # YYYY-MM-DD
                    trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            elif isinstance(trade_date, datetime):
                trade_date = trade_date.date()

            # 获取分笔数据
            tick_df = tick_data.get_tick_data_from_db(stock_code, trade_date, trade_date)

            if tick_df.empty:
                logger.warning(f"股票 {stock_code} {trade_date} 没有分笔数据可导出")
                return None

            # 获取股票名称
            stock_name = self._get_stock_name(stock_code)

            # 生成文件名
            date_str = trade_date.strftime('%Y%m%d')
            filename = f"tick_data_{date_str}_{stock_name}_{stock_code}"

            return self._export_dataframe(tick_df, filename, format)

        except Exception as e:
            logger.error(f"导出分笔数据失败: {e}")
            return None

    def export_indicator_data(self, stock_code, period='daily', indicators=None, start_date=None, end_date=None,
                              format='excel'):
        """导出技术指标数据"""
        try:
            sql = """
            SELECT stock_code, trade_date, indicator_name, indicator_value
            FROM indicator_data
            WHERE stock_code = :stock_code AND period_type = :period
            """
            params = {'stock_code': stock_code, 'period': period}

            if indicators:
                placeholders = ','.join([f':indicator_{i}' for i in range(len(indicators))])
                sql += f" AND indicator_name IN ({placeholders})"
                for i, indicator in enumerate(indicators):
                    params[f'indicator_{i}'] = indicator

            if start_date:
                sql += " AND trade_date >= :start_date"
                params['start_date'] = start_date

            if end_date:
                sql += " AND trade_date <= :end_date"
                params['end_date'] = end_date

            sql += " ORDER BY trade_date, indicator_name"

            indicator_df = db_manager.query_to_dataframe(sql, params)

            if indicator_df.empty:
                logger.warning(f"股票 {stock_code} 没有指标数据可导出")
                return None

            # 透视表格式
            pivot_df = indicator_df.pivot_table(
                index=['stock_code', 'trade_date'],
                columns='indicator_name',
                values='indicator_value'
            ).reset_index()

            # 获取股票名称
            stock_name = self._get_stock_name(stock_code)

            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            indicators_str = '_'.join(indicators) if indicators else 'all'
            filename = f"{stock_name}_{stock_code}_indicators_{indicators_str}_{timestamp}"

            return self._export_dataframe(pivot_df, filename, format)

        except Exception as e:
            logger.error(f"导出指标数据失败: {e}")
            return None

    def export_multiple_stocks(self, stock_codes, data_type='basic', period='daily', format='excel', zip_output=True):
        """批量导出多只股票数据"""
        try:
            exported_files = []

            for stock_code in stock_codes:
                try:
                    if data_type == 'basic':
                        filepath = self.export_basic_data(stock_code, period, format=format)
                    elif data_type == 'tick':
                        trade_date = datetime.now().strftime('%Y-%m-%d')
                        filepath = self.export_tick_data(stock_code, trade_date, format=format)
                    elif data_type == 'indicators':
                        filepath = self.export_indicator_data(stock_code, period, format=format)
                    else:
                        logger.warning(f"不支持的数据类型: {data_type}")
                        continue

                    if filepath:
                        exported_files.append(filepath)
                        logger.info(f"成功导出股票 {stock_code} 数据: {filepath}")

                except Exception as e:
                    logger.error(f"导出股票 {stock_code} 数据失败: {e}")
                    continue

            # 如果需要压缩
            if zip_output and exported_files:
                zip_filepath = self._create_zip_archive(exported_files, f"multiple_stocks_{data_type}")
                logger.info(f"批量导出完成，压缩文件: {zip_filepath}")
                return zip_filepath

            return exported_files

        except Exception as e:
            logger.error(f"批量导出失败: {e}")
            return []

    def export_custom_query(self, sql, params=None, filename=None, format='excel'):
        """导出自定义查询结果"""
        try:
            result_df = db_manager.query_to_dataframe(sql, params)

            if result_df.empty:
                logger.warning("查询结果为空，无数据可导出")
                return None

            if filename is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"custom_query_{timestamp}"

            return self._export_dataframe(result_df, filename, format)

        except Exception as e:
            logger.error(f"导出自定义查询失败: {e}")
            return None

    def export_statistical_report(self, stock_codes=None, period='daily', format='excel'):
        """导出统计报告"""
        try:
            # 获取股票列表
            if stock_codes is None:
                stock_list_df = db_manager.query_to_dataframe(
                    "SELECT DISTINCT stock_code FROM basic_data WHERE period_type = :period", {'period': period})
                stock_codes = stock_list_df['stock_code'].tolist()[:50]  # 限制为前50只股票

            reports = []

            for stock_code in stock_codes:
                try:
                    # 获取基础统计信息
                    sql = """
                    SELECT
                        stock_code,
                        COUNT(*) as total_records,
                        MIN(trade_date) as start_date,
                        MAX(trade_date) as end_date,
                        AVG(close_price) as avg_price,
                        MAX(high_price) as max_price,
                        MIN(low_price) as min_price,
                        SUM(volume) as total_volume,
                        SUM(amount) as total_amount,
                        AVG(change_pct) as avg_change_pct,
                        STDDEV(change_pct) as volatility
                    FROM basic_data
                    WHERE stock_code = :stock_code AND period_type = :period
                    GROUP BY stock_code
                    """

                    params = {'stock_code': stock_code, 'period': period}
                    stats_df = db_manager.query_to_dataframe(sql, params)

                    if not stats_df.empty:
                        reports.append(stats_df.iloc[0])

                except Exception as e:
                    logger.error(f"生成股票 {stock_code} 统计报告失败: {e}")
                    continue

            if reports:
                report_df = pd.DataFrame(reports)

                # 添加股票名称
                stock_names = {}
                for code in report_df['stock_code']:
                    stock_names[code] = self._get_stock_name(code)

                report_df['stock_name'] = report_df['stock_code'].map(stock_names)

                # 重新排列列顺序
                cols = ['stock_code', 'stock_name'] + [col for col in report_df.columns if
                                                       col not in ['stock_code', 'stock_name']]
                report_df = report_df[cols]

                # 生成文件名
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"statistical_report_{period}_{timestamp}"

                return self._export_dataframe(report_df, filename, format)
            else:
                logger.warning("没有统计数据可导出")
                return None

        except Exception as e:
            logger.error(f"导出统计报告失败: {e}")
            return None

    def _export_dataframe(self, df, filename, format):
        """导出DataFrame到指定格式"""
        try:
            if format.lower() == 'excel':
                filepath = os.path.join(self.export_path, f"{filename}.xlsx")
                df.to_excel(filepath, index=False)

            elif format.lower() == 'csv':
                filepath = os.path.join(self.export_path, f"{filename}.csv")
                df.to_csv(filepath, index=False, encoding='utf-8-sig')

            elif format.lower() == 'json':
                filepath = os.path.join(self.export_path, f"{filename}.json")
                # 处理日期和时间字段
                df_copy = df.copy()
                for col in df_copy.columns:
                    if df_copy[col].dtype == 'object':
                        df_copy[col] = df_copy[col].astype(str)

                df_copy.to_json(filepath, orient='records', date_format='iso', force_ascii=False, indent=2)

            elif format.lower() == 'parquet':
                filepath = os.path.join(self.export_path, f"{filename}.parquet")
                df.to_parquet(filepath, index=False)

            else:
                raise ValueError(f"不支持的导出格式: {format}")

            logger.info(f"数据导出成功: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"导出DataFrame失败: {e}")
            return None

    def _create_zip_archive(self, file_paths, archive_name):
        """创建ZIP压缩包"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            zip_filepath = os.path.join(self.export_path, f"{archive_name}_{timestamp}.zip")

            with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in file_paths:
                    if os.path.exists(file_path):
                        arcname = os.path.basename(file_path)
                        zipf.write(file_path, arcname)

                        # 删除原文件（可选）
                        # os.remove(file_path)

            logger.info(f"压缩包创建成功: {zip_filepath}")
            return zip_filepath

        except Exception as e:
            logger.error(f"创建压缩包失败: {e}")
            return None

    def _get_stock_name(self, stock_code):
        """获取股票名称"""
        try:
            info = stock_info.get_stock_info_from_db(stock_code)
            if not info.empty:
                return info.iloc[0]['stock_name']
            else:
                return stock_code
        except:
            return stock_code

    def _get_latest_tick_date(self, stock_code=None):
        """获取最新的分笔数据日期"""
        try:
            # 查找所有tick_data表
            tables_sql = "SHOW TABLES LIKE 'tick_data_%'"
            tables_result = db_manager.query_to_dataframe(tables_sql)

            if tables_result.empty:
                logger.warning("没有找到任何分笔数据表")
                return None

            # 找到最新的表
            table_names = []
            for _, row in tables_result.iterrows():
                table_name = list(row.values)[0]
                if 'tick_data_' in table_name:
                    table_names.append(table_name)

            if not table_names:
                return None

            # 按日期排序，获取最新的表
            table_names.sort(reverse=True)
            latest_table = table_names[0]

            # 从最新的表中查询最新日期
            if stock_code:
                sql = f"SELECT MAX(trade_date) as latest_date FROM {latest_table} WHERE stock_code = :stock_code"
                params = {'stock_code': stock_code}
            else:
                sql = f"SELECT MAX(trade_date) as latest_date FROM {latest_table}"
                params = None

            result = db_manager.query_to_dataframe(sql, params)
            if not result.empty and result.iloc[0]['latest_date'] is not None:
                latest_date = result.iloc[0]['latest_date']
                if isinstance(latest_date, str):
                    return datetime.strptime(latest_date, '%Y-%m-%d').date()
                return latest_date
            return None
        except Exception as e:
            logger.error(f"获取最新分笔数据日期失败: {e}")
            return None

    def _get_latest_basic_date(self, stock_code=None, period='daily'):
        """获取最新的基础数据日期"""
        try:
            if stock_code:
                sql = "SELECT MAX(trade_date) as latest_date FROM basic_data WHERE stock_code = :stock_code AND period = :period"
                params = {'stock_code': stock_code, 'period': period}
            else:
                sql = "SELECT MAX(trade_date) as latest_date FROM basic_data WHERE period = :period"
                params = {'period': period}

            result = enhanced_db_manager.safe_query_to_dataframe(sql, params, required_tables=['basic_data'])
            if not result.empty and result.iloc[0]['latest_date'] is not None:
                latest_date = result.iloc[0]['latest_date']
                if isinstance(latest_date, str):
                    return datetime.strptime(latest_date, '%Y-%m-%d').date()
                return latest_date
            return None
        except Exception as e:
            logger.error(f"获取最新基础数据日期失败: {e}")
            return None

    def get_available_tick_dates(self, stock_code):
        """获取股票可用的分笔数据日期列表"""
        try:
            sql = "SELECT DISTINCT trade_date FROM tick_data WHERE stock_code = :stock_code ORDER BY trade_date DESC"
            result = db_manager.safe_query_to_dataframe(sql, {'stock_code': stock_code}, required_tables=['tick_data'])
            if not result.empty:
                return result['trade_date'].tolist()
            return []
        except Exception as e:
            logger.error(f"获取可用分笔数据日期失败: {e}")
            return []

    def get_available_basic_periods(self, stock_code):
        """获取股票可用的基础数据周期列表"""
        try:
            sql = "SELECT DISTINCT period FROM basic_data WHERE stock_code = :stock_code ORDER BY period"
            result = db_manager.safe_query_to_dataframe(sql, {'stock_code': stock_code}, required_tables=['basic_data'])
            if not result.empty:
                return result['period'].tolist()
            return ['daily']  # 默认返回daily
        except Exception as e:
            logger.error(f"获取可用基础数据周期失败: {e}")
            return ['daily']

    def schedule_export(self, export_config):
        """定时导出任务配置"""
        try:
            # 这里可以集成定时任务库如APScheduler
            # 目前只是保存配置

            config_filepath = os.path.join(self.export_path, 'scheduled_exports.json')

            if os.path.exists(config_filepath):
                with open(config_filepath, 'r', encoding='utf-8') as f:
                    scheduled_exports = json.load(f)
            else:
                scheduled_exports = []

            # 添加新的导出配置
            export_config['created_at'] = datetime.now().isoformat()
            scheduled_exports.append(export_config)

            with open(config_filepath, 'w', encoding='utf-8') as f:
                json.dump(scheduled_exports, f, ensure_ascii=False, indent=2)

            logger.info(f"定时导出任务配置已保存: {export_config}")
            return True

        except Exception as e:
            logger.error(f"配置定时导出失败: {e}")
            return False

    def get_export_history(self):
        """获取导出历史"""
        try:
            export_files = []

            for file_path in Path(self.export_path).glob('*'):
                if file_path.is_file():
                    stat = file_path.stat()
                    export_files.append({
                        'filename': file_path.name,
                        'filepath': str(file_path),
                        'size': stat.st_size,
                        'created_time': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })

            # 按创建时间排序
            export_files.sort(key=lambda x: x['created_time'], reverse=True)

            return export_files

        except Exception as e:
            logger.error(f"获取导出历史失败: {e}")
            return []

    def cleanup_old_exports(self, days=30):
        """清理旧的导出文件"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            deleted_count = 0

            for file_path in Path(self.export_path).glob('*'):
                if file_path.is_file():
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime < cutoff_date:
                        file_path.unlink()
                        deleted_count += 1
                        logger.info(f"删除过期导出文件: {file_path}")

            logger.info(f"清理完成，删除了 {deleted_count} 个过期文件")
            return deleted_count

        except Exception as e:
            logger.error(f"清理导出文件失败: {e}")
            return 0


# 创建全局实例
data_exporter = DataExporter()
