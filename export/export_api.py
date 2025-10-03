#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据导出API模块
提供数据导出相关的API接口
"""

from flask import Blueprint, request, jsonify, send_file
from datetime import datetime, date
import os
from loguru import logger
from export.data_export import data_exporter

export_api = Blueprint('export_api', __name__, url_prefix='/api/export')


@export_api.route('/stock_list', methods=['GET'])
def export_stock_list():
    """导出股票列表"""
    try:
        format = request.args.get('format', 'excel')
        market = request.args.get('market', 'all')

        filepath = data_exporter.export_stock_list(format=format, market=market)

        if filepath:
            return jsonify({
                'success': True,
                'message': '股票列表导出成功',
                'filepath': filepath,
                'filename': os.path.basename(filepath)
            })
        else:
            return jsonify({'error': '导出失败'}), 500

    except Exception as e:
        logger.error(f"导出股票列表API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/basic_data/<string:stock_code>', methods=['GET'])
def export_basic_data(stock_code):
    """导出基础数据"""
    try:
        period = request.args.get('period', 'daily')  # 默认daily
        format = request.args.get('format', 'excel')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        filepath = data_exporter.export_basic_data(
            stock_code=stock_code,
            period=period,
            start_date=start_date,
            end_date=end_date,
            format=format
        )

        if filepath:
            return jsonify({
                'success': True,
                'message': f'股票 {stock_code} {period} 基础数据导出成功',
                'filepath': filepath,
                'filename': os.path.basename(filepath),
                'period': period,
                'date_range': f"{start_date or '默认开始日期'} 至 {end_date or '默认结束日期'}"
            })
        else:
            return jsonify({'error': '导出失败'}), 500

    except Exception as e:
        logger.error(f"导出基础数据API失败: {e}")
        return jsonify({'error': str(e)}), 500

@export_api.route('/basic_data/<string:stock_code>/periods', methods=['GET'])
def get_available_basic_periods(stock_code):
    """获取股票可用的基础数据周期"""
    try:
        periods = data_exporter.get_available_basic_periods(stock_code)
        return jsonify({
            'success': True,
            'stock_code': stock_code,
            'available_periods': periods,
            'default_period': 'daily'
        })
    except Exception as e:
        logger.error(f"获取可用基础数据周期失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/tick_data/<string:stock_code>', methods=['GET'])
def export_tick_data(stock_code):
    """导出分笔数据"""
    try:
        trade_date = request.args.get('trade_date')  # 如果不提供，将使用最新日期
        format = request.args.get('format', 'excel')

        filepath = data_exporter.export_tick_data(
            stock_code=stock_code,
            trade_date=trade_date,
            format=format
        )

        if filepath:
            actual_date = trade_date if trade_date else "最新日期"
            return jsonify({
                'success': True,
                'message': f'股票 {stock_code} {actual_date} 分笔数据导出成功',
                'filepath': filepath,
                'filename': os.path.basename(filepath)
            })
        else:
            return jsonify({'error': '导出失败'}), 500

    except Exception as e:
        logger.error(f"导出分笔数据API失败: {e}")
        return jsonify({'error': str(e)}), 500

@export_api.route('/tick_data/<string:stock_code>/dates', methods=['GET'])
def get_available_tick_dates(stock_code):
    """获取股票可用的分笔数据日期"""
    try:
        dates = data_exporter.get_available_tick_dates(stock_code)
        return jsonify({
            'success': True,
            'stock_code': stock_code,
            'available_dates': dates,
            'total_count': len(dates)
        })
    except Exception as e:
        logger.error(f"获取可用分笔数据日期失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/indicator_data/<string:stock_code>', methods=['GET'])
def export_indicator_data(stock_code):
    """导出指标数据"""
    try:
        period = request.args.get('period', 'daily')
        format = request.args.get('format', 'excel')
        indicators = request.args.get('indicators')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        if indicators:
            indicators = indicators.split(',')

        filepath = data_exporter.export_indicator_data(
            stock_code=stock_code,
            period=period,
            indicators=indicators,
            start_date=start_date,
            end_date=end_date,
            format=format
        )

        if filepath:
            return jsonify({
                'success': True,
                'message': f'股票 {stock_code} 指标数据导出成功',
                'filepath': filepath,
                'filename': os.path.basename(filepath)
            })
        else:
            return jsonify({'error': '导出失败'}), 500

    except Exception as e:
        logger.error(f"导出指标数据API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/batch', methods=['POST'])
def export_batch():
    """批量导出多只股票数据"""
    try:
        data = request.json
        stock_codes = data.get('stock_codes', [])
        data_type = data.get('data_type', 'basic')
        period = data.get('period', 'daily')
        format = data.get('format', 'excel')
        zip_output = data.get('zip_output', True)

        if not stock_codes:
            return jsonify({'error': '缺少股票代码列表'}), 400

        result = data_exporter.export_multiple_stocks(
            stock_codes=stock_codes,
            data_type=data_type,
            period=period,
            format=format,
            zip_output=zip_output
        )

        if result:
            if isinstance(result, str):  # ZIP文件路径
                return jsonify({
                    'success': True,
                    'message': f'批量导出成功，共 {len(stock_codes)} 只股票',
                    'filepath': result,
                    'filename': os.path.basename(result),
                    'type': 'zip'
                })
            else:  # 文件列表
                return jsonify({
                    'success': True,
                    'message': f'批量导出成功，共 {len(result)} 个文件',
                    'files': [{'filepath': fp, 'filename': os.path.basename(fp)} for fp in result],
                    'type': 'files'
                })
        else:
            return jsonify({'error': '批量导出失败'}), 500

    except Exception as e:
        logger.error(f"批量导出API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/custom_query', methods=['POST'])
def export_custom_query():
    """导出自定义查询结果"""
    try:
        data = request.json
        sql = data.get('sql')
        params = data.get('params')
        filename = data.get('filename')
        format = data.get('format', 'excel')

        if not sql:
            return jsonify({'error': '缺少SQL查询语句'}), 400

        filepath = data_exporter.export_custom_query(
            sql=sql,
            params=params,
            filename=filename,
            format=format
        )

        if filepath:
            return jsonify({
                'success': True,
                'message': '自定义查询结果导出成功',
                'filepath': filepath,
                'filename': os.path.basename(filepath)
            })
        else:
            return jsonify({'error': '导出失败'}), 500

    except Exception as e:
        logger.error(f"导出自定义查询API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/statistical_report', methods=['GET'])
def export_statistical_report():
    """导出统计报告"""
    try:
        stock_codes = request.args.get('stock_codes')
        period = request.args.get('period', 'daily')
        format = request.args.get('format', 'excel')

        if stock_codes:
            stock_codes = stock_codes.split(',')

        filepath = data_exporter.export_statistical_report(
            stock_codes=stock_codes,
            period=period,
            format=format
        )

        if filepath:
            return jsonify({
                'success': True,
                'message': '统计报告导出成功',
                'filepath': filepath,
                'filename': os.path.basename(filepath)
            })
        else:
            return jsonify({'error': '导出失败'}), 500

    except Exception as e:
        logger.error(f"导出统计报告API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/download/<path:filename>', methods=['GET'])
def download_file(filename):
    """下载导出的文件"""
    try:
        filepath = os.path.join(data_exporter.export_path, filename)

        if not os.path.exists(filepath):
            return jsonify({'error': '文件不存在'}), 404

        return send_file(filepath, as_attachment=True, download_name=filename)

    except Exception as e:
        logger.error(f"下载文件API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/history', methods=['GET'])
def get_export_history():
    """获取导出历史"""
    try:
        history = data_exporter.get_export_history()

        return jsonify({
            'success': True,
            'count': len(history),
            'files': history
        })

    except Exception as e:
        logger.error(f"获取导出历史API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/cleanup', methods=['POST'])
def cleanup_old_exports():
    """清理旧的导出文件"""
    try:
        days = request.json.get('days', 30)
        deleted_count = data_exporter.cleanup_old_exports(days)

        return jsonify({
            'success': True,
            'message': f'清理完成，删除了 {deleted_count} 个过期文件',
            'deleted_count': deleted_count
        })

    except Exception as e:
        logger.error(f"清理导出文件API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/schedule', methods=['POST'])
def schedule_export():
    """配置定时导出任务"""
    try:
        export_config = request.json

        # 验证必要字段
        required_fields = ['name', 'schedule', 'export_type']
        for field in required_fields:
            if field not in export_config:
                return jsonify({'error': f'缺少必要字段: {field}'}), 400

        success = data_exporter.schedule_export(export_config)

        if success:
            return jsonify({
                'success': True,
                'message': '定时导出任务配置成功',
                'config': export_config
            })
        else:
            return jsonify({'error': '配置定时导出任务失败'}), 500

    except Exception as e:
        logger.error(f"配置定时导出API失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/formats', methods=['GET'])
def get_supported_formats():
    """获取支持的导出格式"""
    return jsonify({
        'success': True,
        'formats': [
            {
                'name': 'excel',
                'description': 'Excel文件格式',
                'extension': '.xlsx',
                'mime_type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            },
            {
                'name': 'csv',
                'description': 'CSV文件格式',
                'extension': '.csv',
                'mime_type': 'text/csv'
            },
            {
                'name': 'json',
                'description': 'JSON文件格式',
                'extension': '.json',
                'mime_type': 'application/json'
            },
            {
                'name': 'parquet',
                'description': 'Parquet文件格式',
                'extension': '.parquet',
                'mime_type': 'application/octet-stream'
            }
        ]
    })


@export_api.route('/data_types', methods=['GET'])
def get_supported_data_types():
    """获取支持的数据类型"""
    return jsonify({
        'success': True,
        'data_types': [
            {
                'name': 'basic',
                'description': '基础OHLCV数据',
                'periods': ['1min', '5min', '15min', '30min', '1hour', 'daily', 'week', 'month']
            },
            {
                'name': 'tick',
                'description': '分笔交易数据',
                'periods': ['tick']
            },
            {
                'name': 'indicators',
                'description': '技术指标数据',
                'indicators': ['MACD', 'RSI', 'KDJ', 'BOLL', 'MA']
            }
        ]
    })


@export_api.route('/latest_data_info', methods=['GET'])
def get_latest_data_info():
    """获取最新数据信息"""
    try:
        stock_code = request.args.get('stock_code')

        # 获取最新分笔数据日期
        latest_tick_date = data_exporter._get_latest_tick_date(stock_code)

        # 获取最新基础数据日期
        latest_basic_date = data_exporter._get_latest_basic_date(stock_code, 'daily')

        return jsonify({
            'success': True,
            'stock_code': stock_code,
            'latest_tick_date': latest_tick_date.strftime('%Y-%m-%d') if latest_tick_date else None,
            'latest_basic_date': latest_basic_date.strftime('%Y-%m-%d') if latest_basic_date else None,
            'message': '已获取最新数据信息'
        })
    except Exception as e:
        logger.error(f"获取最新数据信息失败: {e}")
        return jsonify({'error': str(e)}), 500


@export_api.route('/quick_export/<string:stock_code>', methods=['GET'])
def quick_export_latest(stock_code):
    """快速导出股票最新数据"""
    try:
        data_type = request.args.get('data_type', 'basic')  # basic 或 tick
        format = request.args.get('format', 'excel')

        if data_type == 'tick':
            # 导出最新分笔数据
            filepath = data_exporter.export_tick_data(
                stock_code=stock_code,
                trade_date=None,  # 自动使用最新日期
                format=format
            )
        else:
            # 导出基础数据（默认daily，最近一年）
            filepath = data_exporter.export_basic_data(
                stock_code=stock_code,
                period='daily',
                format=format
            )

        if filepath:
            return jsonify({
                'success': True,
                'message': f'股票 {stock_code} 最新{data_type}数据导出成功',
                'filepath': filepath,
                'filename': os.path.basename(filepath),
                'data_type': data_type
            })
        else:
            return jsonify({'error': '导出失败，可能没有相关数据'}), 500

    except Exception as e:
        logger.error(f"快速导出失败: {e}")
        return jsonify({'error': str(e)}), 500
