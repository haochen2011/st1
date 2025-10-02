#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
总API模块
提供所有股票数据相关的API接口
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, date
from loguru import logger
from database import db_manager
from stock_info import stock_info
from tick_data import tick_data
from basic_data import basic_data
from indicator_processor import indicator_processor

main_api = Blueprint('main_api', __name__, url_prefix='/api')


@main_api.route('/stock/list', methods=['GET'])
def get_stock_list():
    """获取股票列表"""
    try:
        market = request.args.get('market', 'all')

        sql = "SELECT * FROM stock_info"
        params = {}

        if market != 'all':
            sql += " WHERE market = :market"
            params['market'] = market

        sql += " ORDER BY stock_code"

        stocks = db_manager.query_to_dataframe(sql, params)

        if stocks.empty:
            return jsonify({'error': '无股票数据'}), 404

        # 转换日期格式
        stocks['list_date'] = stocks['list_date'].astype(str)

        return jsonify({
            'market': market,
            'count': len(stocks),
            'data': stocks.to_dict('records')
        })

    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return jsonify({'error': str(e)}), 500


@main_api.route('/stock/info/<string:stock_code>', methods=['GET'])
def get_stock_info(stock_code):
    """获取股票基本信息"""
    try:
        stock_data = stock_info.get_stock_info_from_db(stock_code)

        if stock_data.empty:
            return jsonify({'error': '股票不存在'}), 404

        stock_record = stock_data.iloc[0].to_dict()

        # 转换日期格式
        if stock_record.get('list_date'):
            stock_record['list_date'] = str(stock_record['list_date'])

        return jsonify(stock_record)

    except Exception as e:
        logger.error(f"获取股票信息失败: {e}")
        return jsonify({'error': str(e)}), 500


@main_api.route('/data/tick/<string:stock_code>', methods=['GET'])
def get_tick_data_api(stock_code):
    """获取分笔数据"""
    try:
        trade_date = request.args.get('trade_date', datetime.now().strftime('%Y-%m-%d'))

        tick_df = tick_data.get_tick_data_from_db(stock_code, trade_date, trade_date)

        if tick_df.empty:
            return jsonify({'error': '无分笔数据'}), 404

        # 转换时间格式
        tick_df['trade_time'] = tick_df['trade_time'].astype(str)
        tick_df['trade_date'] = tick_df['trade_date'].astype(str)

        return jsonify({
            'stock_code': stock_code,
            'trade_date': trade_date,
            'count': len(tick_df),
            'data': tick_df.to_dict('records')
        })

    except Exception as e:
        logger.error(f"获取分笔数据失败: {e}")
        return jsonify({'error': str(e)}), 500


@main_api.route('/data/basic/<string:stock_code>', methods=['GET'])
def get_basic_data_api(stock_code):
    """获取基础数据"""
    try:
        period = request.args.get('period', 'daily')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        basic_df = basic_data.get_basic_data_from_db(stock_code, period, start_date, end_date)

        if basic_df.empty:
            return jsonify({'error': '无基础数据'}), 404

        # 转换日期格式
        basic_df['trade_date'] = basic_df['trade_date'].astype(str)

        return jsonify({
            'stock_code': stock_code,
            'period': period,
            'count': len(basic_df),
            'data': basic_df.to_dict('records')
        })

    except Exception as e:
        logger.error(f"获取基础数据失败: {e}")
        return jsonify({'error': str(e)}), 500


@main_api.route('/data/update/<string:stock_code>', methods=['POST'])
def update_stock_data(stock_code):
    """更新股票数据"""
    try:
        data_type = request.json.get('data_type', 'basic')  # basic, tick, all
        force_update = request.json.get('force_update', False)

        result = {}

        if data_type in ['basic', 'all']:
            # 更新基础数据
            updated_basic = basic_data.update_basic_data(stock_code, force_update=force_update)
            result['basic_data'] = len(updated_basic)

        if data_type in ['tick', 'all']:
            # 更新分笔数据
            tick_result = tick_data.download_and_save_tick_data(stock_code)
            result['tick_data'] = tick_result['data_count'] if tick_result else 0

        return jsonify({
            'stock_code': stock_code,
            'update_result': result,
            'message': '数据更新成功'
        })

    except Exception as e:
        logger.error(f"更新股票数据失败: {e}")
        return jsonify({'error': str(e)}), 500


@main_api.route('/analysis/statistics/<string:stock_code>', methods=['GET'])
def get_statistics(stock_code):
    """获取股票统计信息"""
    try:
        period = request.args.get('period', 'daily')

        # 获取基础数据
        basic_df = basic_data.get_basic_data_from_db(stock_code, period)

        if basic_df.empty:
            return jsonify({'error': '无数据进行统计'}), 404

        # 计算统计信息
        stats = {
            'stock_code': stock_code,
            'period': period,
            'data_count': len(basic_df),
            'date_range': {
                'start': str(basic_df['trade_date'].min()),
                'end': str(basic_df['trade_date'].max())
            },
            'price_stats': {
                'current_price': float(basic_df['close_price'].iloc[-1]),
                'max_price': float(basic_df['high_price'].max()),
                'min_price': float(basic_df['low_price'].min()),
                'avg_price': float(basic_df['close_price'].mean()),
                'price_std': float(basic_df['close_price'].std())
            },
            'volume_stats': {
                'total_volume': int(basic_df['volume'].sum()),
                'avg_volume': float(basic_df['volume'].mean()),
                'max_volume': int(basic_df['volume'].max()),
                'min_volume': int(basic_df['volume'].min())
            },
            'change_stats': {
                'max_change_pct': float(basic_df['change_pct'].max()),
                'min_change_pct': float(basic_df['change_pct'].min()),
                'avg_change_pct': float(basic_df['change_pct'].mean()),
                'positive_days': int((basic_df['change_pct'] > 0).sum()),
                'negative_days': int((basic_df['change_pct'] < 0).sum())
            }
        }

        return jsonify(stats)

    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")
        return jsonify({'error': str(e)}), 500


@main_api.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    try:
        # 测试数据库连接
        result = db_manager.execute_sql("SELECT 1 as test")

        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected'
        })

    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 500