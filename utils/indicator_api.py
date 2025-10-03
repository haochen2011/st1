#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指标API模块
提供技术指标相关的API接口
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, date
from loguru import logger
from processors.indicator_processor import indicator_processor
from data.database import db_manager

indicator_api = Blueprint('indicator_api', __name__, url_prefix='/api/indicator')


@indicator_api.route('/calculate/<string:stock_code>', methods=['GET'])
def calculate_indicators(stock_code):
    """计算技术指标"""
    try:
        period = request.args.get('period', 'daily')
        indicators = request.args.get('indicators', 'MACD,RSI,KDJ').split(',')

        # 获取基础数据
        sql = """
        SELECT * FROM basic_data 
        WHERE stock_code = :stock_code AND period_type = :period 
        ORDER BY trade_date
        """
        params = {'stock_code': stock_code, 'period': period}

        basic_data = db_manager.query_to_dataframe(sql, params)

        if basic_data.empty:
            return jsonify({'error': '无基础数据'}), 404

        # 计算指标
        result_data = basic_data.copy()

        if 'MACD' in indicators:
            result_data = indicator_processor.calculate_macd(result_data)
        if 'RSI' in indicators:
            result_data = indicator_processor.calculate_rsi(result_data)
        if 'KDJ' in indicators:
            result_data = indicator_processor.calculate_kdj(result_data)

        # 转换为JSON格式
        result_data['trade_date'] = result_data['trade_date'].astype(str)

        return jsonify({
            'stock_code': stock_code,
            'period': period,
            'indicators': indicators,
            'data': result_data.to_dict('records')
        })

    except Exception as e:
        logger.error(f"计算指标失败: {e}")
        return jsonify({'error': str(e)}), 500


@indicator_api.route('/get/<string:stock_code>/<string:indicator_name>', methods=['GET'])
def get_indicator_data(stock_code, indicator_name):
    """获取指标数据"""
    try:
        period = request.args.get('period', 'daily')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        sql = """
        SELECT * FROM indicator_data 
        WHERE stock_code = :stock_code AND indicator_name = :indicator_name AND period_type = :period
        """
        params = {
            'stock_code': stock_code,
            'indicator_name': indicator_name,
            'period': period
        }

        if start_date:
            sql += " AND trade_date >= :start_date"
            params['start_date'] = start_date

        if end_date:
            sql += " AND trade_date <= :end_date"
            params['end_date'] = end_date

        sql += " ORDER BY trade_date"

        indicator_data = db_manager.query_to_dataframe(sql, params)

        if indicator_data.empty:
            return jsonify({'error': '无指标数据'}), 404

        # 转换为JSON格式
        indicator_data['trade_date'] = indicator_data['trade_date'].astype(str)

        return jsonify({
            'stock_code': stock_code,
            'indicator_name': indicator_name,
            'period': period,
            'data': indicator_data.to_dict('records')
        })

    except Exception as e:
        logger.error(f"获取指标数据失败: {e}")
        return jsonify({'error': str(e)}), 500