"""
Flask应用主文件 - A股股票分析系统API
优化版本：支持超时机制和多数据源自动切换
"""
from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_cors import CORS
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List
import asyncio
from app_config import Config
from models import init_database, get_db_session, SessionLocal
from data_fetcher import data_fetcher
from resonance_analysis import resonance_analyzer
from limit_up_analysis import limit_up_analyzer
from anomaly_detection import anomaly_detector
from channel_analysis import channel_analyzer
from technical_indicators import technical_analyzer
from loguru import logger

# 初始化Flask应用
app = Flask(__name__)
CORS(app)

# 配置
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['JSON_AS_ASCII'] = False

# 初始化数据库
init_database()


@app.route('/')
def index():
    """主页"""
    return jsonify({
        'message': 'A股股票分析系统API',
        'version': '2.0',
        'features': '支持超时机制和多数据源自动切换',
        'status': 'running'
    })


@app.route('/api/stock/list')
def get_stock_list():
    """获取股票列表"""
    try:
        stock_list = data_fetcher.get_stock_list()

        if stock_list.empty:
            return jsonify({
                'code': 404,
                'message': '获取股票列表失败',
                'data': []
            })

        # 转换为字典列表
        stocks = stock_list.to_dict('records')

        return jsonify({
            'code': 200,
            'message': '获取成功',
            'data': stocks[:100],  # 限制返回数量
            'total': len(stocks)
        })

    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'服务器错误: {str(e)}',
            'data': []
        })


@app.route('/api/stock/<stock_code>/realtime')
def get_realtime_data(stock_code):
    """获取实时行情数据"""
    try:
        realtime_data = data_fetcher.get_realtime_data([stock_code])

        if realtime_data.empty:
            return jsonify({
                'code': 404,
                'message': '获取实时数据失败',
                'data': {}
            })

        stock_data = realtime_data.iloc[0].to_dict()

        return jsonify({
            'code': 200,
            'message': '获取成功',
            'data': stock_data
        })

    except Exception as e:
        logger.error(f"获取{stock_code}实时数据失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'服务器错误: {str(e)}',
            'data': {}
        })


@app.route('/api/stock/<stock_code>/history')
def get_historical_data(stock_code):
    """获取历史行情数据"""
    try:
        # 获取查询参数
        start_date = request.args.get('start_date',
                                      (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d'))
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))

        historical_data = data_fetcher.get_historical_data(stock_code, start_date, end_date)

        if historical_data.empty:
            return jsonify({
                'code': 404,
                'message': '获取历史数据失败',
                'data': []
            })

        # 添加技术指标
        data_with_indicators = technical_analyzer.calculate_all_indicators(historical_data)

        # 转换为前端需要的格式
        chart_data = []
        for _, row in data_with_indicators.iterrows():
            chart_data.append({
                'date': row['trade_date'].strftime('%Y-%m-%d'),
                'open': row['open_price'],
                'high': row['high_price'],
                'low': row['low_price'],
                'close': row['close_price'],
                'volume': row['volume'],
                'ma5': row.get('ma_5'),
                'ma10': row.get('ma_10'),
                'ma20': row.get('ma_20'),
                'rsi': row.get('rsi'),
                'macd_dif': row.get('macd_dif'),
                'macd_dea': row.get('macd_dea'),
                'highlight': row.get('highlight_candle', False)  # 黄色蜡烛图标记
            })

        return jsonify({
            'code': 200,
            'message': '获取成功',
            'data': chart_data
        })

    except Exception as e:
        logger.error(f"获取{stock_code}历史数据失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'服务器错误: {str(e)}',
            'data': []
        })


@app.route('/api/analysis/resonance/<stock_code>')
def get_resonance_analysis(stock_code):
    """获取三层共振分析"""
    try:
        analysis_result = resonance_analyzer.perform_full_analysis(stock_code)

        return jsonify({
            'code': 200,
            'message': '分析完成',
            'data': analysis_result
        })

    except Exception as e:
        logger.error(f"三层共振分析失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'分析失败: {str(e)}',
            'data': {}
        })


@app.route('/api/analysis/limit-up')
def get_limit_up_analysis():
    """获取涨停板分析"""
    try:
        trade_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        limit_up_report = limit_up_analyzer.generate_limit_up_report(trade_date)

        return jsonify({
            'code': 200,
            'message': '分析完成',
            'data': limit_up_report
        })

    except Exception as e:
        logger.error(f"涨停板分析失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'分析失败: {str(e)}',
            'data': {}
        })


@app.route('/api/analysis/channel/<stock_code>')
def get_channel_analysis(stock_code):
    """获取多空通道分析"""
    try:
        # 获取历史数据
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        stock_data = data_fetcher.get_historical_data(stock_code, start_date, end_date)

        if stock_data.empty:
            return jsonify({
                'code': 404,
                'message': '无法获取股票数据',
                'data': {}
            })

        # 执行通道分析
        channel_result = channel_analyzer.perform_full_channel_analysis(stock_data)

        return jsonify({
            'code': 200,
            'message': '分析完成',
            'data': channel_result
        })

    except Exception as e:
        logger.error(f"通道分析失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'分析失败: {str(e)}',
            'data': {}
        })


@app.route('/api/monitor/anomaly')
def get_anomaly_monitoring():
    """获取异动监测"""
    try:
        # 获取监控的股票列表（可以从数据库或配置文件读取）
        watch_list = request.args.get('codes', '').split(',')
        if not watch_list or watch_list == ['']:
            # 默认监控一些热门股票
            watch_list = ['000001', '000002', '600000', '600036', '000858']

        anomaly_result = anomaly_detector.monitor_stock_list(watch_list)

        return jsonify({
            'code': 200,
            'message': '监测完成',
            'data': anomaly_result
        })

    except Exception as e:
        logger.error(f"异动监测失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'监测失败: {str(e)}',
            'data': {}
        })


@app.route('/api/market/index')
def get_market_index():
    """获取大盘指数数据"""
    try:
        market_data = data_fetcher.get_market_index_data()

        return jsonify({
            'code': 200,
            'message': '获取成功',
            'data': market_data
        })

    except Exception as e:
        logger.error(f"获取大盘数据失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'获取失败: {str(e)}',
            'data': {}
        })


@app.route('/api/market/sectors')
def get_sector_data():
    """获取板块数据"""
    try:
        sector_data = data_fetcher.get_sector_data()

        if sector_data.empty:
            return jsonify({
                'code': 404,
                'message': '获取板块数据失败',
                'data': []
            })

        sectors = sector_data.to_dict('records')

        return jsonify({
            'code': 200,
            'message': '获取成功',
            'data': sectors
        })

    except Exception as e:
        logger.error(f"获取板块数据失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'获取失败: {str(e)}',
            'data': []
        })


@app.route('/api/analysis/comprehensive/<stock_code>')
def get_comprehensive_analysis(stock_code):
    """获取综合分析（包含所有分析模块）"""
    try:
        # 获取历史数据
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        stock_data = data_fetcher.get_historical_data(stock_code, start_date, end_date)

        if stock_data.empty:
            return jsonify({
                'code': 404,
                'message': '无法获取股票数据',
                'data': {}
            })

        # 执行综合分析
        result = {}

        # 三层共振分析
        result['resonance'] = resonance_analyzer.perform_full_analysis(stock_code)

        # 通道分析
        result['channel'] = channel_analyzer.perform_full_channel_analysis(stock_data)

        # 技术指标分析
        data_with_indicators = technical_analyzer.calculate_all_indicators(stock_data)
        if not data_with_indicators.empty:
            latest_indicators = data_with_indicators.iloc[-1].to_dict()
            result['technical'] = {
                'indicators': latest_indicators,
                'signals': technical_analyzer.generate_trading_signals(data_with_indicators)
            }

        # 异动检测
        anomalies = anomaly_detector.detect_price_anomaly(stock_data)
        anomalies.extend(anomaly_detector.detect_volume_anomaly(stock_data))
        anomalies.extend(anomaly_detector.detect_turnover_anomaly(stock_data))
        result['anomalies'] = anomalies

        return jsonify({
            'code': 200,
            'message': '综合分析完成',
            'data': result
        })

    except Exception as e:
        logger.error(f"综合分析失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'分析失败: {str(e)}',
            'data': {}
        })


@app.route('/api/search')
def search_stocks():
    """搜索股票"""
    try:
        query = request.args.get('q', '').strip()
        if not query:
            return jsonify({
                'code': 400,
                'message': '搜索关键字不能为空',
                'data': []
            })

        # 获取股票列表
        stock_list = data_fetcher.get_stock_list()

        if stock_list.empty:
            return jsonify({
                'code': 404,
                'message': '无股票数据',
                'data': []
            })

        # 搜索匹配的股票
        mask = (stock_list['code'].str.contains(query, case=False, na=False) |
                stock_list['name'].str.contains(query, case=False, na=False))

        results = stock_list[mask].head(20).to_dict('records')

        return jsonify({
            'code': 200,
            'message': '搜索完成',
            'data': results
        })

    except Exception as e:
        logger.error(f"搜索股票失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'搜索失败: {str(e)}',
            'data': []
        })


@app.route('/api/config')
def get_config():
    """获取系统配置"""
    try:
        config_data = {
            'update_interval': Config.UPDATE_INTERVAL_MINUTES,
            'market_open': Config.is_market_open(),
            'data_sources': Config.DATA_SOURCES,
            'thresholds': {
                'price_change': Config.PRICE_CHANGE_THRESHOLD,
                'volume_ratio': Config.VOLUME_RATIO_THRESHOLD,
                'turnover': Config.TURNOVER_THRESHOLD
            },
            'timeout_seconds': Config.TIMEOUT_SECONDS,
            'max_retries': Config.MAX_RETRIES
        }

        return jsonify({
            'code': 200,
            'message': '获取成功',
            'data': config_data
        })

    except Exception as e:
        logger.error(f"获取配置失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'获取失败: {str(e)}',
            'data': {}
        })


@app.route('/api/health')
def health_check():
    """健康检查"""
    try:
        # 测试数据获取功能
        test_result = data_fetcher.get_stock_list()
        data_available = not test_result.empty

        return jsonify({
            'code': 200,
            'message': '系统正常',
            'data': {
                'status': 'healthy',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_available': data_available,
                'market_open': Config.is_market_open(),
                'features': [
                    '超时机制',
                    '多数据源切换',
                    '异动监测',
                    '技术分析',
                    '三层共振分析'
                ]
            }
        })
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return jsonify({
            'code': 500,
            'message': f'系统异常: {str(e)}',
            'data': {
                'status': 'unhealthy',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        })


@app.errorhandler(404)
def not_found(error):
    """404错误处理"""
    return jsonify({
        'code': 404,
        'message': '页面未找到',
        'data': None
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """500错误处理"""
    return jsonify({
        'code': 500,
        'message': '服务器内部错误',
        'data': None
    }), 500


@app.before_request
def before_request():
    """请求前处理"""
    # 记录请求日志
    logger.info(f"Request: {request.method} {request.url}")


@app.after_request
def after_request(response):
    """请求后处理"""
    # 设置响应头
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'

    return response


if __name__ == '__main__':
    # 创建日志目录
    os.makedirs('logs', exist_ok=True)

    # 配置日志
    logger.add(
        Config.LOG_FILE,
        rotation="1 day",
        retention="30 days",
        level=Config.LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )

    logger.info("A股股票分析系统启动中...")
    logger.info("系统特性：支持超时机制和多数据源自动切换")

    # 启动Flask应用
    app.run(
        host=Config.FLASK_HOST,
        port=Config.FLASK_PORT,
        debug=Config.FLASK_DEBUG
    )