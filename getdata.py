import requests
import sqlite3
import time
from datetime import datetime
import pandas as pd
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


BINANCE_BASE_URL = 'https://fapi.binance.com'
BITGET_BASE_URL = 'https://api.bitget.com'
db_path = '/Users/bitcrab/tradingdata/crypto_trades.db'

def get_bitget_pairs() -> List[str]:
    """获取 Bitget 的所有U本位永续合约交易对"""
    try:
        # 获取 U本位合约交易对
        umcbl_response = requests.get(f'{BITGET_BASE_URL}/api/mix/v1/market/contracts?productType=umcbl')
        umcbl_data = umcbl_response.json()
        umcbl_pairs = [item['symbol'] for item in umcbl_data['data']]
        pairs = [pair[:-6] for pair in umcbl_pairs]
        return pairs
    except Exception as e:
        logging.error(f"获取Bitget交易对失败: {e}")
        return []

def update_trading_pairs():
    """更新交易对列表"""
    try:
        # 获取币安交易对
        response = requests.get(f'{BINANCE_BASE_URL}/fapi/v1/exchangeInfo')
        response.raise_for_status()
        binance_data = response.json()

        # 获取 Bitget 交易对
        umcbl_response = requests.get(f'{BITGET_BASE_URL}/api/mix/v1/market/contracts?productType=umcbl')
        umcbl_data = umcbl_response.json()
        umcbl_pairs = [item['symbol'] for item in umcbl_data['data']]
        bitget_pairs = [pair[:-6] for pair in umcbl_pairs]

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        current_time = datetime.now()

        # 获取数据库中已存在的所有交易对
        cursor.execute('SELECT DISTINCT symbol FROM um_pairs')
        existing_symbols = set(row[0] for row in cursor.fetchall())

        # 处理币安交易对
        binance_active_pairs = [
            symbol['symbol'] for symbol in binance_data['symbols']
            if symbol['status'] == 'TRADING' and symbol['contractType'] == 'PERPETUAL'
        ]

        # 更新币安交易对
        for symbol in binance_active_pairs:
            if symbol not in existing_symbols:
                cursor.execute('''
                INSERT INTO um_pairs (symbol, exchange, last_update)
                VALUES (?, 'BN', ?)
                ''', (symbol, current_time))
                existing_symbols.add(symbol)

        # 处理 Bitget 交易对
        for symbol in bitget_pairs:
            if symbol not in existing_symbols:
                cursor.execute('''
                INSERT INTO um_pairs (symbol, exchange, last_update)
                VALUES (?, 'BG', ?)
                ''', (symbol, current_time))
                existing_symbols.add(symbol)

        conn.commit()
        conn.close()

        # 计算新增的交易对数量
        new_binance_pairs = len([s for s in binance_active_pairs if s not in existing_symbols])
        new_bitget_pairs = len([s for s in bitget_pairs if s not in existing_symbols])

        logging.info(f"成功更新交易对列表，新增币安: {new_binance_pairs}个, 新增Bitget: {new_bitget_pairs}个")
        return True

    except Exception as e:
        logging.error(f"更新交易对列表失败: {e}")
        return False


def get_all_pairs() -> Dict[str, List[str]]:
    """获取所有交易对，按交易所分类，排除黑名单中的交易对"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 修改 SQL 查询，增加 blacklist 条件，并确保 exchange 不为空
    cursor.execute('''
    SELECT symbol, exchange 
    FROM um_pairs 
    WHERE (blacklist IS NULL OR blacklist != 'Y')
    AND exchange IS NOT NULL 
    AND exchange != ''
    ''')

    results = cursor.fetchall()
    conn.close()

    pairs = {
        'BN': [],
        'BG': []
    }

    for symbol, exchange in results:
        # 添加额外的检查
        if exchange in pairs:
            pairs[exchange].append(symbol)
        else:
            logging.warning(f"发现未知交易所标识: {exchange}, 交易对: {symbol}")

    # 添加日志记录获取到的交易对数量
    logging.info(f"获取到有效交易对：币安 {len(pairs['BN'])}个, Bitget {len(pairs['BG'])}个")

    return pairs

def split_list(lst, n):
    """将列表均匀分割成n份"""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def collect_data_for_pairs(pairs: List[str], current_time: datetime) -> List[Dict]:
    """收集指定交易对列表的数据"""
    thread_data = []
    for symbol in pairs:
        logging.warning(f"开始收集{symbol}数据")
        data = get_binance_data(symbol)
        if data:
            data['timestamp'] = current_time
            data['symbol'] = symbol
            thread_data.append(data)
    return thread_data

def get_binance_data(symbol: str) -> Optional[Dict]:
    """获取单个交易对的数据"""
    data = {}

    try:
        # 获取标记价格和指数价格
        response = requests.get(f'{BINANCE_BASE_URL}/fapi/v1/premiumIndex', params={'symbol': symbol})
        response.raise_for_status()
        premium_data = response.json()
        data['mark_price'] = float(premium_data['markPrice'])
        data['index_price'] = float(premium_data['indexPrice'])
        data['basis'] = data['mark_price'] - data['index_price']
        data['basis_percent'] = (data['basis'] / data['index_price']) * 100

        ##data['last_funding_rate'] = (data['mark_price'] - data['index_price'] ) / data['index_price'] * 1.15


        # 获取最新资金费率
        response = requests.get(f'{BINANCE_BASE_URL}/fapi/v1/fundingRate', params={'symbol': symbol, 'limit': 1})
        response.raise_for_status()
        funding_data = response.json()
        data['last_funding_rate'] = float(funding_data[0]['fundingRate']) if funding_data else 0


        # 获取持仓量
        response = requests.get(f'{BINANCE_BASE_URL}/fapi/v1/openInterest', params={'symbol': symbol})
        response.raise_for_status()
        oi_data = response.json()
        data['oi'] = float(oi_data['openInterest'])

        # 公共参数
        ratio_params = {'symbol': symbol, 'period': '5m', 'limit': 1}

        # 获取账户多空比
        response = requests.get(f'{BINANCE_BASE_URL}/futures/data/globalLongShortAccountRatio', params=ratio_params)
        response.raise_for_status()
        ls_account = response.json()
        data['long_short_account_ratio'] = float(ls_account[0]['longShortRatio'])

        # 获取大户账户多空比
        response = requests.get(f'{BINANCE_BASE_URL}/futures/data/topLongShortAccountRatio', params=ratio_params)
        response.raise_for_status()
        top_account = response.json()
        data['top_trader_account_ls_ratio'] = float(top_account[0]['longShortRatio'])

        # 获取大户持仓多空比
        response = requests.get(f'{BINANCE_BASE_URL}/futures/data/topLongShortPositionRatio', params=ratio_params)
        response.raise_for_status()
        top_position = response.json()
        data['top_trader_position_ls_ratio'] = float(top_position[0]['longShortRatio'])

        # 获取主动买卖量
        response = requests.get(f'{BINANCE_BASE_URL}/futures/data/takerlongshortRatio', params=ratio_params)
        response.raise_for_status()
        taker_data = response.json()
        buy_vol = float(taker_data[0]['buyVol'])
        sell_vol = float(taker_data[0]['sellVol'])
        data['taker_buy_sell_ratio'] = buy_vol / sell_vol if sell_vol != 0 else 0

        return data

    except Exception as e:
        logging.error(f"{symbol} 数据获取失败: {e}")
        return None

def collect_bitget_data(pairs: List[str], current_time: datetime) -> List[Dict]:
    """收集 Bitget 交易对的数据"""
    thread_data = []
    for symbol in pairs:
        logging.warning(f"开始收集 Bitget {symbol} 数据")
        data = get_bitget_data(symbol)
        if data:
            data['timestamp'] = current_time
            data['symbol'] = symbol
            thread_data.append(data)
    return thread_data


def get_bitget_data(symbol: str) -> Optional[Dict]:
    """获取 Bitget 单个交易对的数据"""
    data = {}
    try:
        # 获取标记价格和指数价格
        response = requests.get(f'{BITGET_BASE_URL}/api/v2/mix/market/symbol-price',
                                params={'productType': 'usdt-futures', 'symbol': symbol})
        response.raise_for_status()
        price_data = response.json()

        bitget_symbol = symbol+'_UMCBL'

        if price_data['code'] == '00000' and price_data['data']:
            # 获取价格数据
            symbol_data = price_data['data'][0]
            data['mark_price'] = float(symbol_data['markPrice'])
            data['index_price'] = float(symbol_data['indexPrice'])
            data['basis'] = data['mark_price'] - data['index_price']
            data['basis_percent'] = (data['basis'] / data['index_price']) * 100

            # 获取资金费率
            response = requests.get(
                f'{BITGET_BASE_URL}/api/v2/mix/market/current-fund-rate',
                params={
                    'symbol': symbol,
                    'productType': 'usdt-futures'
                }
            )
            response.raise_for_status()
            funding_data = response.json()
            ##print(data)
            if funding_data['code'] == '00000' and funding_data['data']:
                data['last_funding_rate'] = float(funding_data['data'][0]['fundingRate'])
            else:
                data['last_funding_rate'] = 0


            # 获取持仓量
            response = requests.get(f'{BITGET_BASE_URL}/api/mix/v1/market/open-interest?symbol={bitget_symbol}')
            oi_data = response.json()
            ##print(oi_data)
            if oi_data['code'] == '00000':
                data['oi'] = float(oi_data['data']['amount'])
            else:
                data['oi'] = 0

            # Bitget可能没有这些数据，设置默认值
            data['long_short_account_ratio'] = 0
            data['top_trader_account_ls_ratio'] = 0
            data['top_trader_position_ls_ratio'] = 0
            data['taker_buy_sell_ratio'] = 0

            return data
        else:
            logging.error(f"获取 Bitget {symbol} 价格数据失败: {price_data.get('msg', 'No data returned')}")
            return None

    except Exception as e:
        logging.error(f"Bitget {symbol} 数据获取失败: {e}")
        logging.error(f"错误详情: ", exc_info=True)
        return None


def collect_and_store_data(num_threads: int = 5):
    """收集所有交易对的数据并存储"""
    try:
        logging.warning("开始收集数据")
        pairs_by_exchange = get_all_pairs()

        if not pairs_by_exchange or ('BN' not in pairs_by_exchange and 'BG' not in pairs_by_exchange):
            logging.error("没有找到任何交易对数据")
            return

        logging.info(f"获取到交易对数据: 币安 {len(pairs_by_exchange.get('BN', []))}个, "
                     f"Bitget {len(pairs_by_exchange.get('BG', []))}个")

        current_time = datetime.now()

        # 分别处理币安和Bitget的交易对
        binance_pairs = pairs_by_exchange.get('BN', [])
        bitget_pairs = pairs_by_exchange.get('BG', [])

        # 创建线程池并提交任务
        all_data = []
        executor = ThreadPoolExecutor(max_workers=num_threads)
        try:
            futures = []

            # 如果有币安交易对，分配线程处理
            if binance_pairs:
                split_pairs = split_list(binance_pairs, max(1, num_threads - 1))
                for i, pair_list in enumerate(split_pairs):
                    future = executor.submit(collect_data_for_pairs, pair_list, current_time)
                    futures.append((future, f"Binance-{i}"))
                logging.info(f"已提交 {len(split_pairs)} 个币安数据收集任务")

            # 如果有Bitget交易对，分配一个线程处理
            if bitget_pairs:
                bitget_future = executor.submit(collect_bitget_data, bitget_pairs, current_time)
                futures.append((bitget_future, "Bitget"))
                logging.info("已提交 Bitget 数据收集任务")
            # 收集所有结果
            for future, thread_name in futures:
                try:
                    thread_data = future.result()
                    if thread_data:
                        logging.info(f"线程 {thread_name} 完成数据收集，获取到 {len(thread_data)} 条数据")
                        all_data.extend(thread_data)
                    else:
                        logging.warning(f"线程 {thread_name} 没有返回数据")
                except Exception as e:
                    logging.error(f"线程 {thread_name} 执行失败: {str(e)}")
                    logging.exception("详细错误信息：")

        finally:
            executor.shutdown(wait=True)
            logging.info("所有线程已完成")

        if not all_data:
            logging.warning("没有收集到任何数据")
            return

        try:
            # 转换为DataFrame并批量写入
            df = pd.DataFrame(all_data)
            columns = [
                'timestamp', 'symbol', 'mark_price', 'index_price', 'basis',
                'basis_percent', 'last_funding_rate', 'oi', 'long_short_account_ratio',
                'top_trader_account_ls_ratio', 'top_trader_position_ls_ratio',
                'taker_buy_sell_ratio'
            ]
            df = df[columns]

            conn = sqlite3.connect(db_path)
            df.to_sql('um_data', conn, if_exists='append', index=False)
            conn.close()

            logging.info(f"成功批量存储 {len(df)} 条数据")
            logging.info(f"数据统计:\n"
                         f"交易对数量: {df['symbol'].nunique()}\n"
                         f"平均基差率: {df['basis_percent'].mean():.4f}%")

        except Exception as e:
            logging.error(f"数据存储失败: {str(e)}")
            logging.exception("详细错误信息：")

    except Exception as e:
        logging.error(f"数据收集过程中发生错误: {str(e)}")
        logging.exception("详细错误信息：")

def main():

    print('begin update trading pairs')
    update_trading_pairs()
    last_pairs_update = datetime.now()

    while True:
        try:
            # 每小时更新一次交易对列表
            current_time = datetime.now()
            if (current_time - last_pairs_update).total_seconds() >= 3600:  # 3600秒 = 1小时
                update_trading_pairs()
                last_pairs_update = current_time
                
            collect_and_store_data()

            # 等待到下一分钟
            time.sleep(10)

        except Exception as e:
            logging.error(f"主循环发生错误: {e}")
            time.sleep(60)  # 发生错误时等待1分钟后继续

if __name__ == '__main__':
    main()