import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
import logging

db_path='/Users/crypto_trades.db' ###replace with your own db path

def monitor_trading_data():
    """监控交易数据，检查资金费率和持仓量变化"""
    try:
        # 连接数据库
        conn = sqlite3.connect(db_path)

        # 获取当前时间
        current_time = datetime.now()
        print(current_time)

        # 获取所有唯一的交易对及其对应的交易所
        symbols_query = """
        SELECT DISTINCT um_data.symbol, um_pairs.exchange 
        FROM um_data 
        JOIN um_pairs ON um_data.symbol = um_pairs.symbol
        """
        symbols_df = pd.read_sql_query(symbols_query, conn)

        for _, row in symbols_df.iterrows():
            symbol = row['symbol']
            exchange = 'Binance' if row['exchange'] == 'BN' else 'Bitget'

            # 获取该交易对最近的10条记录
            query = """
            SELECT timestamp, last_funding_rate, oi, basis_percent
            FROM um_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 10
            """
            df = pd.read_sql_query(query, conn, params=(symbol,))

            # 将timestamp转换为datetime对象
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            if df.empty or len(df) < 3:
                continue

            # 检查最新记录的时间戳
            latest_record_time = df['timestamp'].iloc[0]
            time_diff = current_time - latest_record_time

            # 检查资金费率（最近5分钟内的记录）
            #print('here I am')
            if time_diff <= timedelta(minutes=500):
                latest_funding_rate = df['last_funding_rate'].iloc[0]
                #print(latest_funding_rate)
                if abs(latest_funding_rate) > 0.002:  # 0.1%
                    logging.warning(f"警报 - 高资金费率 - {exchange}: {symbol}:")
                    logging.warning(f"当前时间： {latest_record_time}。 当前资金费率: {latest_funding_rate * 100:.4f}%")

                # 检查基差率
                latest_basis_percent = df['basis_percent'].iloc[0]
                if abs(latest_basis_percent) > 1:  # 1%
                    logging.warning(f"警报 - 高基差率 - {exchange}: {symbol}:")
                    logging.warning(f"当前时间： {latest_record_time}。当前基差率: {latest_basis_percent:.4f}%")
                    # 如果同时出现高资金费率，额外提醒
                    if abs(latest_funding_rate) > 0.002:
                        logging.warning(
                            f"注意: {exchange}: {symbol} 同时出现高资金费率 ({latest_funding_rate * 100:.4f}%) "
                            f"和高基差率 ({latest_basis_percent:.4f}%)")

            # 检查OI变化（最近15分钟内的记录）
            recent_records = df[df['timestamp'] >= current_time - timedelta(minutes=50)]

            if len(recent_records) >= 3:
                # 计算最近3条记录的OI均值
                recent_3_oi_mean = df.head(3)['oi'].mean()

                # 计算最近10条记录的OI均值
                all_10_oi_mean = df['oi'].mean()+0.1

                # 检查OI是否激增
                if recent_3_oi_mean / all_10_oi_mean > 1.1:
                    logging.warning(f"警报 - OI激增 - {exchange}: {symbol}:")
                    logging.warning(f"当前时间： {latest_record_time}。 最近3条记录OI均值: {recent_3_oi_mean:,.0f}")
                    logging.warning(f"最近10条记录OI均值: {all_10_oi_mean:,.0f}")
                    logging.warning(f"增长比例: {(recent_3_oi_mean / all_10_oi_mean - 1) * 100:.2f}%")

        conn.close()

    except Exception as e:
        logging.error(f"监控过程中发生错误: {e}")
        if conn:
            conn.close()


def format_monitoring_message(symbol: str, exchange: str, alert_type: str, details: dict) -> str:
    """格式化监控消息"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exchange_name = 'Binance' if exchange == 'BN' else 'Bitget'

    if alert_type == "funding_rate":
        return (f"[{current_time}] 资金费率警报 - {exchange_name}: {symbol} \n"
                f"当前资金费率: {details['rate'] * 100:.4f}%")

    elif alert_type == "oi_surge":
        return (f"[{current_time}] 持仓量激增警报 - {exchange_name}: {symbol}\n"
                f"最近3条均值: {details['recent_mean']:,.0f}\n"
                f"10条均值: {details['total_mean']:,.0f}\n"
                f"增长比例: {(details['ratio'] - 1) * 100:.2f}%")

    return ""

while True:
    try:
            # 运行监控
        monitor_trading_data()

            # 等待一分钟
        time.sleep(60)

    except Exception as e:
        logging.error(f"主循环发生错误: {e}")
        time.sleep(60)