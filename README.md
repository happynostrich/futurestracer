# futurestracer
本脚本供加密合约交易者监控行情使用，监控binance与bitget各USDT本位合约参数变化并警告。
部署时需本机安装数据库，如SQLite，安装好用新数据库路径替换掉原来代码中缺省值，如 db_path = '/Users/crypto_trades.db'
init.py用来创建所需数据库表。运行一次即可。
getdata.py用来不断读取数据，写入数据库。
alarm.py每隔一分钟扫描数据库，根据设置给出警报。
