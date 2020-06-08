import asyncio
import os

import asyncpg
import ccxt.async_support as ccxt

symbol_map = {
    'BTC/USDT': 'btc',
    'ETH/USDT': 'eth',
    'BNB/USDT': 'bnb',
    'EOS/USDT': 'eos'
}


def build_tables(symbol):
    table_name_value = 'ticker_value_%s' % symbol_map[symbol]
    table_name_info = 'ticker_info_%s' % symbol_map[symbol]
    sql_value = f"""
    CREATE TABLE IF NOT EXISTS {table_name_value} (
        id bigserial    PRIMARY KEY,
        timestamp       bigint NOT NULL,
        high            numeric,
        low             numeric,
        bid             numeric,
        bidVolume       numeric,
        ask             numeric,
        askVolume       numeric,
        vwap            numeric,
        open            numeric,
        close           numeric,
        last            numeric,
        previousClose   numeric,
        change          numeric,
        percentage      numeric,
        baseVolume      numeric,
        quoteVolume     numeric
    );
    """
    sql_info = f"""
        CREATE TABLE IF NOT EXISTS {table_name_info} (
            id bigserial        PRIMARY KEY,
            timestamp           bigint NOT NULL,
            priceChange         numeric,
            priceChangePercent  numeric,
            weightedAvgPrice    numeric,
            prevClosePrice      numeric,
            lastPrice           numeric,
            lastQty             numeric,
            bidPrice            numeric,
            bidQty              numeric,
            askPrice            numeric,
            askQty              numeric,
            openPrice           numeric,
            highPrice           numeric,
            lowPrice            numeric,
            volume              numeric,
            quoteVolume         numeric,
            openTime            bigint,
            closeTime           bigint,
            firstId             bigint,
            lastId              bigint,
            count               bigint
        );
        """
    return sql_value, sql_info


def build_insert_ticker(symbol, ticker, ticker_info):
    table_name_value = 'ticker_value_%s' % symbol_map[symbol]
    table_name_info = 'ticker_info_%s' % symbol_map[symbol]
    sql_value = f"""
        INSERT INTO {table_name_value} (
            timestamp,
            high,
            low,
            bid,
            bidVolume,
            ask,
            askVolume,
            vwap,
            open,
            close,
            last,
            previousClose,
            change,
            percentage,
            baseVolume,
            quoteVolume
        ) values (
            {ticker['timestamp']},
            {ticker['high']},
            {ticker['low']},
            {ticker['bid']},
            {ticker['bidVolume']},
            {ticker['ask']},
            {ticker['askVolume']},
            {ticker['vwap']},
            {ticker['open']},
            {ticker['close']},
            {ticker['last']},
            {ticker['previousClose']},
            {ticker['change']},
            {ticker['percentage']},
            {ticker['baseVolume']},
            {ticker['quoteVolume']}
        );
    """
    sql_info = f"""
        INSERT INTO {table_name_info} (
            timestamp,
            priceChange,
            priceChangePercent,
            weightedAvgPrice,
            prevClosePrice,
            lastPrice,
            lastQty,
            bidPrice,
            bidQty,
            askPrice,
            askQty,
            openPrice,
            highPrice,
            lowPrice,
            volume,
            quoteVolume,
            openTime,
            closeTime,
            firstId,
            lastId,
            count
        ) VALUES (
            {ticker['timestamp']},
            {ticker_info['priceChange']},
            {ticker_info['priceChangePercent']},
            {ticker_info['weightedAvgPrice']},
            {ticker_info['prevClosePrice']},
            {ticker_info['lastPrice']},
            {ticker_info['lastQty']},
            {ticker_info['bidPrice']},
            {ticker_info['bidQty']},
            {ticker_info['askPrice']},
            {ticker_info['askQty']},
            {ticker_info['openPrice']},
            {ticker_info['highPrice']},
            {ticker_info['lowPrice']},
            {ticker_info['volume']},
            {ticker_info['quoteVolume']},
            {ticker_info['openTime']},
            {ticker_info['closeTime']},
            {ticker_info['firstId']},
            {ticker_info['lastId']},
            {ticker_info['count']}
        );
    """
    return sql_value, sql_info


async def handle_symbol(symbol, exchange, pg_pool):
    async with pg_pool.acquire() as connection:
        async with connection.transaction():
            try:
                ticker = await exchange.fetch_ticker(symbol)
                sql_value, sql_info = build_insert_ticker(symbol, ticker, ticker['info'])
                await connection.execute(sql_value)
                await connection.execute(sql_info)
            except ccxt.RequestTimeout as e:
                print('[' + type(e).__name__ + ']')
                print(str(e)[0:200])
            except ccxt.DDoSProtection as e:
                print('[' + type(e).__name__ + ']')
                print(str(e.args)[0:200])
            except ccxt.ExchangeNotAvailable as e:
                print('[' + type(e).__name__ + ']')
                print(str(e.args)[0:200])
            except ccxt.ExchangeError as e:
                print('[' + type(e).__name__ + ']')
                print(str(e)[0:200])
                exit(1)


database_host = os.environ['PG_HOST']
database_port = os.environ['PG_PORT']
database_name = os.environ['PG_NAME']
database_user = os.environ['PG_USER']
database_pass = os.environ['PG_PASS']


async def main(symbols):
    exchange = ccxt.binance({'enableRateLimit': True})
    pg_pool = await asyncpg.create_pool(
        host=database_host,
        port=database_port,
        user=database_user,
        password=database_pass,
        database=database_name,
        command_timeout=60
    )

    async with pg_pool.acquire() as connection:
        async with connection.transaction():
            print(f'Initalizing tables for {symbols}')
            for symbol in symbols:
                sql_value, sql_info = build_tables(symbol)
                await connection.execute(sql_value)
                await connection.execute(sql_info)

    while True:
        print(exchange.iso8601(exchange.milliseconds()), 'fetching', symbols, 'ticker from', exchange.name)
        for symbol in symbols:
            await handle_symbol(symbol, exchange, pg_pool)


symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'EOS/USDT']
if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main(symbols))
