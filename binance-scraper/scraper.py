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
        timestamp       timestamp NOT NULL,
        high            numeric(16),
        low             numeric(16),
        bid             numeric(16),
        bidVolume       numeric(16),
        ask             numeric(16),
        askVolume       numeric(16),
        vwap            numeric(16),
        open            numeric(16),
        close           numeric(16),
        last            numeric(16),
        previousClose   numeric(16),
        change          numeric(16),
        percentage      numeric(16),
        baseVolume      numeric(16),
        quoteVolume     numeric(16)
    );
    """
    sql_info = f"""
        CREATE TABLE IF NOT EXISTS {table_name_info} (
            id bigserial        PRIMARY KEY,
            timestamp           timestamp NOT NULL,
            priceChange         numeric(16),
            priceChangePercent  numeric(16),
            weightedAvgPrice    numeric(16),
            prevClosePrice      numeric(16),
            lastPrice           numeric(16),
            lastQty             numeric(16),
            bidPrice            numeric(16),
            bidQty              numeric(16),
            askPrice            numeric(16),
            askQty              numeric(16),
            openPrice           numeric(16),
            highPrice           numeric(16),
            lowPrice            numeric(16),
            volume              numeric(16),
            quoteVolume         numeric(16),
            openTime            numeric(16),
            closeTime           numeric(16),
            firstId             numeric(16),
            lastId              numeric(16),
            count               numeric(16)
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
            to_timestamp({ticker['timestamp']}),
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
            to_timestamp({ticker['timestamp']}),
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
