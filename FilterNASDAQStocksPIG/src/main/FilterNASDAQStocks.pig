records = LOAD '/home/hduser/Documents/Dezyre/module6-Pig/input/NASDAQ_daily_prices_A.csv' USING PigStorage(',') AS (exchange:chararray, symbol:chararray, date:chararray, stockPriceOpen:double,
    stockPriceHigh:double,  stockPriceLow: double, stockPriceClose: double, stockVolume:long, stockPriceAdjClose: double);

REGISTER FilterNASDAQStocks-1.0-SNAPSHOT.jar;
filteredRecords = FILTER records BY com.pravritti.FilterNASDAQStocks(stockPriceOpen, stockPriceHigh, stockPriceLow, stockPriceClose);

DUMP filteredRecords;