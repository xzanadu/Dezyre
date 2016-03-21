records = LOAD '/home/hduser/Documents/Dezyre/module6-Pig/input/NASDAQ_daily_prices_A.csv' USING PigStorage(',') AS (exchange:chararray, symbol:chararray, date:chararray, stockPriceOpen:double,
    stockPriceHigh:double,  stockPriceLow: double, stockPriceClose: double, stockVolume:long, stockPriceAdjClose: double);

groupRecords = GROUP records BY symbol;

StockGrpByVol = FOREACH groupRecords GENERATE group, SUM(records.stockVolume);

TopTenStockByVol = LIMIT StockGrpByVol 10;

DUMP TopTenStockByVol;