records = LOAD '/home/hduser/Documents/Dezyre/module6-Pig/input/NASDAQ_daily_prices_A.csv' USING PigStorage(',')
AS (exchange:chararray, symbol:chararray, date:chararray, stockPriceOpen:double,
       stockPriceHigh:double,  stockPriceLow: double, stockPriceClose: double, stockVolume:long, stockPriceAdjClose: double);

REGISTER DateAsString-1.0-SNAPSHOT.jar;

dateAsString = FOREACH records GENERATE com.pravritti.DateAsString(date);
DUMP dateAsString;
