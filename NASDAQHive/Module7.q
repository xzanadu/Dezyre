CREATE DATABASE dezyrehive;

CREATE EXTERNAL TABLE dezyrehive.dailyprices (xchange STRING, symbol STRING, dt DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vol INT, adjustedclose DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
LOCATION '/user/hive/warehouse/dezyrehive/dailyprices';

CREATE EXTERNAL TABLE dezyrehive.dividends (xchange STRING, symbol STRING, dt DATE, divd DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
LOCATION '/user/hive/warehouse/dezyrehive/dividends';

LOAD DATA LOCAL INPATH '/home/hduser/Documents/Dezyre/module7-hive/input/NASDAQ_daily_prices_A.csv' OVERWRITE INTO TABLE dezyrehive.dailyprices;

LOAD DATA LOCAL INPATH '/home/hduser/Documents/Dezyre/module7-hive/input/NASDAQ_dividends_A.csv' OVERWRITE INTO TABLE dezyrehive.dividends;

SELECT symbol, SUM(vol) FROM dezyrehive.dailyprices WHERE close > 5 group by symbol;

SELECT symbol, MAX(high) FROM dezyrehive.dailyprices group by symbol;

SELECT symbol, MAX(divd) FROM dezyrehive.dividends group by symbol;

SELECT symbol, MAX(high), MAX(divd) FROM dailyprices JOIN dividends ON (dailyprices.symbol = dividends.symbol) group by dailyprices.symbol;

SELECT symbol, MAX(high), MAX(divd) FROM dailyprices FULL OUTER JOIN dividends ON (dailyprices.symbol = dividends.symbol) group by dailyprices.symbol;





