records = LOAD '/home/hduser/Documents/Dezyre/module6-Pig/input/Batting.csv' USING PigStorage(',') AS (playerID:chararray, yearID:int,
stind:int, teamID:chararray, leagueID:chararray,  game: int, gamesBatted: int, atBat:int, runs:int, hits:int, twoBase:int, threebase:int,
homeRuns:int, runBattedIn:int, sb:int, cs:int, bb:int, so:int, ibb:int, hbp:int, sh:int, sf:int, gidp:int, gOld:int);

groupRecords = GROUP records BY playerID;

runsScoreByPlayer = FOREACH groupRecords GENERATE group, SUM(records.runs);

DUMP runsScoreByPlayer;