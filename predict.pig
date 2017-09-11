data = LOAD 'Files/top10TestOut/part-r-00000' USING PigStorage('\t') AS (key:chararray, value:chararray);
topn = FOREACH data GENERATE FLATTEN(STRSPLIT(key, ',')) AS (movieid:chararray, userid:chararray), FLATTEN(STRSPLIT(value, ',')) AS (movies:chararray, similarity:chararray);
topNnew = FOREACH topn GENERATE (int)movieid, (int)userid, (int)movies, (double)similarity;

training = LOAD 'OrderedTrainingSet' USING PigStorage('\t') AS (movies:int, userid:int, rating:double, numRatings:int, sumRatings:double);

movieAvg = LOAD 'MovieAvg' USING PigStorage('\t') AS (movie:int, avg:double);

trainingAvg = FOREACH training GENERATE movies, userid, rating, sumRatings/(double)numRatings AS avgB;

joined = JOIN topNnew BY (movies, userid), trainingAvg BY (movies, userid);
joinedAB = JOIN joined BY topNnew::movieid, movieAvg BY movie;
cleaned = FOREACH joinedAB GENERATE topNnew::movieid AS movieid, topNnew::userid AS userid, (topNnew::similarity * (0.925*(movieAvg::avg - trainingAvg::avgB) + trainingAvg::rating)) AS up, topNnew::similarity AS similarity;

grouped = GROUP cleaned BY (movieid,userid); 

--result = FOREACH grouped GENERATE group, ((SUM(cleaned.up)/SUM(cleaned.similarity))<=0.5? 1.0 : ROUND(SUM(cleaned.up)/SUM(cleaned.similarity)));
result = FOREACH grouped GENERATE group, SUM(cleaned.up)/SUM(cleaned.similarity) AS prediction;
newResult = FOREACH result GENERATE group AS newGroup, (prediction<=0.5?1.0:ROUND(prediction)) AS newPrediction; 
--SPLIT result INTO small IF result.prediction < 1, big IF result >= 1;
--newSmall = FOREACH small 
result1 = FOREACH newResult GENERATE FLATTEN(newGroup), newPrediction as prediction;
--STORE result INTO 'result';
testing = LOAD 'TestSet' USING PigStorage('\t') AS (movieid:int, userid:int, rating:double);
compare = JOIN result1 BY (movieid, userid), testing BY (movieid, userid);
errors = FOREACH compare GENERATE testing::movieid AS movieid, testing::userid AS userid, ABS(testing::rating - result1::prediction) AS error;
errors2 = FOREACH compare GENERATE testing::movieid AS movieid, testing::userid AS userid, (testing::rating - result1::prediction) * (testing::rating - result1::prediction) AS error;
DESCRIBE errors;
groupedErrors = GROUP errors ALL; 
groupedErrors2 = GROUP errors2 ALL;
MAE = FOREACH groupedErrors GENERATE SUM(errors.error)/COUNT(errors);
RSM = FOREACH groupedErrors2 GENERATE SQRT(SUM(errors2.error)/COUNT(errors2)) AS temp;
STORE MAE INTO 'MAE10_0.925';
STORE RSM INTO 'RSM10_0.925';

