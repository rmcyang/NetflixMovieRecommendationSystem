DEFINE split_into_training_testing(inputData, split_percentage)
RETURNS training, testing
{
    data = foreach $inputData generate RANDOM() as random_assignment, *;
    split data into testing_data if random_assignment <= $split_percentage, training_data otherwise;
    $training = foreach training_data generate $1..;
    $testing = foreach testing_data generate $1..;
};

indata = LOAD 'TrainingRatings.txt' USING PigStorage(',') AS (item_id:int, user_id:int, rating:double);
training, testing = split_into_training_testing(indata, 0.1);
grouped = GROUP indata BY item_id;
movieIndex = FOREACH grouped GENERATE group;
movieAvg = FOREACH grouped GENERATE group, AVG(indata.rating);

groupedUser = GROUP indata BY user_id;
userAvg = FOREACH groupedUser GENERATE group, AVG(indata.rating);

groupedTraining = GROUP training BY item_id;
itemInfo = FOREACH groupedTraining GENERATE group AS item_id, COUNT(training) AS numRating, SUM(training.rating) AS sumRatings;
prepared = JOIN itemInfo BY item_id, training BY item_id;
--DESCRIBE prepared;
ordered = FOREACH prepared GENERATE itemInfo::item_id, training::user_id, training::rating, itemInfo::numRating, itemInfo::sumRatings;



STORE userAvg INTO 'UserAvg';
STORE ordered INTO 'OrderedTrainingSet';
STORE movieIndex INTO 'MovieIndex';
STORE movieAvg INTO 'MovieAvg';
--STORE training INTO 'TrainingSet';
STORE testing INTO 'ValidationSet';
