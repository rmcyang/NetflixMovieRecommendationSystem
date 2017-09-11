# NetflixMovieRecommendationSystem

This movie recommendation system based on item-item collaborative filtering algorithm 
was implemented in Hadoop MapReduce and PIG. The data provided by Netflix contains 3.25 million
training ratings and around 100000 testing ratings.

The following data was obtained by PIG(Count.pig):

Distinct movies test set 1701<br />
Distinct users in test set 27555<br />
Distinct movies in training set 1821<br />
Distinct users in training set 28978<br />
Number of paris in training set 3255352<br />
Number of pairs in test set 100478

Steps:

1. Data Preprocessing(DataPreprocessing.pig):
The specific function of Data Preprocessing is to transform the training set with schema (movie-id, user-id,
ratings), and emit data with schema (user-id, movie-id, stats{rating, numRatings, sumRatings}),
numRatings stands for the number of users who have rated movie x; sumRaings stands for sum
of all ratings of movie x; both of which are used in calculating Pearson Correlation in Job1.
Preprocessing also generate lists like MovieAvg, MovieIndex & UserAvg, which would be used
in later MapReduce Jobs.

2. Similarity Matrix:
This step is to generate a symmetric similarity. This symmetric similarity matrix will be loaded into the distributed cache for the next
step of implementations.

3. Generate a list of the most top k similar movies for each movie.<br />
a. Get Similarity Matrix for Each Movie-Movie Pair. Two MapReduce jobs was implemented to complete this progress.<br />
b. One MapReduce job to generete top-k list for each movie.

4. Predicttion(predict.pig) - to predict the ratings of movie-user pair:<br />
a. Theinput is TopN list of similarities of each movie, MovieAvg for querying average ratings for
each movie and formatted training set for getting movie-user ratings. The prediction emits tuples as ([movie, user], rating). The predict ratings will fill in all the blanks of movie-user pairs.<br />
b. Evaluation - Use two error measurement for evaluation, the Mean Absolute Error and the Root MeanSquared Error.
