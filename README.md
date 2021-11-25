# Recommendation Engine built using Spark/Scala


For my course Systems for Data Science given in EPFL, we built a movie recommendation engine using Spark. We used the MovieLens 100K dataset, and tested the implementation on 25 million users. Since this is a big number of people, we used the breeze library to represent the matrices as sparse matrices CSCMatrix. We describe the algorithm used. Suppose r_{u,i} is the rating given by user u to movie i, and r_{u,.} is the average rating by the user u and r_{.,i} is the average rating for movie i. Then the scaled ratings are given by:

![alt text](https://github.com/aiday-mar/Spark-Recommendation-Engine/blob/main/eq6.PNG?raw=true)

![alt text](https://github.com/aiday-mar/Spark-Recommendation-Engine/blob/main/eq7.PNG?raw=true)


From this we can calculate the degree of similarity between two users s_{u,v} as follows:

![alt text](https://github.com/aiday-mar/Spark-Recommendation-Engine/blob/main/eq5.PNG?raw=true)

![alt text](https://github.com/aiday-mar/Spark-Recommendation-Engine/blob/main/eq4.PNG?raw=true)

From this we can calulate the predicted rating of user u for movie i as follows:

![alt text](https://github.com/aiday-mar/Spark-Recommendation-Engine/blob/main/eq3.PNG?raw=true)

![alt text](https://github.com/aiday-mar/Spark-Recommendation-Engine/blob/main/eq2.PNG?raw=true)

Once the predicted ratings are calculated, it is possible to test the algorithm using the mean absolute error on the testing set.

![alt text](https://github.com/aiday-mar/Spark-Recommendation-Engine/blob/main/MAE.PNG?raw=true)

Here is a description of the written code.m The scale function is implemented as follows:

```
def scale(rating: Double, ave_rating: Double): Double ={
   if (rating > ave_rating) {
      return 5 - ave_rating
   }
   else if (rating < ave_rating) {
      return ave_rating - 1
   }
   else {
      return 1
   }
}
```

The average user ratings are found (the average movie ratings are calculated similarly).

```
for( i <- 0 until conf_users) {

   val user_specific_training_row = train(i to i, 0 to (conf_movies - 1))
   val i_training_row_csc = CSCMatrix.tabulate(user_specific_training_row.rows, user_specific_training_row.cols)(user_specific_training_row(_,_))

   var sum = 0.0
   var counter = 0.0


    for ((k,v) <- i_training_row_csc.activeIterator) {
       val row = k._1
       val col = k._2
       sum = sum + i_training_row_csc(row, col)
       counter = counter + 1
   }

   average_rating_user(i) =(sum/counter.toDouble).toDouble
}
```

We compute the normalized deviations:

```
for ((k,v) <- train.activeIterator) {
   val row = k._1
   val col = k._2

   train(row, col) = (train(row, col) -average_rating_user(row))/scale(train(row,col), average_rating_user(row))
   intermediate(row, col) = train(row, col)*train(row, col)
}
````

The processed denominators are then:

```
val processed_denominators = DenseVector.zeros[Double](conf_users)

val processed_normalized_deviations = train.copy


for( i <- 0 until conf_users) {

   val i_squared_normalized_deviations = intermediate(i to i, 0 to conf_movies - 1)

   var sum = 0.0

   val i_squared_normalized_deviations_csc = CSCMatrix.tabulate(i_squared_normalized_deviations.rows, i_squared_normalized_deviations.cols)(i_squared_normalized_deviations(_, _))

   
for ((k,v) <- i_squared_normalized_deviations_csc.activeIterator) {
      val row = k._1
      val col = k._2
      sum = sum + i_squared_normalized_deviations_csc(row, col)
   }

   processed_denominators(i) = sum.toDouble
}
```

We compute the processed normalized deviations.

```
for ((k,v) <- train.activeIterator) {

   val row = k._1
   val col = k._2

   processed_normalized_deviations(row, col) = processed_normalized_deviations(row, col)/math.sqrt(processed_denominators(row))
}
```

Then we compute the similarities.

```
val broadcast_processed_normalized_deviations = sc.broadcast(processed_normalized_deviations)

def topk(user : Int) : (Int, IndexedSeq[(Int, Double)]) = {

   val processed_normalized_deviations_broadcasted = broadcast_processed_normalized_deviations.value
   val user_specific_processed_normalized_deviations = processed_normalized_deviations_broadcasted(user to user, 0 to (conf_movies - 1))
   val user_specific_processed_normalized_deviations_csc = CSCMatrix.tabulate(user_specific_processed_normalized_deviations.rows, user_specific_processed_normalized_deviations.cols)(user_specific_processed_normalized_deviations(_, _))


   val user_specific_similarities = user_specific_processed_normalized_deviations_csc * processed_normalized_deviations.t

   user_specific_similarities(0, user) = 0

   (user, argtopk(user_specific_similarities, conf_k).map{v => (v._2, user_specific_similarities(v))})
}

val topks = sc.parallelize(0 to conf_users - 1).map(u => topk(u)).collect()

var topks_values = for {
   user <- topks
   entry <- user._2
} yield (user._1, entry._1, entry._2)

val topksBuilder = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_users)

for (entry <- topks_values) {
   topksBuilder.add(entry._1, entry._2, entry._3)
}

var similaritiesTopK = topksBuilder.result()
```

The predictions are.

```
val predictions_builder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())
val broadcast_similaritiesTopK = sc.broadcast(similaritiesTopK)
val broadcast_train = sc.broadcast(train)
val broadcast_average_rating_user = sc.broadcast(average_rating_user)

def prediction(user : Int, item : Int) : Double = {

   val similaritiesTopK_broadcasted = broadcast_similaritiesTopK.value
   val train_broadcasted = broadcast_train.value
   val average_rating_user_broadcasted = broadcast_average_rating_user.value


   var user_specific_similarities = similaritiesTopK_broadcasted(user to user, 0 to conf_users - 1)

   var user_specific_similarities_csc = CSCMatrix.tabulate(user_specific_similarities.rows, user_specific_similarities.cols)(user_specific_similarities(_, _))

   val column_train = train_broadcasted(0 to conf_users - 1, item to item)

   var column_train_csc = CSCMatrix.tabulate(column_train.rows, column_train.cols)(column_train(_, _))

   val numerator = user_specific_similarities_csc * column_train_csc


   for ((k,v) <- user_specific_similarities_csc.activeIterator) {
       val row = k._1
       val col = k._2
       user_specific_similarities_csc(row, col) = scala.math.abs(user_specific_similarities_csc(row, col))
   }


   for ((k,v) <- column_train_csc.activeIterator) {
       val row = k._1
       val col = k._2
       column_train_csc(row, col) = 1
   }

   val denominator = user_specific_similarities_csc * column_train_csc
   val user_specific_deviation = numerator(0,0).toDouble/denominator(0,0).toDouble

   val prediction = average_rating_user(user) + user_specific_deviation * scale(average_rating_user(user) + user_specific_deviation, average_rating_user(user))

    prediction
}

val predictions_parallelize = sc.parallelize((for((k,v) <- test.activeIterator) yield k).toSeq).map{case (user,item) => (user,item,prediction(user,item))}.collect()

for (entry <- predictions_parallelize) {
    predictions_builder.add(entry._1, entry._2, entry._3)
}

val predictions = predictions_builder.result()
```

Finally we compute the MAE.

```
for ((k,v) <- test.activeIterator) {
   val row = k._1
   val col = k._2

   val toAdd = scala.math.abs(predictions(row, col) - test(row, col))

   if (!toAdd.isNaN) {
      test(row, col) = toAdd
   } else {
      test(row, col) = -1
   }
}

var n_nonzero = 0.0
var final_sum = 0.0

for ((k,v) <- test.activeIterator) {

   val row = k._1
   val col = k._2

   if(test(row,col) != -1) {
      n_nonzero = n_nonzero + 1
      final_sum = final_sum + test(row, col)
   }
}

val mae = (final_sum.toDouble/n_nonzero.toDouble).toDouble
```
