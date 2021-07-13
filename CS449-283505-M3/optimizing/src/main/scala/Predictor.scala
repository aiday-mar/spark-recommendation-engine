import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String]()
  verify()
}

object Predictor {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- trainFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        trainBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    var train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0,9)) + "s")
  
    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    var test = testBuilder.result()
    testFile.close

    // *** Functions ***
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()

    def scale(rating: Double, ave_rating: Double): Double = {
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

    def calculateSD(arrayTime : ArrayBuffer[Double]) : Double = {
      val mean = arrayTime.sum/5
      var arrayOfTerms = new ArrayBuffer[Double]()
      for ( i <- 0 to 4) {
         arrayOfTerms += (arrayTime(i)-mean)*(arrayTime(i)-mean)/(1000*1000)
      }
      return math.sqrt(arrayOfTerms.sum/5)
    }

    // *** QUESTION 3.2.1 ***
  
    println("Computing exercise 3.2.1")
         
    // Average exist for all users and item, so store in a Dense matrix
    val average_rating_user = DenseVector.zeros[Double](conf.users())
    val average_rating_item = DenseVector.zeros[Double](conf.movies())

    // Find average rating for all users
    for( i <- 0 until conf.users()) {
 
       // Returns a slice matrix
       val i_training_row = train(i to i, 0 to (conf.movies() - 1))
      
       var sum = 0.0
       var counter = 0.0

       // Convert the slice matrix to a CSC matrix
       val i_training_row_csc = CSCMatrix.tabulate(i_training_row.rows, i_training_row.cols)(i_training_row(_, _))
        
       for ((k,v) <- i_training_row_csc.activeIterator) {
         val row = k._1
         val col = k._2
         sum = sum + i_training_row_csc(row, col)
         counter = counter + 1
       }       

       // Divide the sum of the elements by the number of elements
       average_rating_user(i) = (sum/counter.toDouble).toDouble
    }

    // Find average for all items
    for (i <- 0 until conf.movies()) {

       // Returns a slice matrix 
       val i_training_column = train(0 to (conf.users() - 1), i to i)

       var sum = 0.0
       var counter = 0.0

       // Convert the slice matrix to a CSC matrix
       val i_training_column_csc = CSCMatrix.tabulate(i_training_column.rows, i_training_column.cols)(i_training_column(_, _))

       for ((k,v) <- i_training_column_csc.activeIterator) {
         val row = k._1
         val col = k._2
         sum = sum + i_training_column_csc(row, col)
         counter = counter + 1
       }
       
       // Divide the sum of the elements by the number of elements
       average_rating_item(i) = (sum/counter.toDouble).toDouble
    }
    
    val intermediate = train.copy

    // Computing the normalized deviations, result stored in train so as to save memory
    for ((k,v) <- train.activeIterator) {
       val row = k._1
       val col = k._2

       train(row, col) = (train(row, col) - average_rating_user(row))/scale(train(row,col), average_rating_user(row))
       intermediate(row, col) = train(row, col)*train(row, col)
    }
    
    // To compute the processed normalized deviations, find the denominator first
    // This denominator takes the sum of the squares of the normalized deviations
    // for user u over all items rated by user u. These will be stored in a Dense vector
    val processed_denominators = DenseVector.zeros[Double](conf_users)

    val processed_normalized_deviations = train.copy

    // Computing the denominators
    for( i <- 0 until conf_users) {

       // println("Computing the denominator of the processed ratings for user " + i)
       val i_squared_normalized_deviations = intermediate(i to i, 0 to conf_movies - 1)

       var sum = 0.0

       // Convert the slice matrix to a CSC matrix
       val i_squared_normalized_deviations_csc = CSCMatrix.tabulate(i_squared_normalized_deviations.rows, i_squared_normalized_deviations.cols)(i_squared_normalized_deviations(_, _))

       for ((k,v) <- i_squared_normalized_deviations_csc.activeIterator) {
         val row = k._1
         val col = k._2
         sum = sum + i_squared_normalized_deviations_csc(row, col)
       }

       processed_denominators(i) = sum.toDouble
    }

    // Computing the processed normalized deviations
    for ((k,v) <- train.activeIterator) {

       val row = k._1
       val col = k._2

       // Processed rating
       processed_normalized_deviations(row, col) = processed_normalized_deviations(row, col)/math.sqrt(processed_denominators(row))
    }

    // Compute the matrix of similarities by multiplying the matrix of processed normalized deviations with its transpose
    var similarities = (processed_normalized_deviations * processed_normalized_deviations.t)        
    
    // Considering k = 100 and k = 200
    var MAE:Map[Int,Double] = Map()
    val kValues = Map(1 -> 100, 2 -> 200)
 
    for(k <- 1 to 2) {

       // Creating the similarity matrix for top k similarities, main user along rows
       val similaritiesTopKBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.users())
       
       // Select a row from the original similarity matrix
       for (i <- 0 to conf.users() - 1) {

          var user_specific_similarities = similarities(i to i, 0 to conf.users() - 1)
          user_specific_similarities(0,i) = 0
  
          // Selecting only the top k similarities for that user i
          for (j <- argtopk(user_specific_similarities,kValues(k))) {
             
             // The second user must not be equal to the first user
             if (j._2 != i) {
                
                // In this case add the similarity
                similaritiesTopKBuilder.add(i, j._2, user_specific_similarities(j))
             }
          }
       }


       // Build the final CSC matrix 
       var similaritiesTopK = similaritiesTopKBuilder.result()

       var copy_train = train.copy

       // To calculate the denominator of the user-specific weighted sum deviation, you need to set all the entries of this matrix to one
       for ((k,v) <- copy_train.activeIterator) {
          val row = k._1
          val col = k._2

          // processed rating
          copy_train(row, col) = 1
       }

       // Compute the predictions
       val predictions_builder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())
   
       for (i <- 0 to conf.users() - 1) {
          
          // Select the top k similarities for user i
          var user_specific_similarities = similaritiesTopK(i to i, 0 to conf.users() - 1)
          
          // Transform into a CSC matrix 
          val user_specific_similarities_csc = CSCMatrix.tabulate(user_specific_similarities.rows, user_specific_similarities.cols)(user_specific_similarities(_, _))
      
          // Take the product of that vector of similarities with the normalized deviations
          val vector_matrix_product = user_specific_similarities_csc * train
      
          // Converting to absolute value each entry of the CSC matrix
          for ((k,v) <- user_specific_similarities_csc.activeIterator) {
             val row = k._1
             val col = k._2

             user_specific_similarities_csc(row, col) = scala.math.abs(user_specific_similarities_csc(row, col))
          }

          val vector_matrix_product_2 = user_specific_similarities_csc * copy_train

          for ((k,v) <- vector_matrix_product.activeIterator) {
             val row = k._1
             val col = k._2
             val fraction = vector_matrix_product(row,col)/vector_matrix_product_2(row,col)
             val prediction = average_rating_user(row) + fraction * scale(average_rating_user(row) + fraction, average_rating_user(row))
  
             predictions_builder.add(i,col,prediction)
          }
       }
       
       var predictions = predictions_builder.result() 
      
       for ((k,v) <- test.activeIterator) {
          val row = k._1
          val col = k._2

          test(row, col) = scala.math.abs(predictions(row, col) - test(row, col))
       }   
   
       var n_nonzero = 0.0
       var final_sum = 0.0

       for ((k,v) <- test.activeIterator) {

          val row = k._1
          val col = k._2
          n_nonzero = n_nonzero + 1
          final_sum = final_sum + test(row, col)
       }    

       val mae = (final_sum/n_nonzero.toDouble).toDouble 
       MAE += (kValues(k) -> mae) 
    }

    // *** QUESTION 3.2.2 & 3.3.3 *** 

    println("Computing exercise 3.2.2 and 3.3.3")

    var timePrediction = new ArrayBuffer[Double]()
    var timeSimilarities = new ArrayBuffer[Double]()
    val k = conf_k

    for ( z <- 0 to 4) { 
      
       var t1_beginning = System.nanoTime
       
       // Average exist for all users and item, so store in a Dense matrix
       val average_rating_user_time = DenseVector.zeros[Double](conf.users())
       val average_rating_item_time = DenseVector.zeros[Double](conf.movies())

       // Find average rating for all users
       for( i <- 0 until conf.users()) {
 
          // Returns a slice matrix
          val i_training_row_time = train(i to i, 0 to (conf.movies() - 1))
      
          var sum_time = 0.0
          var counter_time = 0.0

          // Convert the slice matrix to a CSC matrix
          val i_training_row_csc_time = CSCMatrix.tabulate(i_training_row_time.rows, i_training_row_time.cols)(i_training_row_time(_, _))
        
          for ((k,v) <- i_training_row_csc_time.activeIterator) {
             val row = k._1
             val col = k._2
             sum_time = sum_time + i_training_row_csc_time(row, col)
             counter_time = counter_time + 1
          }       

       // Divide the sum of the elements by the number of elements
       average_rating_user_time(i) = (sum_time/counter_time.toDouble).toDouble
       }

       // Find average for all items
       for (i <- 0 until conf.movies()) {

          // Returns a slice matrix 
          val i_training_column_time = train(0 to (conf.users() - 1), i to i)

          var sum_time = 0.0
          var counter_time = 0.0

          // Convert the slice matrix to a CSC matrix
          val i_training_column_csc_time = CSCMatrix.tabulate(i_training_column_time.rows, i_training_column_time.cols)(i_training_column_time(_, _))

          for ((k,v) <- i_training_column_csc_time.activeIterator) {
             val row = k._1
             val col = k._2
             sum_time = sum_time + i_training_column_csc_time(row, col)
             counter_time = counter_time + 1
          }
       
          // Divide the sum of the elements by the number of elements
          average_rating_item_time(i) = (sum_time/counter_time.toDouble).toDouble
       }
    
       val intermediate_time = train.copy

       // Computing the normalized deviations, result stored in train so as to save memory
       for ((k,v) <- train.activeIterator) {
          val row = k._1
          val col = k._2

          train(row, col) = (train(row, col) - average_rating_user_time(row))/scale(train(row,col), average_rating_user_time(row))
          intermediate_time(row, col) = train(row, col)*train(row, col)
       }
    
       val processed_denominators_time = DenseVector.zeros[Double](conf_users)

       val processed_normalized_deviations_time = train.copy

       // Computing the denominators
       for( i <- 0 until conf_users) {

          val i_squared_normalized_deviations_time = intermediate_time(i to i, 0 to conf_movies - 1)

          var sum_time = 0.0

          // Convert the slice matrix to a CSC matrix
          val i_squared_normalized_deviations_csc_time = CSCMatrix.tabulate(i_squared_normalized_deviations_time.rows, i_squared_normalized_deviations_time.cols)(i_squared_normalized_deviations_time(_, _))

          for ((k,v) <- i_squared_normalized_deviations_csc_time.activeIterator) {
             val row = k._1
             val col = k._2
             sum_time = sum_time + i_squared_normalized_deviations_csc_time(row, col)
          }

          processed_denominators_time(i) = sum_time.toDouble
       }

       // Computing the processed normalized deviations
       for ((k,v) <- train.activeIterator) {

          val row = k._1
          val col = k._2

          processed_normalized_deviations_time(row, col) = processed_normalized_deviations_time(row, col)/math.sqrt(processed_denominators_time(row))
       }

       // Compute the matrix of similarities by multiplying the matrix of processed normalized deviations with its transpose
       var similarities_time = (processed_normalized_deviations_time * processed_normalized_deviations_time.t) 

       // Creating the similarity matrix for top k similarities, main user along rows
       val similaritiesTopKBuilderTime = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.users())
       
       // Select a row from the original similarity matrix
       for (i <- 0 to conf.users() - 1) {

          var user_specific_similarities_time = similarities_time(i to i, 0 to conf.users() - 1)
          user_specific_similarities_time(0,i) = 0
  
          // Selecting only the top k similarities for that user i
          for (j <- argtopk(user_specific_similarities_time,k)) {
             
             // The second user must not be equal to the first user
             if (j._2 != i) {
                
                // In this case add the similarity
                similaritiesTopKBuilderTime.add(i, j._2, user_specific_similarities_time(j))
             }
          }
       }

       // Build the final CSC matrix 
       var similaritiesTopKTime = similaritiesTopKBuilderTime.result()

       val t2_similarities = System.nanoTime
       timeSimilarities += t2_similarities - t1_beginning

       var copy_train_time = train.copy

       // To calculate the denominator of the user-specific weighted sum deviation, you need to set all the entries of this matrix to one
       for ((k,v) <- copy_train_time.activeIterator) {
          val row = k._1
          val col = k._2

          // processed rating
          copy_train_time(row, col) = 1
       }

       // Compute the predictions
       val predictions_builder_time = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())
   
       for (i <- 0 to conf.users() - 1) {
          
          // Select the top k similarities for user i
          var user_specific_similarities_time = similaritiesTopKTime(i to i, 0 to conf.users() - 1)
          
          // Transform into a CSC matrix 
          val user_specific_similarities_csc_time = CSCMatrix.tabulate(user_specific_similarities_time.rows, user_specific_similarities_time.cols)(user_specific_similarities_time(_, _))
      
          // Take the product of that vector of similarities with the normalized deviations
          val vector_matrix_product_time = user_specific_similarities_csc_time * train
      
          // Converting to absolute value each entry of the CSC matrix
          for ((k,v) <- user_specific_similarities_csc_time.activeIterator) {
             val row = k._1
             val col = k._2

             user_specific_similarities_csc_time(row, col) = scala.math.abs(user_specific_similarities_csc_time(row, col))
          }

          val vector_matrix_product_2_time = user_specific_similarities_csc_time * copy_train_time

          for ((k,v) <- vector_matrix_product_time.activeIterator) {
             val row = k._1
             val col = k._2
             val fraction_time = vector_matrix_product_time(row,col)/vector_matrix_product_2_time(row,col)
             val prediction_time = average_rating_user_time(row) + fraction_time * scale(average_rating_user_time(row) + fraction_time, average_rating_user_time(row))
  
             predictions_builder_time.add(i,col,prediction_time)
          }
       }
       
       var predictions_time = predictions_builder_time.result() 
      
       val t2_prediction = System.nanoTime

       timePrediction += t2_prediction - t1_beginning
    }

    
    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {
        var json = "";
        {
          // Limiting the scope of implicit formats with {}
          implicit val formats = org.json4s.DefaultFormats

          val answers: Map[String, Any] = Map(
            "Q3.3.1" -> Map(
              "MaeForK=100" -> MAE(100), // Datatype of answer: Double
              "MaeForK=200" -> MAE(200)  // Datatype of answer: Double
            ),
            "Q3.3.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> Map(
                "min" -> timeSimilarities.min/1000,  // Datatype of answer: Double
                "max" -> timeSimilarities.max/1000, // Datatype of answer: Double
                "average" -> timeSimilarities.sum/5000, // Datatype of answer: Double
                "stddev" -> calculateSD(timeSimilarities) // Datatype of answer: Double
              )
            ),
            "Q3.3.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> Map(
                "min" -> timePrediction.min/1000,  // Datatype of answer: Double
                "max" -> timePrediction.max/1000, // Datatype of answer: Double
                "average" -> timePrediction.sum/5000, // Datatype of answer: Double
                "stddev" -> calculateSD(timePrediction) // Datatype of answer: Double
              )
            )
            // Answer the Question 3.3.4 exclusively on the report.
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}
