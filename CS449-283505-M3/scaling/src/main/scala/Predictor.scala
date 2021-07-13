import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkEnv
import org.apache.spark.broadcast

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
    var conf = new Conf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- trainFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        trainBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0,9)) + "s")

    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()
       
    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close

   
    // --- Functions ---
 
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
    
    // --- QUESTION 4.1.1 ---
  
    val beginning_time = System.nanoTime()
             
    // Average exist for all users and item, so store in a Dense matrix
    val average_rating_user = DenseVector.zeros[Double](conf_users)
    val average_rating_item = DenseVector.zeros[Double](conf_movies)
    
    println("Computing the average user ratings")

    // Find average rating for all users
    for( i <- 0 until conf_users) {

       // print("Average rating for user " + i)
       
       // Returns a slice matrix
       val user_specific_training_row = train(i to i, 0 to (conf_movies - 1))
       val i_training_row_csc = CSCMatrix.tabulate(user_specific_training_row.rows, user_specific_training_row.cols)(user_specific_training_row(_,_))
      
       var sum = 0.0
       var counter = 0.0

       // Convert the slice matrix to a CSC matrix
       // val i_training_row_csc = CSCMatrix.tabulate(i_training_row.rows, i_training_row.cols)(i_training_row(_, _))
        
       for ((k,v) <- i_training_row_csc.activeIterator) {
         val row = k._1
         val col = k._2
         sum = sum + i_training_row_csc(row, col)
         counter = counter + 1
       }       

       // Divide the sum of the elements by the number of elements
       average_rating_user(i) = (sum/counter.toDouble).toDouble
    }

    println("Computing the average movie ratings")

    // Find average for all items
    for (i <- 0 until conf_movies) {
       
       // println("Average rating for movie " + i)
       // Returns a slice matrix 
       val user_specific_training_column = train(0 to (conf_users - 1), i to i)
       val i_training_column_csc = CSCMatrix.tabulate(user_specific_training_column.rows, user_specific_training_column.cols)(user_specific_training_column(_,_))

       var sum = 0.0
       var counter = 0.0

       // Convert the slice matrix to a CSC matrix
       // val i_training_column_csc = CSCMatrix.tabulate(i_training_column.rows, i_training_column.cols)(i_training_column(_, _))

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
 
    println("Computing the normalized deviations")

    // Computing the normalized deviations, result stored in train so as to save memory
    for ((k,v) <- train.activeIterator) {
       val row = k._1
       val col = k._2
       
       // println("Computing the normalized deviations for user " + row + " and movie " + col)
       
       train(row, col) = (train(row, col) - average_rating_user(row))/scale(train(row,col), average_rating_user(row))
       intermediate(row, col) = train(row, col)*train(row, col)
    }
    
    val processed_denominators = DenseVector.zeros[Double](conf_users)
    
    val processed_normalized_deviations = train.copy
   
    println("Computing the processed denominators")

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
     
    println("Computing the processed normalized deviations")

    // Computing the processed normalized deviations  
    for ((k,v) <- train.activeIterator) {

       val row = k._1
       val col = k._2

       // Processed rating
       processed_normalized_deviations(row, col) = processed_normalized_deviations(row, col)/math.sqrt(processed_denominators(row))
    }  

    println("Computing the similarities - takes several minutes") 
   
    // We broadcast the processed_normalized_deviations onto all the executors 
    val broadcast_processed_normalized_deviations = sc.broadcast(processed_normalized_deviations)
    
    def topk(user : Int) : (Int, IndexedSeq[(Int, Double)]) = {
      
       // println("Computing the similarities for user " + user)
    
       val processed_normalized_deviations_broadcasted = broadcast_processed_normalized_deviations.value
     
       // Find the vector of similarities just for user u
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
    
    val similarities_time = System.nanoTime() - beginning_time
    
    println("Computing the predictions - takes several minutes")
   
    val predictions_builder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())
    val broadcast_similaritiesTopK = sc.broadcast(similaritiesTopK)
    val broadcast_train = sc.broadcast(train) 
    val broadcast_average_rating_user = sc.broadcast(average_rating_user) 
    
    def prediction(user : Int, item : Int) : Double = {

       // println("Computing the prediction for user " + user + " and movie " + item)

       val similaritiesTopK_broadcasted = broadcast_similaritiesTopK.value
       val train_broadcasted = broadcast_train.value
       val average_rating_user_broadcasted = broadcast_average_rating_user.value

       // Select the top k similarities for user i
       var user_specific_similarities = similaritiesTopK_broadcasted(user to user, 0 to conf_users - 1)

       // Transform into a CSC matrix
       var user_specific_similarities_csc = CSCMatrix.tabulate(user_specific_similarities.rows, user_specific_similarities.cols)(user_specific_similarities(_, _))

       // Select the column of interest
       val column_train = train_broadcasted(0 to conf_users - 1, item to item)

       // Transform into a CSC matrix
       var column_train_csc = CSCMatrix.tabulate(column_train.rows, column_train.cols)(column_train(_, _))

       // Take the product of that vector of similarities with the normalized deviations
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

    val prediction_time = System.nanoTime() - beginning_time
    
    println("Computing the MAE")

    for ((k,v) <- test.activeIterator) {
       val row = k._1
       val col = k._2
        
       val toAdd = scala.math.abs(predictions(row, col) - test(row, col))
        
       if (!toAdd.isNaN) {        
          test(row, col) = toAdd
       } else {
          // Set to some negative number
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
            "Q4.1.1" -> Map(
              "MaeForK=200" -> mae  // Datatype of answer: Double
            ),
            // Both Q4.1.2 and Q4.1.3 should provide measurement only for a single run
            "Q4.1.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> similarities_time/1000  // Datatype of answer: Double
            ),
            "Q4.1.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> prediction_time/1000 // Datatype of answer: Double  
            )
            // Answer the other questions of 4.1.2 and 4.1.3 in your report
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  } 
}

