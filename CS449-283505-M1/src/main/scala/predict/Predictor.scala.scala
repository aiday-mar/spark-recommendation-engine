package predict

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  println("Loading training data from: " + conf.train()) 
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(train.count == 80000, "Invalid training data")
  // training data is above

  println("Loading test data from: " + conf.test()) 
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(test.count == 20000, "Invalid test data")
  // test data is above


  // *** QUESTION 3.1.4 ***

  val globalPred = train.map(r => r.rating).reduce(_+_)/train.count
  val maeGlobal = test.map(r => scala.math.abs(r.rating - globalPred)).reduce(_+_)/test.count.toDouble
   
  val groupedUsers = train.groupBy(x => x.user).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
  val groupedItems = train.groupBy(x => x.item).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
  
  // join test with groupedUsers and groupedItems
  val temp = test.map(r => (r.user, r.item, r.rating)).keyBy{t => t._1}.join(groupedUsers.keyBy{t => t._1}).map { 
    case (user, ((_, item, rating), (_, average_rating_user))) => (user, item, rating, average_rating_user) 
  }
  val averagesRDD = temp.keyBy{t => (t._2)}.join(groupedItems.keyBy{t => t._1}).map {
    case (item, ((user, _, rating, average_rating_user), (_, average_rating_item))) => (user, item, rating, average_rating_user, average_rating_item)
  } 
  
  // MAE from user-base predictions and item based predictions 
  val maePerUser = averagesRDD.map(r => scala.math.abs(r._3 - r._4)).reduce(_+_)/averagesRDD.count.toDouble 
  val maePerItem = averagesRDD.map(r => scala.math.abs(r._3 - r._5)).reduce(_+_)/averagesRDD.count.toDouble
  
  // including normalized deviation
  val normalizedRDD = averagesRDD.map{ 
     case (user, item, rating, average_rating_user, average_rating_item) => (user, item, rating, average_rating_user, average_rating_item, (rating - average_rating_user)/(scale(rating, average_rating_user)))
  }  
  
  // finding the averge normalized deviation per item
  val groupedNormalizedDeviations = normalizedRDD.groupBy(x => x._2).map{x => (x._1, x._2.toList.map(r => r._6).reduce(_+_)/x._2.toList.size)} 
  
  // gathering all the data into one RDD and finally calculating the proposed baseline of equation (5)
  val baselineRDD =  averagesRDD.keyBy{t => (t._2)}.join(groupedNormalizedDeviations.keyBy{t => t._1}).map{
    case (item, ((user, _, rating, average_rating_user, average_rating_item), (_, global_deviation_item))) => (user, item, rating, average_rating_user + global_deviation_item*scale((average_rating_user + global_deviation_item),average_rating_user))
  }
  
  // mae for the proposed baseline 
  val maeBaseline = baselineRDD.map(r => scala.math.abs(r._3 - r._4)).reduce(_+_)/baselineRDD.count.toDouble

  def scale(rating : Double, ave_rating : Double) : Double = {
     if (rating > ave_rating) {return 5 - ave_rating}
     else if (rating < ave_rating) {return ave_rating - 1}
     else { return 1}
  }
   
  
  // *** QUESTION 3.1.5 ****
  var timeGlobalPrediction = new ArrayBuffer[Double]()
  var timePerUserPrediction = new ArrayBuffer[Double]()
  var timePerItemPrediction = new ArrayBuffer[Double]()
  var timeBaselinePrediction = new ArrayBuffer[Double]()
  
  for(x <- 0 to 9){
     val t1 = System.nanoTime
     val globalPredTime = train.map(r => r.rating).reduce(_+_)/train.count
     val t2 = System.nanoTime
     timeGlobalPrediction += t2 - t1
  }  

  for(x <- 0 to 9){
     val t1 = System.nanoTime
     val groupedUsers = train.groupBy(x => x.user).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
     val userRDDTime = test.map(r => (r.user, r.item, r.rating)).keyBy{t => t._1}.join(groupedUsers.keyBy{t => t._1}).map {
       case (user, ((_, item, rating), (_, average_rating_user))) => (user, item, rating, average_rating_user)
     }
     val t2 = System.nanoTime
     timePerUserPrediction += t2 - t1
  }

  for(x <- 0 to 9){
     val t1 = System.nanoTime
     val groupedItems = train.groupBy(x => x.item).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
     val itemRDDTime = test.map(r => (r.user, r.item, r.rating)).keyBy{t => t._2}.join(groupedItems.keyBy{t => t._1}).map {
       case (item, ((user, _, rating), (_, average_rating_item))) => (user, item, rating, average_rating_item)
     }
     val t2 = System.nanoTime
     timePerItemPrediction += t2 - t1
  }
  
  for(x <- 0 to 9){
     val t1 = System.nanoTime
     
     val groupedUsers = train.groupBy(x => x.user).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
     val groupedItems = train.groupBy(x => x.item).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}

     // join test with groupedUsers and groupedItems
     val temp = test.map(r => (r.user, r.item, r.rating)).keyBy{t => t._1}.join(groupedUsers.keyBy{t => t._1}).map {
        case (user, ((_, item, rating), (_, average_rating_user))) => (user, item, rating, average_rating_user)
     }
     val averagesRDD = temp.keyBy{t => (t._2)}.join(groupedItems.keyBy{t => t._1}).map {
        case (item, ((user, _, rating, average_rating_user), (_, average_rating_item))) => (user, item, rating, average_rating_user, average_rating_item)
     }

     // MAE from user-base predictions and item based predictions
     val maePerUser = averagesRDD.map(r => scala.math.abs(r._3 - r._4)).reduce(_+_)/averagesRDD.count.toDouble
     val maePerItem = averagesRDD.map(r => scala.math.abs(r._3 - r._5)).reduce(_+_)/averagesRDD.count.toDouble

     // including normalized deviation
     val normalizedRDD = averagesRDD.map{
       case (user, item, rating, average_rating_user, average_rating_item) => (user, item, rating, average_rating_user, average_rating_item, (rating - average_rating_user)/(scale(rating, average_rating_user)))
     } 

     // finding the averge normalized deviation per item
     val groupedNormalizedDeviations = normalizedRDD.groupBy(x => x._2).map{x => (x._1, x._2.toList.map(r => r._6).reduce(_+_)/x._2.toList.size)}

     // gathering all the data into one RDD and finally calculating the proposed baseline of equation (5)
     val baselineRDD =  averagesRDD.keyBy{t => (t._2)}.join(groupedNormalizedDeviations.keyBy{t => t._1}).map{
       case (item, ((user, _, rating, average_rating_user, average_rating_item), (_, global_deviation_item))) => (user, item, rating, average_rating_user + global_deviation_item*scale((average_rating_user + global_deviation_item),average_rating_user))
     }

     val t2 = System.nanoTime
     timeBaselinePrediction += t2 - t1
  }
  
  def calculateSD(arrayTime : ArrayBuffer[Double]) : Double = {
     val mean = arrayTime.sum/10
     var arrayOfTerms = new ArrayBuffer[Double]()
     for ( i <- 0 to 9) {
         arrayOfTerms += (arrayTime(i)-mean)*(arrayTime(i)-mean)/(1000*1000)
     }
     return math.sqrt(arrayOfTerms.sum/10)
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
            "Q3.1.4" -> Map(
              "MaeGlobalMethod" -> maeGlobal,
              "MaePerUserMethod" -> maePerUser,
              "MaePerItemMethod" -> maePerItem, 
              "MaeBaselineMethod" -> maeBaseline
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> timeGlobalPrediction.min/1000, 
                "max" -> timeGlobalPrediction.max/1000, 
                "average" -> timeGlobalPrediction.sum/10000,
                "stddev" -> calculateSD(timeGlobalPrediction),
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> timePerUserPrediction.min/1000, 
                "max" -> timePerUserPrediction.max/1000, 
                "average" -> timePerUserPrediction.sum/10000,
                "stddev" -> calculateSD(timePerUserPrediction),
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> timePerItemPrediction.min/1000, 
                "max" -> timePerItemPrediction.max/1000,
                "average" -> timePerItemPrediction.sum/10000,
                "stddev" -> calculateSD(timePerItemPrediction),
             ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> timeBaselinePrediction.min/1000,
                "max" -> timeBaselinePrediction.max/1000,
                "average" -> timeBaselinePrediction.sum/10000,
                "stddev" -> calculateSD(timeBaselinePrediction),
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> (timeBaselinePrediction.sum/10)/(timeGlobalPrediction.sum/10)
            ),
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
