package similarity

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

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")
 
  
  // *** QUESTION 2.3.1 ***
 
  println("Computing exercise 2.3.1")
 
  var timePrediction = new ArrayBuffer[Double]()
  var timeSimilarities = new ArrayBuffer[Double]()

  val t1_beginning = System.nanoTime

  // RDD[(user, average_rating_user)]
  val groupedUsers = train.groupBy(x => x.user).map{ x => (x._1, x._2.toList.map(r => r.rating).reduce(_ + _) / x._2.toList.size) }.persist()

  // RDD[(item, average_rating_item)]
  val groupedItems = train.groupBy(x => x.item).map{ x => (x._1, x._2.toList.map(r => r.rating).reduce(_ + _) / x._2.toList.size) }

  // RDD[(user, item, rating, average_rating_user, average_rating_item)]
  val temp = test.map(r => (r.user, r.item, r.rating)).keyBy{t => t._1}.join(groupedUsers.keyBy{t => t._1}).map { 
    case (user, ((_, item, rating), (_, average_rating_user))) => (user, item, rating, average_rating_user) 
  }.persist()
  val averagesRDD = temp.keyBy{t => (t._2)}.join(groupedItems.keyBy{t => t._1}).map {
    case (item, ((user, _, rating, average_rating_user), (_, average_rating_item))) => (user, item, rating, average_rating_user, average_rating_item)
  }.persist()
  
  // RDD[(user, item, rating, average_rating_user, average_rating_item, normalized_rating)]
  val normalizedRDD = averagesRDD.map {
    case (user, item, rating, average_rating_user, average_rating_item) => (user, item, rating, average_rating_user, average_rating_item, (rating - average_rating_user) / (scale(rating, average_rating_user)))
  }.persist()

  // RDD[(user, item, normalized_rating)]
  val normalizedRatings = normalizedRDD.map{x => (x._1, x._2, x._6)}.persist()
  
  // RDD[(user, normalized_ratings squared and summed for all items rated by the user)]
  val groupedNormalizedDeviationsRDD = normalizedRDD.groupBy(x => x._1).map { x => (x._1, x._2.toList.map(r => r._6 * r._6).reduce(_ + _)) }

  // RDD[(user, item, processed_rating, rating, average_rating_user)]
  val processedNormalizedDeviationsRDD = normalizedRDD.keyBy { _._1 }.join(groupedNormalizedDeviationsRDD.keyBy { t => t._1 }).map {
    case (user, ((_, item, rating, average_rating_user, average_rating_item, normalized_rating), (_, processed_denominator)))
    => (user, item, normalized_rating / (math.sqrt(processed_denominator)), rating, average_rating_user)
  }.persist()
 
  val intermediate = processedNormalizedDeviationsRDD.map{r => (r._2, r._1, r._3)}.keyBy{_._1}
  val shared =  intermediate.join(intermediate)
  val modified = shared.map{
    case (item, ((_, user1, processed_rating1), (_, user2, processed_rating2))) => ((user1, user2), processed_rating1*processed_rating2)
  }
  val similarities = modified.reduceByKey(_+_).filter{x => x._1._1 != x._1._2}.persist()

  val t2_similarities = System.nanoTime
  timeSimilarities += t2_similarities - t1_beginning

  val numeratorDenominatorRDD = similarities.keyBy{x => x._1._2}.join(normalizedRatings.keyBy{x => x._1})
    
  val userSpecificWeightedSumDeviationsRDD = numeratorDenominatorRDD.map{ 
    case (user2, (((user1, _), similarity), (_, item, normalized_rating))) => ((user1, item), (similarity*normalized_rating, scala.math.abs(similarity))) 
  }.reduceByKey(
    (x,y) => (x._1 + y._1, x._2 + y._2)
  ).map{x => (x._1._1, x._1._2, x._2._1/x._2._2)}
 
  val gatheredDataRDD = processedNormalizedDeviationsRDD.keyBy{ t => (t._1, t._2) }.join(userSpecificWeightedSumDeviationsRDD.keyBy{ t => (t._1, t._2) })
   
  // RDD[(user, item, rating, baseline_acs, average_rating_user)]
  val baselineAcsRDD = gatheredDataRDD.map {
    case ((user, item), ((_, _, _, rating, average_rating_user), (_, _, weightedSumDeviation)))
    => (user, item, rating, average_rating_user + weightedSumDeviation * scale((average_rating_user + weightedSumDeviation), average_rating_user))
  }.persist()
     
  val t2_prediction = System.nanoTime

  val maeBaselineAcs = baselineAcsRDD.map(r => scala.math.abs(r._3.toDouble - r._4.toDouble)).filter(!_.isNaN).reduce(_ + _).toDouble/baselineAcsRDD.filter(r => ! r._4.isNaN).count.toDouble

  timePrediction += t2_prediction - t1_beginning

  val maeBaseline = 0.7669
  val difference1 = maeBaselineAcs - maeBaseline 
 
  // *** QUESTION 2.3.2 ***

  println("Computing exercise 2.3.2")

  // RDD[(user, Set(movies seend by user))]
  val groupedForJaccard = test.map{r => (r.user, r.item)}.groupBy(r => r._1).map(x => (x._1, x._2.map(y => y._2).toSet))
  val cartesianJaccard = groupedForJaccard.cartesian(groupedForJaccard)

  val jaccardSimilarities = cartesianJaccard.map{
    case ((user1, set1), (user2, set2)) => ((user1, user2), jaccard(set1, set2), intersection(set1, set2))
  }

  val numeratorDenominatorJaccardRDD = jaccardSimilarities.map{x => ((x._1._1, x._1._2), x._2)}.keyBy{x => x._1._2}.join(normalizedRatings.keyBy{x => x._1})

  val userSpecificWeightedSumDeviationsJaccardRDD = numeratorDenominatorJaccardRDD.map{
    case (user2, (((user1, _), similarity), (_, item, normalized_rating))) => ((user1, item), (similarity*normalized_rating, scala.math.abs(similarity)))
  }.reduceByKey(
    (x,y) => (x._1 + y._1, x._2 + y._2)
  ).map{x => (x._1._1, x._1._2, x._2._1/x._2._2)}

  val gatheredDataJaccardRDD = processedNormalizedDeviationsRDD.keyBy{ t => (t._1, t._2) }.join(userSpecificWeightedSumDeviationsJaccardRDD.keyBy{ t => (t._1, t._2) })

  val baselineJaccardRDD = gatheredDataJaccardRDD.map {
    case ((user, item), ((_, _, _, rating, average_rating_user), (_, _, weightedSumDeviation)))
    => (user, item, rating, average_rating_user + weightedSumDeviation * scale((average_rating_user + weightedSumDeviation), average_rating_user))
  }.persist()

  // Calculate the MAE for the Jaccard baseline
  val maeBaselineJaccard = baselineJaccardRDD.map(r => scala.math.abs(r._3.toDouble - r._4.toDouble)).filter(!_.isNaN).reduce(_ + _).toDouble/baselineJaccardRDD.filter(r => ! r._4.isNaN).count.toDouble

  val difference2 = maeBaselineJaccard - maeBaselineAcs
 
  // *** EXERCISE 2.3.3 ***
  
  println("Computing exercise 2.3.3")
  val numberUsers = groupedUsers.count.toInt 
  val numberSimilarityComputations = numberUsers*(numberUsers - 1)/2

  // *** EXERCISE 2.3.4 ****

  println("Computing exercise 2.3.4")
  val numberComputations = jaccardSimilarities.map{x => x._3}.persist()
  val max = numberComputations.max
  val min = numberComputations.min
  val mean = numberComputations.reduce(_+_)/numberComputations.count
  val std = scala.math.abs{numberComputations.map{x => (x-mean)*(x-mean)}.reduce(_+_)/numberComputations.count}

  // *** EXERCISE 2.3.5 ***

  println("Computing exercise 2.3.5")
  val numberBytes = 8*numberSimilarityComputations

  // *** EXERCISE 2.3.6 AND 2.3.7 ****
  
  println("Computing exercise 2.3.6 and 2.3.7")

  for(x <- 0 to 3){
     val t1_beginning_time = System.nanoTime
     
     // RDD[(user, average_rating_user)]
     val groupedUsersTime = train.groupBy(x => x.user).map{ x => (x._1, x._2.toList.map(r => r.rating).reduce(_ + _) / x._2.toList.size) }.persist()

     // RDD[(item, average_rating_item)]
     val groupedItemsTime = train.groupBy(x => x.item).map{ x => (x._1, x._2.toList.map(r => r.rating).reduce(_ + _) / x._2.toList.size) }

     // RDD[(user, item, rating, average_rating_user, average_rating_item)]
     val tempTime = test.map(r => (r.user, r.item, r.rating)).keyBy{t => t._1}.join(groupedUsersTime.keyBy{t => t._1}).map {
       case (user, ((_, item, rating), (_, average_rating_user))) => (user, item, rating, average_rating_user)
     }.persist()
     val averagesRDDTime = tempTime.keyBy{t => (t._2)}.join(groupedItemsTime.keyBy{t => t._1}).map {
       case (item, ((user, _, rating, average_rating_user), (_, average_rating_item))) => (user, item, rating, average_rating_user, average_rating_item)
     }.persist()

     // RDD[(user, item, rating, average_rating_user, average_rating_item, normalized_rating)]
     val normalizedRDDTime = averagesRDDTime.map {
        case (user, item, rating, average_rating_user, average_rating_item) 
        => (user, item, rating, average_rating_user, average_rating_item, (rating - average_rating_user) / (scale(rating, average_rating_user)))
     }.persist()

     // RDD[(user, item, normalized_rating)]
     val normalizedRatingsTime = normalizedRDDTime.map{x => (x._1, x._2, x._6)}.persist()

     // RDD[(user, normalized_ratings squared and summed for all items rated by the user)]
     val groupedNormalizedDeviationsRDDTime = normalizedRDDTime.groupBy(x => x._1).map { x => (x._1, x._2.toList.map(r => r._6 * r._6).reduce(_ + _)) }

     // RDD[(user, item, processed_rating, rating, average_rating_user)]
     val processedNormalizedDeviationsRDDTime = normalizedRDDTime.keyBy { _._1 }.join(groupedNormalizedDeviationsRDDTime.keyBy { t => t._1 }).map {
       case (user, ((_, item, rating, average_rating_user, average_rating_item, normalized_rating), (_, processed_denominator)))
       => (user, item, normalized_rating / (math.sqrt(processed_denominator)), rating, average_rating_user)
     }.persist()     
    
     val intermediateTime = processedNormalizedDeviationsRDDTime.map{r => (r._2, r._1, r._3)}.keyBy{_._1}
     val sharedTime =  intermediateTime.join(intermediateTime)
      
     val modifiedTime = sharedTime.map{
        case (item, ((_, user1, processed_rating1), (_, user2, processed_rating2))) => ((user1, user2), processed_rating1*processed_rating2)
     }
     val similaritiesTime = modifiedTime.reduceByKey(_+_).filter{x => x._1._1 != x._1._2}.persist()

     val t2_similarities_time = System.nanoTime
     
     val numeratorDenominatorRDDTime = similaritiesTime.keyBy{x => x._1._2}.join(normalizedRatingsTime.keyBy{x => x._1})

     val userSpecificWeightedSumDeviationsRDDTime = numeratorDenominatorRDDTime.map{
        case (user2, (((user1, _), similarity), (_, item, normalized_rating))) => ((user1, item), (similarity*normalized_rating, scala.math.abs(similarity)))
     }.reduceByKey(
        (x,y) => (x._1 + y._1, x._2 + y._2)
     ).map{x => (x._1._1, x._1._2, x._2._1/x._2._2)}

     val gatheredDataRDDTime = processedNormalizedDeviationsRDDTime.keyBy{ t => (t._1, t._2) }.join(userSpecificWeightedSumDeviationsRDDTime.keyBy{ t => (t._1, t._2) })
     
     val baselineAcsRDDTime = gatheredDataRDDTime.map {
       case ((user, item), ((_, _, _, rating, average_rating_user), (_, _, weightedSumDeviation)))
     => (user, item, rating, average_rating_user + weightedSumDeviation * scale((average_rating_user + weightedSumDeviation), average_rating_user))
     }.persist()

     val t2_prediction_time = System.nanoTime

     val maeBaselineAcsTime = baselineAcsRDDTime.map(r => scala.math.abs(r._3.toDouble - r._4.toDouble)).filter(!_.isNaN).reduce(_ + _).toDouble/baselineAcsRDDTime.filter(r => ! r._4.isNaN).count.toDouble
     
     timePrediction += t2_prediction_time - t1_beginning_time
     timeSimilarities += t2_similarities_time - t1_beginning_time

     groupedUsersTime.unpersist()
     tempTime.unpersist()
     averagesRDDTime.unpersist()
     normalizedRDDTime.unpersist()
     normalizedRatingsTime.unpersist()
     processedNormalizedDeviationsRDDTime.unpersist()
     similaritiesTime.unpersist()
     baselineAcsRDDTime.unpersist()    
  } 
 
  // *** FUNCTIONS ***
 
  // Scale function
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

  def jaccard(a: Set[Int], b: Set[Int]): Double =  {
       val c = a.intersect(b)
       val d = a.union(b)
       val numerator = c.size.toDouble
       val denominator = d.size.toDouble
       
       if(denominator == 0){
          0
       } else {
          numerator/denominator
       }           
  } 

  def intersection(a : Set[Int], b : Set[Int]) : Int = {
     a.intersect(b).size.toInt
  }

  def calculateSD(arrayTime : ArrayBuffer[Double]) : Double = {
     val mean = arrayTime.sum/5
     var arrayOfTerms = new ArrayBuffer[Double]()
     for ( i <- 0 to 4) {
         arrayOfTerms += (arrayTime(i)-mean)*(arrayTime(i)-mean)/(1000*1000)
     }
     return math.sqrt(arrayOfTerms.sum/5)
  }

  // println(timePrediction.mkString(" "))
  // println(timeSimilarities.mkString(" "))

  // *** Save answers as JSON ***
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
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> maeBaselineAcs, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> difference1 // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> maeBaselineJaccard, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> difference2 // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> numberSimilarityComputations // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> min,  // Datatype of answer: Double
              "max" -> max, // Datatype of answer: Double
              "average" -> mean, // Datatype of answer: Double
              "stddev" -> std // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> numberBytes // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> timePrediction.min/1000, // Datatype of answer: Double
              "max" -> timePrediction.max/1000, // Datatype of answer: Double
              "average" -> timePrediction.sum/5000, // Datatype of answer: Double
              "stddev" -> calculateSD(timePrediction) // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> timeSimilarities.min/1000,  // Datatype of answer: Double
              "max" -> timeSimilarities.max/1000, // Datatype of answer: Double
              "average" -> timeSimilarities.sum/5000, // Datatype of answer: Double
              "stddev" -> calculateSD(timeSimilarities) // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> timeSimilarities.sum/5000/numberSimilarityComputations, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> timeSimilarities.sum/timePrediction.sum // Datatype of answer: Double
          )
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
