package knn

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.language.implicitConversions
import scala.reflect.ClassTag
import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}
import scala.collection.JavaConverters._
import scala.collection.generic.Growable


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
  // We don't consider the case when the users are equal
  val similarities = modified.reduceByKey(_+_).map{ x => (x._1._1, x._1._2, x._2)}.filter{x => x._1 != x._2}.keyBy{t => t._1}

  // k = 10, 30, 50, 100, 200, 300, 400, 800, 943
  
  var MAE:Map[Int,Double] = Map()
  val kValues = Map(1 -> 10, 2 -> 30, 3 -> 50, 4 -> 100, 5 -> 200, 6 -> 300, 7 -> 400, 8 -> 800, 9 -> 943)
  var numberBytesMap:Map[Int, Int] = Map()  

  for ( i <- 1 until 10) {
    
     println("Computing exercise 3.2.1 for k = " + kValues(i))
 
     // val filteredSimilarities = similarities.topByKey(kValues(i))(Ordering.by(_._3)).flatMap(_._2)
     val filteredSimilarities = topByKey(similarities, kValues(i))(Ordering.by(_._3)).flatMap(_._2)

     val numberBytes = (8*filteredSimilarities.count).toInt
     numberBytesMap += (kValues(i) -> numberBytes)     

     val numeratorDenominatorRDD = filteredSimilarities.keyBy{x => x._2}.join(normalizedRatings.keyBy{x => x._1})
  
     val userSpecificWeightedSumDeviationsRDD = numeratorDenominatorRDD.map{ 
        case (user2, ((user1, _, similarity), (_, item, normalized_rating))) => ((user1, item), (similarity*normalized_rating, scala.math.abs(similarity))) 
     }.reduceByKey(
       (x,y) => (x._1 + y._1, x._2 + y._2)
     ).map{x => (x._1._1, x._1._2, x._2._1/x._2._2)}

     val gatheredDataRDD = processedNormalizedDeviationsRDD.keyBy{ t => (t._1, t._2) }.join(userSpecificWeightedSumDeviationsRDD.keyBy{ t => (t._1, t._2) })
  
     // RDD[(user, item, rating, baseline_acs, average_rating_user)]
     val baselineAcsRDD = gatheredDataRDD.map {
        case ((user, item), ((_, _, _, rating, average_rating_user), (_, _, weightedSumDeviation)))
     => (user, item, rating, average_rating_user + weightedSumDeviation * scale((average_rating_user + weightedSumDeviation), average_rating_user))
     }.persist()

     // Calculate the MAE for the proposed baseline 
     val maeBaselineAcs = baselineAcsRDD.map(r => scala.math.abs(r._3.toDouble - r._4.toDouble)).filter(!_.isNaN).reduce(_ + _).toDouble/baselineAcsRDD.filter(r => ! r._4.isNaN).count.toDouble

     MAE += (kValues(i) -> maeBaselineAcs)  
  }
 
  println("Computing exercise 2.3.1, finding the lowest K with better mae than baseline")
 
  val maeBaseline = 0.7669
  var smallestK = 1
  var smallestBaseline =  1.0

  while (smallestBaseline >= maeBaseline) {

     val filteredSimilarities = topByKey(similarities, smallestK)(Ordering.by(_._3)).flatMap(_._2)
     val numeratorDenominatorRDD = filteredSimilarities.keyBy{x => x._2}.join(normalizedRatings.keyBy{x => x._1})

     val userSpecificWeightedSumDeviationsRDD = numeratorDenominatorRDD.map{
        case (user2, ((user1, _, similarity), (_, item, normalized_rating))) => ((user1, item), (similarity*normalized_rating, scala.math.abs(similarity)))
     }.reduceByKey(
       (x,y) => (x._1 + y._1, x._2 + y._2)
     ).map{x => (x._1._1, x._1._2, x._2._1/x._2._2)}

     val gatheredDataRDD = processedNormalizedDeviationsRDD.keyBy{ t => (t._1, t._2) }.join(userSpecificWeightedSumDeviationsRDD.keyBy{ t => (t._1, t._2) })

     // RDD[(user, item, rating, baseline_acs, average_rating_user)]
     val baselineAcsRDD = gatheredDataRDD.map {
        case ((user, item), ((_, _, _, rating, average_rating_user), (_, _, weightedSumDeviation)))
     => (user, item, rating, average_rating_user + weightedSumDeviation * scale((average_rating_user + weightedSumDeviation), average_rating_user))
     }.persist()

     // Calculate the MAE for the proposed baseline
     smallestBaseline = baselineAcsRDD.map(r => scala.math.abs(r._3.toDouble - r._4.toDouble)).filter(!_.isNaN).reduce(_ + _).toDouble/baselineAcsRDD.filter(r => ! r._4.isNaN).count.toDouble

     smallestK = smallestK + 1
  }
  
  val difference = smallestBaseline - maeBaseline 

  
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
 
  // Changed TopByKey function from MLLib
  def topByKey(similarities : RDD[(Int, (Int, Int, Double))], num: Int)(implicit ord: Ordering[(Int, Int, Double)]): RDD[(Int, Array[(Int, Int, Double)])] = {
    similarities.aggregateByKey(new BoundedPriorityQueue[(Int, Int, Double)](num)(ord))(
      seqOp = (queue, item) => {
        queue += item
      },
      combOp = (queue1, queue2) => {
        queue1 ++= queue2
      }
    ).mapValues(_.toArray.sorted(ord.reverse))  // This is a min-heap, so we reverse the order.
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
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> MAE(10), // Datatype of answer: Double
            "MaeForK=30" -> MAE(30), // Datatype of answer: Double
            "MaeForK=50" -> MAE(50), // Datatype of answer: Double
            "MaeForK=100" -> MAE(100), // Datatype of answer: Double
            "MaeForK=200" -> MAE(200), // Datatype of answer: Double
            "MaeForK=400" -> MAE(400), // Datatype of answer: Double
            "MaeForK=800" -> MAE(800), // Datatype of answer: Double
            "MaeForK=943" -> MAE(943), // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> (smallestK - 1).toInt, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> difference // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> numberBytesMap(10), 
            "MinNumberOfBytesForK=30" -> numberBytesMap(30),
            "MinNumberOfBytesForK=50" -> numberBytesMap(50),
            "MinNumberOfBytesForK=100" -> numberBytesMap(100), 
            "MinNumberOfBytesForK=200" -> numberBytesMap(200), 
            "MinNumberOfBytesForK=400" -> numberBytesMap(400), 
            "MinNumberOfBytesForK=800" -> numberBytesMap(800), 
            "MinNumberOfBytesForK=943" -> numberBytesMap(943)
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> 17179860388L, 
            "MaximumNumberOfUsersThatCanFitInRam" -> (17179860388L/24).floor.toInt
          )

          // Answer the Question 3.2.4 exclusively on the report.
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


class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  //  Note: this class supports Scala 2.12. A parallel source tree has a 2.13 implementation.
  
  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  override def size: Int = underlying.size

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem: A): this.type = {
    if (size < maxSize) {
      underlying.offer(elem)
    } else {
      maybeReplaceLowest(elem)
    }
    this
  }

  def poll(): A = {
    underlying.poll()
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear(): Unit = { underlying.clear() }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else {
      false
    }
  }
}