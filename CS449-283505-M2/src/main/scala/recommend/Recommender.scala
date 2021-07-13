package recommend

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
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Recommender extends App {
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
  println("Loading data from: " + conf.data())
  val dataFile = spark.sparkContext.textFile(conf.data())
  val dataInitial = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(dataInitial.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal())
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles
  assert(personalFile.count == 1682, "Invalid personal data")

  // Added from Milestone 1

  val personalData = personalFile.map(f=>{
     f.split(",").map(_.trim).toList
  }).filter(r => r.size == 3).map{r => Rating(944, r(0).toInt, r(2).toDouble)}
  val myAverage = personalData.map(y => y.rating).reduce(_+_)/personalData.count

  val data = dataInitial.union(personalData)

  // RDD[(user, average_rating_user)]
  val groupedUsers = data.groupBy(x => x.user).map{ x => (x._1, x._2.toList.map(r => r.rating).reduce(_ + _) / x._2.toList.size) }.persist()

  // RDD[(item, average_rating_item)]
  val groupedItems = data.groupBy(x => x.item).map{ x => (x._1, x._2.toList.map(r => r.rating).reduce(_ + _) / x._2.toList.size) }

  // RDD[(user, item, rating, average_rating_user, average_rating_item)]
  val temp = data.map(r => (r.user, r.item, r.rating)).keyBy{t => t._1}.join(groupedUsers.keyBy{t => t._1}).map {
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

  val similarities = modified.reduceByKey(_+_).map{ x => (x._1._1, x._1._2, x._2)}.filter{x => x._1 != x._2}.keyBy{t => t._1} 

  // k = 30, 300
  val kValues = Map(1 -> 30, 2 -> 300)
  var mapRDDS = Map[Int, Array[(Int, Int, Double)]]()  
  val alreadyRated = Set(1, 3, 5, 7, 12, 20, 28, 33, 37, 50, 59, 64, 68, 98, 102, 139, 157, 162, 176, 195, 211, 246, 261, 284, 300, 318, 507, 610, 621, 635, 655, 656, 672, 691, 701, 713, 728, 736, 740, 765, 774, 776, 779, 786, 787, 790, 796, 805, 806, 871)

  for ( i <- 1 until 3) {

     println("Computing exercise 3.2.5 for k = " + kValues(i))

     val filteredSimilarities = topByKey(similarities, kValues(i))(Ordering.by(_._3)).flatMap(_._2)
    
     val numeratorDenominatorRDD = filteredSimilarities.keyBy{x => x._2}.join(normalizedRatings.keyBy{x => x._1})

     // user, item, personal_weighted_sum_deviations
     val personalWeightedSumDeviationsRDD = numeratorDenominatorRDD.map{
        case (user2, ((user1, _, similarity), (_, item, normalized_rating))) => ((user1, item), (similarity*normalized_rating, scala.math.abs(similarity)))
     }.reduceByKey(
       (x,y) => (x._1 + y._1, x._2 + y._2)
     ).map{x => (x._1._1, x._1._2, x._2._1/x._2._2)}
         
     // taking the key against the user and the item
     val gatheredDataRDD = processedNormalizedDeviationsRDD.keyBy{ t => (t._1, t._2) }.join(personalWeightedSumDeviationsRDD.keyBy{ t => (t._1, t._2) })

     // RDD[(user, item, baseline_acs)]
     val baselineAcsRDD = gatheredDataRDD.map {
        case ((user, item), ((_, _, _, _, average_rating_user), (_, _, weightedSumDeviation)))
     => (user, item, average_rating_user + weightedSumDeviation * scale((average_rating_user + weightedSumDeviation), average_rating_user))
     }.filter{x => x._1 == 944}.filter{x => !alreadyRated.contains(x._2)}.sortBy(_._3, false).take(5)
 
     mapRDDS += (kValues(i) -> baselineAcsRDD)
  }
 
  // println("Printing the RDD for k = 30")
  // mapRDDS(30).foreach(println)
  // println("Printing the RDD for k = 300")
  // mapRDDS(300).foreach(println)
  
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

          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.

          "Q3.2.5" -> Map(
            "Top5WithK=30" ->
              List[Any](
                List(804, "Jimmy Hollywood (1994)", 5.0), 
                List(801, "Air Up There, The (1994)", 4.6277), 
                List(797, "Timecop (1994)", 3.8535), 
                List(803, "Heaven & Earth (1993)", 3.0999),
                List(799, "Boys Life (1995)", 2.6356)
              ),

            "Top5WithK=300" ->
              List[Any](
                List(804, "Jimmy Hollywood (1994)", 4.7650), 
                List(797, "Timecop (1994)", 4.2901), 
                List(801, "Air Up There, The (1994)", 3.5233), 
                List(803, "Heaven & Earth (1993)", 3.1418),
                List(799, "Boys Life (1995)", 2.9719)
              )
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