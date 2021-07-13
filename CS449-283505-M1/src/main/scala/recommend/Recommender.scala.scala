package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

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
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(data.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal()) 
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles
  assert(personalFile.count == 1682, "Invalid personal data")
 
  
  // *** QUESTION 4.1.1 ***
  
  // selecting only the ratings where rating exists and for me specifically as user 944  
  val personalData = personalFile.map(f=>{
     f.split(",").map(_.trim).toList
  }).filter(r => r.size == 3).map{r => Rating(944, r(0).toInt, r(2).toDouble)}
  val myAverage = personalData.map(y => y.rating).reduce(_+_)/personalData.count

  // same calculations as in question 3
  val groupedUsers = data.groupBy(x => x.user).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
  val groupedItems = data.groupBy(x => x.item).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
 
  val temp = data.map(r => (r.user, r.item, r.rating)).keyBy{t => t._1}.join(groupedUsers.keyBy{t => t._1}).map {
        case (user, ((_, item, rating), (_, average_rating_user))) => (user, item, rating, average_rating_user)
  }
  val averagesRDD = temp.keyBy{t => (t._2)}.join(groupedItems.keyBy{t => t._1}).map {
        case (item, ((user, _, rating, average_rating_user), (_, average_rating_item))) => (user, item, rating, average_rating_user, average_rating_item)
  }

  val normalizedRDD = averagesRDD.map{
       case (user, item, rating, average_rating_user, average_rating_item) => (user, item, rating, average_rating_user, average_rating_item, (rating - average_rating_user)/(scale(rating, average_rating_user)))
  }
  
  // directly calculate the baseline prediction using my average calculated previously and display in descending order
  val myRecommendations = normalizedRDD.groupBy(x => x._2).map{x => (x._1, x._2.toList.map(r => r._6).reduce(_+_)/x._2.toList.size)}.map{x => (x._1, myAverage+x._2*scale(myAverage + x._2, myAverage))}.sortBy(r => r._2, false)  
  
  // used myRecommendations.take(20).foreach(println) in order to show the first 20 predictions
 
  def scale(rating : Double, ave_rating : Double) : Double = {
     if (rating > ave_rating) {return 5 - ave_rating}
     else if (rating < ave_rating) {return ave_rating - 1}
     else { return 1}
  }
   
  
  // *** QUESTION 4.1.2 ***

  val groupedItemsModified = data.groupBy(x => x.item).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size, x._2.toList.size)}

  val averagesRDDModified = temp.keyBy{t => (t._2)}.join(groupedItemsModified.keyBy{t => t._1}).map {
        case (item, ((user, _, rating, average_rating_user), (_, average_rating_item, number_ratings))) => (user, item, rating, average_rating_user, average_rating_item, number_ratings)
  }

  val normalizedRDDModified = averagesRDDModified.map{
       case (user, item, rating, average_rating_user, average_rating_item, number_ratings) => (user, item, rating, average_rating_user, average_rating_item, (rating - average_rating_user)/(scale(rating, average_rating_user)), number_ratings)
  }
  
  val maxRatings = normalizedRDDModified.sortBy(_._7, false).take(1).map(_._7).head
  // println(maxRatings) gives 583
  val myRecommendationsModified = normalizedRDDModified.groupBy(x => x._2).map{x => (x._1, x._2.toList.map(r => r._6).reduce(_+_)/x._2.toList.size, x._2.toList.map(r=> r._7).head)}.map{x => (x._1, x._3.toDouble/maxRatings.toDouble*(myAverage+x._2*scale(myAverage + x._2, myAverage)))}.sortBy(_._2, false)

  // used myRecommendationsModified.take(20).foreach(println) in order to show the first 20 predictions 
 
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

            "Q4.1.1" -> List[Any](
              List(1293, "Star Kid (1997)", 5.0),
              List(1189, "Prefontaine (1997)", 5.0),
              List(814, "Great Day in Harlem", 5.0),
              List(1536, "Aiqing wansui (1994)", 5.0),
              List(1467, "Saint of Fort Washington:w", 5.0)
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
