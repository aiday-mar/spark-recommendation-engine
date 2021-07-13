package stats

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Analyzer extends App {
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
  

  //*** CODE GOES BELOW ***
  
  // QUESTION 3.1.1 
  val avg = data.map(r => r.rating).reduce(_+_)/data.count
  
  // QUESTION 3.1.2 and 3.1.3
  val groupedUsers = data.groupBy(x => x.user).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
  val groupedItems = data.groupBy(x => x.item).map{x => (x._1, x._2.toList.map(r => r.rating).reduce(_+_)/x._2.toList.size)}
   
  val maxUsers = groupedUsers.sortBy(_._2, false).take(1).map(_._2).head  
  val minUsers = groupedUsers.sortBy(_._2, true).take(1).map(_._2).head

  val maxItems = groupedItems.sortBy(_._2, false).take(1).map(_._2).head
  val minItems = groupedItems.sortBy(_._2, true).take(1).map(_._2).head
   
  val usersCloseToAverage = groupedUsers.filter(r => r._2 < avg + 0.5 && r._2 > avg - 0.5).count.toDouble/100000
  val itemsCloseToAverage = groupedItems.filter(r => r._2 < avg + 0.5 && r._2 > avg - 0.5).count.toDouble/100000
  
  val averageUsers = (minUsers+maxUsers)/2
  val averageItems = (minItems+maxItems)/2

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
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> avg,
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                "min" -> minUsers,
                "max" -> maxUsers, 
                "average" -> averageUsers,
            ),
            "AllUsersCloseToGlobalAverageRating" -> false, 
            "RatioUsersCloseToGlobalAverageRating" -> usersCloseToAverage 
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                "min" -> minItems,
                "max" -> maxItems,
                "average" -> averageItems,
            ),
            "AllItemsCloseToGlobalAverageRating" -> false, 
            "RatioItemsCloseToGlobalAverageRating" -> itemsCloseToAverage
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
