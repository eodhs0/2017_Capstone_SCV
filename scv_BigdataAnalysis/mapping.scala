import java.io.File
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

val HomeDir = "hdfs:///user/student11/SCV"
case class MyRating(userId: String, product: String, rating: Double)

val ratings = sc.textFile(new File(HomeDir, "ratings.dat").toString).map { line =>
      val fields = line.split("::")
      MyRating(fields(0), fields(1), fields(2).toDouble)
     }

val idToInt: RDD[(String, Long)] = ratings.map(_.userId).distinct().zipWithUniqueId()
val reverseMapping1: RDD[(Long, String)] = idToInt map { case (l, r) => (r, l) }
val mapId: Map[String, Int]= idToInt.collect().toMap.mapValues(_.toInt)

val asinToInt: RDD[(String, Long)] = ratings.map(_.product).distinct().zipWithUniqueId()
val reverseMapping2: RDD[(Long, String)] = asinToInt map { case (l, r) => (r, l) }
val mapAsin: Map[String, Int]= asinToInt.collect().toMap.mapValues(_.toInt)