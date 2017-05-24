import java.io.File

import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}


case class MyRating(userId: String, product: String, rating: Double)
case class MyProduct(product: String, name: String)

/** Compute RMSE (Root Mean Squared Error). */
def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
  val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
  val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating)).join(data.map(x => ((x.user, x.product), x.rating))).values
  math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
}

/** Load ratings from file. */
def loadRatings(path: String): Seq[Rating] = {
  val lines = Source.fromFile(path).getLines()
  val ratings = lines.map { line =>
    val fields = line.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }.filter(_.rating > 0.0)
  if (ratings.isEmpty) {
    sys.error("No ratings provided.")
  } else {
    ratings.toSeq
  }
}

// load personal ratings

val myRatings = loadRatings("/home/student11/ALS/personalRatings.txt") 
val myRatingsRDD = sc.parallelize(myRatings, 1)

// load ratings and movie titles

val HomeDir = "hdfs:///user/student11/SCV"

val ratingdata = sc.textFile(new File(HomeDir, "ratings.dat").toString).map { line =>
  val fields = line.split("::")
  (MyRating(fields(0).toString, fields(1).toString, fields(2).toDouble))
}

val ridToInt: RDD[(String, Long)] = ratingdata.map(_.userId).distinct().zipWithIndex()
val ratingMapping1: RDD[(Long, String)] = ridToInt map { case (l, r) => (r, l) }
val mapId: Map[String, Int]= ridToInt.collect().toMap.mapValues(_.toInt)

val rasinToInt: RDD[(String, Long)] = ratingdata.map(_.product).distinct().zipWithIndex()
val ratingMapping2: RDD[(Long, String)] = rasinToInt map { case (l, r) => (r, l) }
val mapAsin: Map[String, Int]= rasinToInt.collect().toMap.mapValues(_.toInt)

val ratings: RDD[Rating] = ratingdata.map { r =>
    Rating(ridToInt.lookup(r.userId).head.toInt, rasinToInt.lookup(r.product).head.toInt, r.rating)
  }

val productdata = sc.textFile(new File(HomeDir, "products.dat").toString).map { line =>
      val fields = line.split("::")
      MyProduct(fields(0), fields(1))
     }

val pidToInt: RDD[(String, Long)] = products.map(_.product).distinct().zipWithIndex()
val productMapping1: RDD[(Long, String)] = pidToInt map { case (l, r) => (r, l) }
val pmapId: Map[String, Int]= pidToInt.collect().toMap.mapValues(_.toInt)

val product: RDD[(Long, String)] = products.map { r =>
    (pidToInt.lookup(r.product).head.toInt, r.name)
  }collect().toMap


val numRatings = ratings.count()
val numUsers = ratings.map(_._2.user).distinct().count()
val numproducts = ratings.map(_._2.product).distinct().count()

println("Got " + numRatings + " ratings from "
  + numUsers + " users on " + numMovies + " products.")

// split ratings into train (60%), validation (20%), and test (20%) based on the 
// last digit of the timestamp, add myRatings to train, and cache them

val numPartitions = 4
val training = ratings.filter(x => x._1 < 6).values.union(myRatingsRDD).repartition(numPartitions).cache()
val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).cache()
val test = ratings.filter(x => x._1 >= 8).values.cache()

val numTraining = training.count()
val numValidation = validation.count()
val numTest = test.count()

println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

// train models and evaluate them on the validation set

val ranks = List(8, 12)
val lambdas = List(0.1, 10.0)
val numIters = List(10, 20)
var bestModel: Option[MatrixFactorizationModel] = None
var bestValidationRmse = Double.MaxValue
var bestRank = 0
var bestLambda = -1.0
var bestNumIter = -1
for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
  val model = ALS.train(training, rank, numIter, lambda)
  val validationRmse = computeRmse(model, validation, numValidation)
  println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "  + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
  if (validationRmse < bestValidationRmse) {
    bestModel = Some(model)
    bestValidationRmse = validationRmse
    bestRank = rank
    bestLambda = lambda
    bestNumIter = numIter
  }
}

// evaluate the best model on the test set

val testRmse = computeRmse(bestModel.get, test, numTest)

println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
  + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

// create a naive baseline and compare it with the best model

val meanRating = training.union(validation).map(_.rating).mean
val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
val improvement = (baselineRmse - testRmse) / baselineRmse * 100
println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

// make personalized recommendations

val myRatedMovieIds = myRatings.map(_.product).toSet
val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
val recommendations = bestModel.get.predict(candidates.map((0, _))).collect().sortBy(- _.rating).take(50)

var i = 1
println("Movies recommended for you:")
recommendations.foreach { r =>
  println("%2d".format(i) + ": " + movies(r.product))
  i += 1
}

// clean up
sc.stop()