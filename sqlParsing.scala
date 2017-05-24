import org.apache.spark._
import java.io._ 
import org.apache.spark.sql.SQLContext;

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val reviewdatas = sqlContext.jsonFile("hdfs:///user/student11/kcore_5.json")
val metadatas = sqlContext.jsonFile("hdfs:///user/student11/metadata.json")
val writeToFile = new PrintWriter(new File("reviews.txt"))

reviewdatas.take(50000).map{
case rd =>
val asin = rd.getString(rd.fieldIndex("asin"))
val reviewerID = rd.getString(rd.fieldIndex("reviewerID"))
val reviewText = rd.getString(rd.fieldIndex("reviewText"))
val ratings = rd.getString(rd.fieldIndex("overall"))
writeToFile.write(asin + ", " + reviewerID + ", " + reviewText + ", " + ratings +"\n")
}
writeToFile.close

val writeToFile = new PrintWriter(new File("metas.txt"))
metadatas.take(50000).map{
case md =>
val asin = md.getString(md.fieldIndex("asin"))
val title = md.getString(md.fieldIndex("title"))
val categories = md.getString(md.fieldIndex("categories"))
writeToFile.write(asin + ", " + title + ", " + categories + "\n")
}
writeToFile.close