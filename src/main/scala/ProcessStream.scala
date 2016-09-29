import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext,SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.functions.{rank,desc,explode,dense_rank}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import com.datastax.spark.connector.{SomeColumns, _}

object ProcessStream{

import RealTimePosts._

import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._

case class RealTimePosts(tags:Seq[String],link:String,title:String,answered:Boolean)

case class Record(word:String)

def main(args: Array[String]){

        val conf = new SparkConf()
	.setAppName("stackoverflowstreamprocessing")
	.setMaster("local[*]")
	.set("spark.cassandra.connection.host","ip-172-31-1-106")
        .set("spark.cassandra.auth.username", "cassandra")
        .set("spark.cassandra.auth.password", "cassandra")

        val sc = new SparkContext(conf)
	val ssc = new StreamingContext(sc, Seconds(10))
	val sqlContext = new SQLContext(sc)
        ssc.checkpoint("checkpoint") 

	import sqlContext.implicits._

      val  topics= List("stackoverflow").toSet
      val  kafkabrokers = Map("metadata.broker.list" -> "ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9092, ec2-54-69-129-249.us-west-2.compute.amazonaws.com:9092, ec2-54-69-102-27.us-west-2.compute.amazonaws.com:9092, ec2-54-69-151-54.us-west-2.compute.amazonaws.com:9092")
     val directKafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkabrokers,topics).map(_._2)
   
	directKafkaStream.print()
	directKafkaStream.foreachRDD{ rdd =>
sqlContext.jsonRDD(rdd).registerTempTable("realtimeposts")
      
if (rdd.toLocalIterator.nonEmpty) {
//        sqlContext.jsonRDD(rdd).registerTempTable("realtimeposts")
val tagsofquestion = sqlContext.sql("Select title,tags,is_answered,link from realtimeposts")
val tagquestion = tagsofquestion.select("title","tags","is_answered").withColumn("tag",explode($"tags"))
tagquestion.registerTempTable("tagquestion")
val tags = sqlContext.sql("select tags from realtimeposts").withColumn("tag",explode($"tags")).select("tag")
val tagcount = tags.map(x=>(x(0).toString,1)).reduceByKey(_+_).toDF()
tagcount.show()
val newtagcount =tagcount.select($"_1".alias("TagName"),$"_2".alias("Count"))
newtagcount.show()
val trendingtagsfromdb = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "trendingtags", "keyspace" -> "stackoverflow")).load
trendingtagsfromdb.show()

val updatedtagcount = trendingtagsfromdb.unionAll(newtagcount)
//.map(x=>(x(0).toString,x(1).toString)).toDF()
//.reduceByKey(_+_).toDF()
updatedtagcount.printSchema()
updatedtagcount.show()

sqlContext.sql( "SELECT tags,link,title,is_answered FROM realtimeposts").map(RealTimePosts(_)).saveToCassandra("stackoverflow","realtimeposts")
//x.toDF().write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "realtimeposts", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()

val df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tagtousers", "keyspace" -> "stackoverflow")).load
val tagtosingleuser = df.withColumn("user",explode($"users"))
tagtosingleuser.registerTempTable("tagtousers")
val fin = sqlContext.sql("select user,title from tagquestion r Join tagtousers t on r.tag = t.tag where is_answered!=true")
val finremove = sqlContext.sql("select user,title from tagquestion r Join tagtousers t on r.tag = t.tag where is_answered =true")
fin.map(x=>(x(0).toString,Set(x(1).toString))).reduceByKey(_ ++ _).saveToCassandra("stackoverflow","usertoquestion",SomeColumns("userid","questions" append)) 
finremove.map(x=>(x(0).toString,Set(x(1).toString))).reduceByKey(_ ++ _).saveToCassandra("stackoverflow","usertoquestion",SomeColumns("userid","questions" remove))
//val temp = rdd.map(_.split(" ",-1)).map(n=>RealTimePosts(n(0),n(1))).toDF()
//temp.registerTempTable("posts")
//val postsdf = sqlContext.sql( "SELECT * FROM posts").write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "realtimeposts", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()
}
}
/**
val  topics= List("votes").toSet
val directKafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkabrokers,topics).map(_._2)

        directKafkaStream.print()
        directKafkaStream.foreachRDD{ rdd =>
sqlContext.jsonRDD(rdd).registerTempTable("votes")

if (rdd.toLocalIterator.nonEmpty) {
val votes=sqlContext.sql("select UserId,VoteTypeId,Count(*) cnt from votes group by UserId,VoteTypeId order by VoteTypeId")
votes.printSchema()
votes.registerTempTable("votes")
val df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "userprofile", "keyspace" -> "stackoverflow")).load

}
}
*/
      ssc.start()
      ssc.awaitTermination()
   }
object RealTimePosts{
def apply(r: Row): RealTimePosts = RealTimePosts(
      r.getAs[Seq[String]](0), r.getString(1),r.getString(2),r.getBoolean(3))
  }
}

