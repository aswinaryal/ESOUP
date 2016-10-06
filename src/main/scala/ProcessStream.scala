import org.apache.spark.streaming.kafka._
import java.util.Calendar
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext,SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.functions.{rank,desc,explode,dense_rank,current_date,current_timestamp}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import com.datastax.spark.connector.{SomeColumns, _}

object ProcessStream{

import RealTimePosts._

import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._

case class RealTimePosts(tags:Seq[String],link:String,title:String,answered:Boolean)

case class RealTimeVotes(id:String, downvotes:String,upvotes:String)

case class Record(word:String)

def main(args: Array[String]){

        val conf = new SparkConf(true)
	.setAppName("stackoverflowstreamprocessing")
	.set("spark.cassandra.connection.host","ip-172-31-1-106")
        .set("spark.cassandra.auth.username", "cassandra")
        .set("spark.cassandra.auth.password", "cassandra")

        val sc = new SparkContext(conf)
	val ssc = new StreamingContext(sc, Seconds(5))
	val sqlContext = new SQLContext(sc)
        ssc.checkpoint("checkpoint") 
	import sqlContext.implicits._

      val  topics= List("posts").toSet
      val  kafkabrokers = Map("metadata.broker.list" -> "ec2-52-32-170-191.us-west-2.compute.amazonaws.com:9092, ec2-52-42-114-72.us-west-2.compute.amazonaws.com:9092, ec2-52-34-46-77.us-west-2.compute.amazonaws.com:9092")
     val directKafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkabrokers,topics).map(_._2)
   
	directKafkaStream.print()
	directKafkaStream.foreachRDD{ rdd =>
sqlContext.jsonRDD(rdd).registerTempTable("realtimeposts")
      
if (rdd.toLocalIterator.nonEmpty) {
val tagsofquestion = sqlContext.sql("Select tags,link,title,is_answered from realtimeposts")
tagsofquestion.map(RealTimePosts(_)).saveToCassandra("stackoverflow","realtimeposts", SomeColumns("tags","link","title","answered"))
val tagquestion = tagsofquestion.select("title","tags","is_answered").withColumn("tag",explode($"tags"))
tagquestion.registerTempTable("tagquestion")

val tags = sqlContext.sql("select tags from realtimeposts where is_answered!=true").withColumn("tag",explode($"tags")).select("tag")
val tagcount = tags.map(x=>(x(0).toString,1)).reduceByKey(_+_).toDF()

val newtagcount =tagcount.select($"_1".alias("tagname"),$"_2".alias("count"))

val trendingtagsfromdb = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "trendingtags", "keyspace" -> "stackoverflow")).load

val deldate = trendingtagsfromdb.select(trendingtagsfromdb("tagname"),trendingtagsfromdb("count"))
val sortresult = deldate.unionAll(newtagcount).map(x=>(x.getString(0),x.getInt(1))).reduceByKey(_+_).toDF()
val trending = sortresult.select($"_1".alias("tagname"),$"_2".alias("count")).withColumn("updatedon",current_timestamp())
trending.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "trendingtags", "keyspace" -> "stackoverflow")).mode(SaveMode.Overwrite).save()

val df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tagtousers", "keyspace" -> "stackoverflow")).load

val tagtosingleuser = df.withColumn("user",explode($"users"))
tagtosingleuser.registerTempTable("tagtousers")

val fin = sqlContext.sql("select user,title from tagquestion r Join tagtousers t on r.tag = t.tag where is_answered!=true")

val finremove = sqlContext.sql("select user,title from tagquestion r Join tagtousers t on r.tag = t.tag where is_answered =true")

fin.map(x=>(x(0).toString,Set(x(1).toString))).reduceByKey(_ ++ _).saveToCassandra("stackoverflow","usertoquestion",SomeColumns("userid","questions" append)) 
finremove.map(x=>(x(0).toString,Set(x(1).toString))).reduceByKey(_ ++ _).saveToCassandra("stackoverflow","usertoquestion",SomeColumns("userid","questions" remove))
//temp.registerTempTable("posts")
//val postsdf = sqlContext.sql( "SELECT * FROM posts").write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "realtimeposts", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()
}
}

val  topics1= List("votes").toSet
val directKafkaStream1 = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkabrokers,topics1).map(_._2)

        directKafkaStream1.print()
        directKafkaStream1.foreachRDD{ rdd =>
sqlContext.jsonRDD(rdd).registerTempTable("upvotes")

if (rdd.toLocalIterator.nonEmpty) {
val upvotes=sqlContext.sql("select id,sum(upvotes) as upvotes from upvotes group by id")
upvotes.show()
val realtimeupvotesfromdb = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "realtimeupvotes", "keyspace" -> "stackoverflow")).load
import org.apache.spark.sql._
val unionupvotes = upvotes.unionAll(realtimeupvotesfromdb).map(x=>(x(0).toString,x.getAs[Long](1))).reduceByKey(_ + _)
unionupvotes.toDF("id","upvotes").show()

unionupvotes.saveToCassandra("stackoverflow","realtimeupvotes",SomeColumns("id","upvotes"))
}
}

val  topics2= List("downvotes").toSet
val directKafkaStream2 = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkabrokers,topics2).map(_._2)

        directKafkaStream2.print()
        directKafkaStream2.foreachRDD{ rdd =>
sqlContext.jsonRDD(rdd).registerTempTable("downvotes")

if (rdd.toLocalIterator.nonEmpty) {
val downvotes=sqlContext.sql("select id,sum(downvotes) as downvotes from downvotes group by id")
val realtimedownvotesfromdb = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "realtimedownvotes", "keyspace" -> "stackoverflow")).load
import org.apache.spark.sql._
val uniondownvotes = downvotes.unionAll(realtimedownvotesfromdb).map(x=>(x(0).toString,x.getAs[Long](1))).reduceByKey(_ + _)
uniondownvotes.toDF("id","downvotes").show()
uniondownvotes.saveToCassandra("stackoverflow","realtimedownvotes",SomeColumns("id","downvotes"))
}
}
      ssc.start()
      ssc.awaitTermination()
   }
object RealTimePosts{
def apply(r: Row): RealTimePosts = RealTimePosts(
      r.getAs[Seq[String]](0), r.getString(1),r.getString(2),r.getBoolean(3))
  }
}

