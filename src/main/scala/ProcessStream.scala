import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext,SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
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
	 sqlContext.sql( "SELECT tags,link,title,is_answered FROM realtimeposts").map(RealTimePosts(_)).saveToCassandra("stackoverflow","realtimeposts")
//x.toDF().write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "realtimeposts", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()
 
//val temp = rdd.map(_.split(" ",-1)).map(n=>RealTimePosts(n(0),n(1))).toDF()
//temp.registerTempTable("posts")
//val postsdf = sqlContext.sql( "SELECT * FROM posts").write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "realtimeposts", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()

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

