import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.databricks.spark.csv
import org.apache.spark.sql.{Row,SaveMode}
import org.apache.spark.sql.functions.{rank,desc,explode,dense_rank}
import org.apache.spark.sql.expressions.Window
import com.datastax.spark.connector._ 
import com.datastax.spark.connector.{SomeColumns, _}
import scala.xml.XML

object Program{

case class Post(id: String,PostTypeId: String,ParentId: String,AcceptedAnswerId: String,CreationDate: String,Score: String,ViewCount: String,owneruserid: String,LastEditorUserId: String,LastEditorDisplayName: String,LastEditDate: String,LastActivityDate: String,Tags: String,AnswerCount: String,CommentCount: String,FavoriteCount: String,CommunityOwnedDate: String,OwnerDisplayName: String)

case class User(id: String,Reputation: String, Location: String, CreationDate: String, DisplayName: String, LastAccessDate: String, Views: String, UpVotes: String, DownVotes: String, AccountId: String, WebsiteUrl: String, Age: String)

case class Vote(id: String,PostId: String, VoteTypeId: String,userid: String, CreationDate: String, BountyAmount: String)

case class Expertise(UserId: String, TagName: String)

case class Tag(UserId: String, domain:Array[String])

case class TagCount(UserId:String, TagName:String, Total:Int)
case class TrendingTags(tag:Array[String])

case class FinalTags(tag: String, number: Int)

case class TagsFile(id:String, TagName:String,Count:Int, ExcerptPostId:String, WikiPostId:String)

def main(args: Array[String]){

val conf = new SparkConf(true).set("spark.cassandra.connection.host","ip-172-31-1-106").set("spark.cassandra.auth.username", "cassandra").set("spark.cassandra.auth.password", "cassandra")

val sc = new SparkContext("spark://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:7077","stackoverflow",conf)
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

import sqlContext.implicits._

val removetagsextralines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/tagsfileextra.txt")

val tagsfilebeforeremovinglines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/Tags.xml")

tagsfilebeforeremovinglines.count()

val tagsfileafterremovinglines = tagsfilebeforeremovinglines.subtract(removetagsextralines)

 val tagsfile = tagsfileafterremovinglines.map(r=>{val elements =scala.xml.XML.loadString(r)
val id = (elements \ "@Id").text  +","+ (elements \ "@TagName").text +","+  ( elements \ "@Count").text +","+(elements \ "@ExcerptPostId").text +","+  (elements \ "@WikiPostId").text
id}).map(_.split(",",-1)).map(n=>TagsFile(n(0),n(1),n(2).toInt,n(3),n(4))).toDF()

tagsfile.registerTempTable("tags")

val tag_count = sqlContext.sql("SELECT TagName,Count from tags order by Count desc limit 10")

tag_count.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "trendingtags", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()
//tag_count.rdd.saveToCassandra("stackoverflow","trendingtags",SomeColumns("tagtitle","total"))

val removepostsextralines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/postsfileextra.txt")

val postsfilebeforeremovinglines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/Input/Posts.xml")

val postsfileafterremovinglines = postsfilebeforeremovinglines.subtract(removepostsextralines)

val post = postsfileafterremovinglines.map(r=>{
val elements =scala.xml.XML.loadString(r)
val id = (elements \ "@Id").text  +","+ (elements \ "@PostTypeId").text +","+ (elements \ "@ParentId").text +","+ (elements \ "@AcceptedAnswerId").text +","+ (elements \ "@CreationDate").text +","+ (elements \ "@Score").text +","+ (elements \ "@ViewCount").text +","+ (elements \ "@OwnerUserId").text +","+ (elements \ "@LastEditorUserId").text +","+ (elements \ "@LastEditorDisplayName").text +","+ (elements \ "@LastEditDate").text +","+ (elements \ "@LastActivityDate").text +","+ (elements \ "@Tags").text +","+ (elements \ "@AnswerCount").text +","+ (elements \ "@CommentCount").text +","+ (elements \ "@FavoriteCount").text +","+ (elements \ "@CommunityOwnedDate").text +","+ (elements \ "@OwnerDisplayName").text
id
}).map(_.split(",",-1)).map(p=>Post(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17))).toDF()

post.printSchema()
post.show(10)
post.registerTempTable("post")

val toptags = sqlContext.sql("SELECT p.owneruserid,d.Tags from post p JOIN (SELECT * from post where AcceptedAnswerId is not null) d ON d.AcceptedAnswerId = p.id where p.CreationDate is not null and p.id is not null and p.owneruserid is not null  and months_between(current_timestamp(),p.CreationDate)<13 group by p.owneruserid,d.Tags,p.CreationDate").map(r=> {
val domainparsed = r(1).toString().replace("""><""",",").replace("""<""","").replace(""">""","").split(",")
Tag(r(0).toString(),domainparsed)})

val q = toptags.toDF("UserId","domain")

val x = q.withColumn("TagName",explode($"domain"))

x.printSchema()

x.registerTempTable("Expertise")

val tagsofuser = sqlContext.sql("select UserId, TagName, Count(*) Total from Expertise where UserId is not null  group by UserId, TagName order by Total desc")

import org.apache.spark.sql.expressions.Window
val overTotal = Window.partitionBy('UserId).orderBy('Total.desc)

val ranked = tagsofuser.withColumn("rank", dense_rank.over(overTotal))

//ranked.show

val finaltoptags = ranked.where('total>=10).where('rank <= 3)

finaltoptags.persist()

finaltoptags.select(finaltoptags("UserId"),finaltoptags("TagName")).map(x=>(x(1).toString,List(x(0).toString))).reduceByKey(_ ++ _).saveToCassandra("stackoverflow","tagtousers",SomeColumns("tag","users"))

finaltoptags.select(finaltoptags("UserId"),finaltoptags("TagName")).map(x=>(x(0).toString,List(x(1).toString))).reduceByKey(_ ++ _).saveToCassandra("stackoverflow","toptags",SomeColumns("userid","tags"))

//finaltoptags.select(finaltoptags("UserId"),finaltoptags("TagName")).map(x=>(x(1).toString,List(x(0).toString))).reduceByKey(_ ++ _).saveToCassandra("stackoverflow","tagtousers",SomeColumns("tag","users"))

//tagsofuser.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tagsofuser", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()

val removeusersextralines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/usersfileextra.txt")
val usersfilebeforeremovinglines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/Input/Users.xml")
usersfilebeforeremovinglines.count()
val usersfileafterremovinglines = usersfilebeforeremovinglines.subtract(removeusersextralines)

val user = usersfileafterremovinglines.map(r=>{
val elements =scala.xml.XML.loadString(r)
val id = (elements \ "@Id").text +","+ (elements \ "@Reputation").text +","+ (elements \ "@Location").text.split(",")(0) +","+ (elements \ "@CreationDate").text +","+ (elements \ "@DisplayName").text +","+ (elements \ "@LastAccessDate").text +","+ (elements \ "@Views").text +","+ (elements \ "@UpVotes").text +","+ (elements \ "@DownVotes").text +","+ (elements \ "@AccountId").text +","+ (elements \ "@WebsiteUrl").text +","+ (elements \ "@Age").text
id}).map(_.split(",",-1)).map(u=>User(u(0),u(1),u(2),u(3),u(4),u(5),u(6),u(7),u(8),u(9),u(10),u(11))).toDF()

user.printSchema()
user.show(5)
user.registerTempTable("user")

val userprofile = sqlContext.sql("Select id, DisplayName,DownVotes,Reputation, UpVotes from user")
userprofile.printSchema()
//userprofile.rdd.saveToCassandra("stackoverflow","userprofile",SomeColumns("Id","DisplayName","Reputation","UpVotes","DownVotes"))
userprofile.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "userprofile", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()

//val toptagsforuser = sqlContext.sql(" SELECT UserId, TagName, Count(*) Total from Expertise where UserId is NOT NULL group by UserId, TagName HAVING Total>=5 order by Total desc")

//toptagsforuser.printSchema()

//toptagsforuser.rdd.saveToCassandra("stackoverflow","toptagsforuser",SomeColumns("UserId","TagName","Total"))

//toptagsforuser.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "toptagsforuser", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()
//toptagsforuser.rdd.saveAsTextFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/toptagsforuser/")

val removevotesextralines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/votesfileextra.txt")

val votesfilebeforeremovinglines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/Input/Votes.xml")

val votesfileafterremovinglines = votesfilebeforeremovinglines.subtract(removevotesextralines)

val vote = votesfileafterremovinglines.map(r=>{
val elements =scala.xml.XML.loadString(r)
val id = (elements \ "@Id").text +","+ (elements \ "@PostId").text +","+ (elements \ "@VoteTypeId").text +","+ (elements \ "@UserId").text +","+ (elements \ "@CreationDate").text +","+ (elements \ "@BountyAmount").text
id}).map(_.split(",",-1)).map(v=>Vote(v(0),v(1),v(2),v(3),v(4),v(5))).toDF()

vote.registerTempTable("vote")


val favoritevotes = sqlContext.sql("SELECT owneruserid, p.id, Count(*) numberofusers from post p JOIN vote v on v.PostId = p.id where v.VoteTypeId='5' group by owneruserid,p.id") 
favoritevotes.show(20)
favoritevotes.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "favoritevotes", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()
//favoritevotes.rdd.saveAsTextFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/favoritevotes/")

val unansweredquestionswithbounty = sqlContext.sql("Select count(*) from vote where VoteTypeId='8'")

val unansweredcountfortag = sqlContext.sql("Select p.id,p.Tags from post Join vote on p.id = v.PostId where v.VoteTypeId='8' and p.PostTypeId='1' and (p.AcceptedAnswerId=null or p.AcceptedAnswerId= ' ')") 
unansweredcountfortag.printSchema()
unansweredcountfortag.collect()

unansweredquestionswithbounty.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "questionscountwithbounty", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()

}
}
