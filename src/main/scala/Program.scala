import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.databricks.spark.csv
import org.apache.spark.sql.{Row,SaveMode}
import org.apache.spark.sql.functions.explode
import com.datastax.spark.connector._ 
import com.datastax.spark.connector.{SomeColumns, _}
import scala.xml.XML

object Program{

case class Post(Id: String,PostTypeId: String,ParentId: String,AcceptedAnswerId: String,CreationDate: String,Score: String,ViewCount: String,OwnerUserId: String,LastEditorUserId: String,LastEditorDisplayName: String,LastEditDate: String,LastActivityDate: String,Tags: String,AnswerCount: String,CommentCount: String,FavoriteCount: String,CommunityOwnedDate: String,OwnerDisplayName: String)

case class User(Id: String,Reputation: String, Location: String, CreationDate: String, DisplayName: String, LastAccessDate: String, Views: String, Upvotes: String, Downvotes: String, AccountId: String, WebSiteURL: String, Age: String)

case class Vote(Id: String,PostId: String, VoteTypeId: String, CreationDate: String, BountyAmount: String, UserId: String)

case class Expertise(UserId: String, tagname: String)

case class Tag(UserId: String, domain:Array[String])

case class TrendingTags(tag:Array[String])

case class TrendingTagsAfterParse(tagtitle: String)

case class FinalTags(tag: String, number: Int)

case class TagsFile(Id:Int,TagName:String,Count:Int, ExcerptPostId:String, WikiPostId:String)

def main(args: Array[String]){

//val conf = new SparkConf().setAppName("Stackoverflow_program")
//val sc = new SparkContext(conf)
//val sqlContext = new SQLContext(sc)

val conf = new SparkConf(true).set("spark.cassandra.connection.host","ip-172-31-1-106")
	.set("spark.cassandra.auth.username", "cassandra")            
        .set("spark.cassandra.auth.password", "cassandra")

val sc = new SparkContext("spark://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:7077","stackoverflow",conf)
val sqlContext = new SQLContext(sc)

import sqlContext.implicits._

val removetagsextralines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/tagsfileextra.txt")

val tagsfilebeforeremovinglines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/Tags.xml")

tagsfilebeforeremovinglines.count()

val tagsfileafterremovinglines = tagsfilebeforeremovinglines.subtract(removetagsextralines)

val tagsfile = tagsfileafterremovinglines.map(r=>{val elements =scala.xml.XML.loadString(r)
val id = (elements \ "@Id").text  +","+ (elements \ "@TagName").text +","+  ( elements \ "@Count").text +","+(elements \ "@ExcerptPostId").text +","+  (elements \ "@WikiPostId").text
id}).map(_.split(",",-1)).map(n=>TagsFile(n(0).toInt,n(1),n(2).toInt,n(3),n(4))).toDF()

tagsfile.registerTempTable("tags")

val tag_count = sqlContext.sql("SELECT TagName,Count from tags order by Count desc limit 10")

tag_count.rdd.saveToCassandra("stackoverflow","trendingtags",SomeColumns("tagtitle","total"))

//val post = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/Input/posts.csv").map(_.split(",")).map(p=>Post(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17))).toDF()

val removepostsextralines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/postsfileextra.txt")

val postsfilebeforeremovinglines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/Posts.xml")

postsfilebeforeremovinglines.count()

val postsfileafterremovinglines = postsfilebeforeremovinglines.subtract(removepostsextralines)


val post = postsfileafterremovinglines.map(r=>{
val elements =scala.xml.XML.loadString(r)
val id = (elements \ "@Id").text  +","+ (elements \ "@PostTypeId").text +","+ (elements \ "@ParentId").text +","+ (elements \ "@AcceptedAnswerId").text +","+ (elements \ "@CreationDate").text +","+ (elements \ "@Score").text +","+ (elements \ "@ViewCount").text +","+ (elements \ "@OwnerUserId").text +","+ (elements \ "@LastEditorUserId").text +","+ (elements \ "@LastEditorDisplayName").text +","+ (elements \ "@LastEditDate").text +","+ (elements \ "@LastActivityDate").text +","+ (elements \ "@Tags").text +","+ (elements \ "@AnswerCount").text +","+ (elements \ "@CommentCount").text +","+ (elements \ "@FavoriteCount").text +","+ (elements \ "@CommunityOwnedDate").text +","+ (elements \ "@OwnerDisplayName").text
id
}).map(_.split(",",-1)).map(p=>Post(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17))).toDF()

post.printSchema()
post.registerTempTable("post")

//val trendingtags = sqlContext.sql("SELECT Tags from post where PostTypeId=1").map(t=>{
//val tagsparsed = t(0).toString().replace("""><""","\",\"").replace("""<""","\"").replace(""">""","\"").split(",")
//TrendingTags(tagsparsed)})

//val ttags = trendingtags.toDF("tag")

//val tt = ttags.withColumn("tagtitle", explode($"tag"))
//tt.registerTempTable("TrendingTagsAfterParse")

//val trendingtagsinyear = sqlContext.sql("select tagtitle, count(*) total from TrendingTagsAfterParse group by tagtitle order by total desc Limit 10")

//trendingtagsinyear.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "trendingtags", "keyspace" -> "stackoverflow")).mode(SaveMode.Append).save()

//trendingtagsinyear.rdd.saveToCassandra("stackoverflow","trendingtags",SomeColumns("tagtitle","total"))

//trendingtagsinyear.rdd.saveAsTextFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/trendingtagsinyear/")

val toptags = sqlContext.sql("SELECT p.OwnerUserId,d.Tags,months_between(current_date(),p.CreationDate) Latest from post p JOIN (SELECT * from post where AcceptedAnswerId is not null) d ON d.AcceptedAnswerId = p.Id where p.Id is not null and p.OwnerUserId is not null and months_between(current_date(),p.CreationDate)<=6 group by p.OwnerUserId,d.Tags,months_between(current_date(),p.CreationDate)").map(r=> {

val domainparsed = r(1).toString().replace("""><""","\",\"").replace("""<""","\"").replace(""">""","\"").split(",")
Tag(r(0).toString(),domainparsed)})

val q = toptags.toDF("UserId","domain")

val x = q.withColumn("tagname",explode($"domain"))

x.printSchema()

x.registerTempTable("Expertise")

val toptagsperuser = sqlContext.sql("select UserId, tagname from Expertise group by UserId, tagname")

toptagsperuser.show(25)

//val user = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/Input/users.csv").map(_.split(",")).map(u=>User(u(0),u(1),u(2),u(3),u(4),u(5),u(6),u(7),u(8),u(9),u(10),u(11))).toDF()

val removeusersextralines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/usersfileextra.txt")

val usersfilebeforeremovinglines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/Users.xml")

usersfilebeforeremovinglines.count()

val usersfileafterremovinglines = usersfilebeforeremovinglines.subtract(removeusersextralines)

val user = usersfileafterremovinglines.map(r=>{
val elements =scala.xml.XML.loadString(r)
val id = (elements \ "@Id").text +","+ (elements \ "@Reputation").text +","+ (elements \ "@Location").text.split(",")(0) +","+ (elements \ "@CreationDate").text +","+ (elements \ "@DisplayName").text +","+ (elements \ "@LastAccessDate").text +","+ (elements \ "@Views").text +","+ (elements \ "@Upvotes").text +","+ (elements \ "@Downvotes").text +","+ (elements \ "@AccountId").text +","+ (elements \ "@WebSiteURL").text +","+ (elements \ "@Age").text
id}).map(_.split(",",-1)).map(u=>User(u(0),u(1),u(2),u(3),u(4),u(5),u(6),u(7),u(8),u(9),u(10),u(11))).toDF()

user.registerTempTable("user")

val toptagsforuser = sqlContext.sql(" SELECT e.UserId,u.DisplayName, u.Reputation, u.Upvotes, u.Downvotes, e.tagname, count(*) cnt from Expertise e JOIN user u ON e.UserId = u.Id where e.UserId is NOT NULL group by e.UserId, u.DisplayName,u.Reputation, u.Upvotes, u.Downvotes, e.tagname HAVING cnt>=5 order by e.UserId,cnt desc")

toptagsforuser.show(10)
//toptagsforuser.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "toptagsforuser", "keyspace" -> "stackoverflow")).mode(SaveMode.Overwrite).save()
//toptagsforuser.rdd.saveAsTextFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/toptagsforuser/")


val removevotesextralines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/votesfileextra.txt")

val votesfilebeforeremovinglines = sc.textFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/stackoverflow/Votes.xml")

votesfilebeforeremovinglines.count()

val votesfileafterremovinglines = usersfilebeforeremovinglines.subtract(removeusersextralines)

val vote = votesfileafterremovinglines.map(_.split(",")).map(v=>Vote(v(0),v(1),v(2),v(3),v(4),v(5))).toDF()

vote.registerTempTable("vote")

val favoritevotes = sqlContext.sql("SELECT p.OwnerUserId, v.UserId, count(*) cnt from post p JOIN vote v on v.PostId = p.Id where v.VoteTypeId=5 group by p.OwnerUserId, v.UserId") 
favoritevotes.show(20)
//favoritevotes.rdd.saveAsTextFile("hdfs://ec2-52-43-50-72.us-west-2.compute.amazonaws.com:9000/favoritevotes/")
}
}
