# ESOUP
Project aims to provide questions recommendations to stack overflow users. It also extends user profile/dashboard to show current/recent trends of the Stack overflow website and some insights into user activity.


ESOUP - Extending Stack Overflow User Profile
Introduction
The project aims to recommends questions to users who has expertise in tags provided to question. The application also shows how user performance numbers like Upvotes, Downvotes are changing in real time and current trending tags in the stack overflow along with their counts getting updated in real time. All this information in showed in user dashboard, which keeps user occupied whenever he visits the application and motivates him to visit application frequently.

Data Source
https://archive.org/details/stackexchange

Complete Application data is spread across multiple file types:
1. Users.xml,
2. Posts.xml,
3. Votes.xml

AWS Cluster
ESOUP runs on 4 clusters on AWS:
	1. 4 large nodes for Spark/Spark Streaming,
	2. 4 large nodes for Cassandra,
	3. 3 large nodes for kafka and Flask/Tornado

Data Pipeline


How It Works:
Since, data is in xml format with self closing tags, it is parsed in to csv format using scala XML library in distributed mode.

There are 2 major flows in the application:
    1. Batch Processing - Historical dataset from stack overflow is stored in Hadoop File System(HDFS). Spark reads files from HDFS and calculates top 3 tags for each user if exists. How I defined top tag? a tag with at least 10 accepted answers in last 1year will make it a top tag for that user. If there are more than 3 tags which matches top tag definition, I take only top 3 based on accepted answers count. Not all users will have top tags, so questions are recommended to users who only has top tags.  After calculating, top tags for all the users results are stored in the cassandra database in the form of user to tags mapping, where tags is of collection type,
    2. Real Time Processing - Two types of records that can enter in the application in real time: Post or Vote. Each of these records is ingested through kafka to spark streaming, where spark streaming selects information it needs from the records. For post, it keeps post title and tags. Then, it gets user records from cassandra who have post tags as top tags. From user to tags mapping and title to tags mapping, User to title mapping is identified and these results are stored in cassandra.
whenever user enters the application by entering the userid, the application is redirected to user dashboard page which shows user performance number, favorite tags, trending tags in the application and recommended questions.
