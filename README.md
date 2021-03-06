# [ESOUP](http://esoup.tech): An Insight Data Science Project
*Extending Stack Overflow User Profile*

## Table of Contents
1. [Introduction](README.md#introduction)
2. [Data Source](README.md#data-source)
3. [AWS Clusters](README.md#aws-clusters)
4. [Data Pipeline](README.md#data-pipeline)
5. [How It Works](README.md#how-it-works)
6. [Presentation](README.md#presentation)

## Introduction
[Back to Table of Contents](README.md#table-of-contents)

The project aims to recommend newly posted questions in the application to the users who has expertise in the related domain of the question. The application also displays how user performance numbers like Upvotes, Downvotes are changing in real time and current trending tags in the application along with their counts getting updated in real time. A Dashboard is built to display all of this information.

## Data Source
[Back to Table of Contents](README.md#table-of-contents)

link - https://archive.org/details/stackexchange

Data Format - XML

Size - 200 GB approx

Complete Application data is spread across multiple file types:
<ul>
<li>Users.xml</li>
<li>Posts.xml</li>
<li>Votes.xml</li>
</ul>

## AWS Clusters
[Back to Table of Contents](README.md#table-of-contents)

ESOUP runs on 4 clusters on AWS:
<ul>
	<li>4 large nodes for Spark/Spark Streaming</li>
	<li>4 large nodes for Cassandra</li>
	<li>3 large nodes for kafka and Flask/Tornado</li>
</ul>

## Data Pipeline
[Back to Table of Contents](README.md#table-of-contents)

![alt text](images/pipeline.png?raw=true "Pipeline")

## How It Works
[Back to Table of Contents](README.md#table-of-contents)

Input data is in xml format with self closing tags, it is parsed in to csv format using scala XML library in distributed mode.

There are 2 major flows in the application:
<ul>
   <li>
   <h3>Batch Processing</h3>Historical dataset from stack overflow is stored in Hadoop File System(HDFS). Spark reads files from HDFS and calculates top 3 tags for each user if exists. How I defined top tag? a tag with at least 10 accepted answers in 
last 1 year will make it a top tag for that user. If there are more than 3 tags which satisfies top tag definition, I take   only top 3 based on accepted answers count. Not all users will have top tags, so questions are recommended to users who only has top tags.  After processing historical data, top tags of the users are stored in the cassandra database in the form of user to tags mapping, where tags is of collection type</li>
    <li>
    <h3>Real Time Processing</h3> Two types of records that can enter in the application in real time: Post or Vote. Each of    these records is ingested to different kafka topics, from which spark streaming reads records. Spark Streaming selects information that it needs from the input records. For post, it keeps post title and tags. Then, it gets user records from cassandra who have post tags as top tags. From user to tags mapping and title to tags mapping, User to title mapping is calculated and then results are stored in cassandra in the form user to questions mapping, where questions is a collection type.
    </li>
    </ul>
whenever user visits the ESOUP application and submits the userid, home page is redirected to user dashboard page which shows
user performance numbers, favorite tags, trending tags in the application and recommended questions.

## Presentation
[Back to Table of Contents](README.md#table-of-contents)

[Presentation](http://bit.ly/esoup) and Demo [Video](https://youtu.be/mzYfRnbpyuc) for ESOUP.
