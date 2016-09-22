# jsonify creates a json representation of the response
from flask import jsonify
from flask import render_template
from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['ec2-52-43-50-72.us-west-2.compute.amazonaws.com'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('stackoverflow')

@app.route('/')
@app.route('/index')
def index():
       user = { 'nickname': 'Miguel' }  
       stmt = "SELECT * FROM trendingtags"
       response = session.execute(stmt)
       tag_list = []
       tag_count = []
       for val in response:
          tag_list.append(val.tagtitle)
	  tag_count.append(val.total)	
       return render_template("index.html", title='Trending Tags',user=user,tag_list = tag_list,tag_count=tag_count)

@app.route('/api/')
def get_trendingtags():
       stmt = "SELECT * FROM trendingtags"
       response = session.execute(stmt)
       response_list = []
       for val in response:
          response_list.append(val)
     
       jsonresponse = [{"Tag name": x.tagtitle, "Total": x.total} for x in response_list]
       return jsonify(tags=jsonresponse)
