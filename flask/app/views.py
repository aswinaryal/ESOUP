#jsonify creates a json representation of the response
from flask import jsonify
from flask import Flask, render_template, request
from app import app
from flask import jsonify
from cassandra.cluster import Cluster
import threading
import sys
sys.path.insert(0,'/home/ubuntu/kafka/')
import producer

cluster = Cluster(['ec2-52-39-76-176.us-west-2.compute.amazonaws.com'])
session = cluster.connect('stackoverflow')

@app.route('/')
def user():
   return render_template('home.html')

@app.route('/dashboard',methods = ['POST', 'GET'])
def result():
   threading.Thread(target=producer.execute).start()
   if request.method == 'POST':
      userid = request.form['userid']
      output = execute(userid)
      return render_template("dashboard.html",json_response=output)

@app.route('/dashboard/<userid>',methods = ['GET'])
def getdata(userid):
      output = execute(userid)
      return jsonify(output)

def execute(userid):
      stmt1 = " Select * from userprofile where id ='"+ str(userid) + "';"
      stmt2 = "Select tags from toptags where userid ='"+ str(userid)+ "';"
      stmt3 = "Select questions from usertoquestion where userid ='"+ str(userid)+ "';"
      stmt4 = "Select tagname,count from trendingtags limit 10"
      stmt5 = "Select upvotes from realtimeupvotes where id ='"+ str(userid) + "';"
      stmt6 = "Select downvotes from realtimedownvotes where id ='"+ str(userid) + "';"
      response1 = session.execute(stmt1)
      response2 = session.execute(stmt2)
      response3 = session.execute(stmt3)
      response4 = session.execute(stmt4)
      response5 = session.execute(stmt5)
      response6 = session.execute(stmt6)
      userprofile = []
      toptags = []
      trendingtags = []
      questions = []
      upvotes = []
      downvotes = []
      for val in response1:
         userprofile.append(val)
      for val in response2:
         toptags.append(val.tags)
      for val in response3:
         questions.append(val.questions)
      for val in response4:
         trendingtags.append(val)
      for val in response5:
	 upvotes.append(val)
      for val in response6:
	 downvotes.append(val)	
      top3tags = []
      questionslist = []
      i=0
      while (toptags and i< len(toptags[0])):
         top3tags.append(str(toptags[0][i]))
         i += 1
      i=0
      while(questions and i<len(questions[0])):
  	 questionslist.append(str(questions[0][i]))
         i+=1	
      userstats = [{"id": str(user.id), "displayname": str(user.displayname), "reputation": str(user.reputation), "upvotes": str(user.upvotes),"downvotes":str(user.downvotes)} for user in userprofile]
      tagcount = [{"tagname": str(t.tagname),"count":t.count} for t in trendingtags]
      return [userstats,top3tags,questionslist,tagcount,upvotes,downvotes]
      
