# jsonify creates a json representation of the response
from flask import jsonify
from flask import Flask, render_template, request
from app import app
from flask import jsonify
from cassandra.cluster import Cluster

cluster = Cluster(['ec2-52-43-50-72.us-west-2.compute.amazonaws.com'])
session = cluster.connect('stackoverflow')

@app.route('/')
def user():
   return render_template('home.html')

@app.route('/dashboard',methods = ['POST', 'GET'])
def result():
   if request.method == 'POST':
      userid = request.form['userid']
      stmt1 = " Select * from userprofile where id ='"+ str(userid) + "' ;"
      stmt2 = "Select tags from toptags where userid ='"+ str(userid) + "' ;"	
      stmt3 = "Select questions from usertoquestion where userid ='"+ str(userid) + "' ;"
      stmt4 = "Select * from trendingtags"
      response1 = session.execute(stmt1)
      response2 = session.execute(stmt2)
      response3 = session.execute(stmt3)
      response4 = session.execute(stmt4)
      userprofile = []
      toptags = []
      trendingtags = []
      questions = []
      for val in response1:
         userprofile.append(val)
      for val in response2:
	 toptags.append(val.tags)
      for val in response3:
	 questions.append(val.questions)
      for val in response4:
	 trendingtags.append(val)
      json_response = [{"DisplayName": user.DisplayName, "Reputation": user.Reputation, "UpVotes": user.UpVotes,"DownVotes":user.DownVotes} for user in userprofile]
      json_response1 = [{"tagname": t.TagName,"count":t.Count} for t in trendingtags]
      return render_template("dashboard.html",user = json_response,trendingtags= json_response1,toptags=toptags,questions=questions)
