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
      response1 = session.execute(stmt1)
      userprofile = []
      for val in response1:
         userprofile.append(val)
	 json_response = [{"DisplayName": user.DisplayName, "Reputation": user.Reputation, "UpVotes": user.UpVotes,"DownVotes":user.DownVotes} for user in userprofile]
      return render_template("dashboard.html",user = json_response)
