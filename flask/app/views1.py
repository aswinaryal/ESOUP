from flask import Flask, render_template, request
from app import app
from flask import jsonify
from cassandra.cluster import Cluster

app = Flask(__name__)

cluster = Cluster(['ec2-52-43-50-72.us-west-2.compute.amazonaws.com'])
session = cluster.connect('stackoverflow')

@app.route('/')
def user():
   return render_template('home.html')

@app.route('/result',methods = ['POST', 'GET'])
def result():
   if request.method == 'POST':
      result = request.form['Name']
      stmt1 = " Select * from userprofile where id ='"+ str(userid) + "' ;"
      response1 = session.execute(stmt1)
      return render_template("dashboard.html",reponse1 = response1)

if __name__ == '__main__':
   app.run(debug = True)
