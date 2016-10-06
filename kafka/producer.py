from kafka import KafkaConsumer, KafkaProducer
import json
import yaml
import random
import threading, logging, time

def main(): 
   producer = KafkaProducer(bootstrap_servers='localhost:9092')
   jsonFile = open("posts.json", 'r')
   count = 0

  # while True:
   time.sleep(20)
   for line in jsonFile:
      if "upvotes" in line:
	  producer.send('votes',line)
	  print ("sent New upVotes"+line)
      elif "downvotes" in line:
	  producer.send('downvotes',line)
	  print("sent new downvotes"+line)
      else:	
          producer.send('posts', line)
	  print ("sent New Post"+line)
      count = count + 1
      if(count%10 == 0):	 
         time.sleep(3)
      if count>75:
         break

if __name__ == "__main__":
   main()
