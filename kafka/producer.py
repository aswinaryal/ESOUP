from kafka import KafkaConsumer, KafkaProducer
import json
import yaml
import random
import threading, logging, time

def main(): 
   producer = KafkaProducer(bootstrap_servers='localhost:9092')
   jsonFile = open("realdata.json", 'r')
   count = 0
 
   while True:
     for line in jsonFile:
        producer.send('stackoverflow', line)
        count = count + 1 
        print ("sent New Post"+line)

if __name__ == "__main__":
   main()
