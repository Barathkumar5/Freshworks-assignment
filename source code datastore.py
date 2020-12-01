#!/usr/bin/env python
# coding: utf-8


import threading 
import time
import json
import os
datastore={} 
threadLock = threading.Lock()
#CREATION ####################################################################################
class myThreadcreate (threading.Thread):
   def __init__(self,key,valuejson,timeout=0):
      threading.Thread.__init__(self)
      self.key=key
      self.valuejson = json.loads(valuejson)
      self.timeout=timeout
   def run(self):
     threadLock.acquire(1) ####Thread safe#####
     c(self.key,self.valuejson,self.timeout) 
     threadLock.release()

def create(key,valuejson,timeout=0):
     t1=myThreadcreate(key,valuejson,timeout)
     t1.start()
    
def c(key,value,timeout=0):
    if key in datastore:
        print("Error: this key already exists") #error message1
    else:
        if len(datastore)<(1024*1020*1024) and value.__sizeof__()<=(16*1024*1024): #constraints for file size less than 1GB and Jasonobject value less than 16KB 
            if timeout==0:
                l=[value,timeout]
            else:
                l=[value,time.time()+timeout]
            if len(key)<=32: #constraints for input key_name capped at 32chars
                    datastore[key]=l
            else:
                print("Error: Key size is too large. It should be within 32 characters")#error message2
        else:
            print("Error: Data store memory(1GB) limit exceeded or json object size(16kb) limit exceeded ")#error message3

#READ ##########################################################################################
class myThreadread (threading.Thread):
   def __init__(self,key):
      threading.Thread.__init__(self)
      self.key=key
   def run(self):
     threadLock.acquire(1)
     r(self.key) 
     threadLock.release()

def read(key):
     t2=myThreadread(key)
     t2.start()
         
def r(key):
    if key not in datastore:
        print("error: given key does not exist in database. Please enter a valid key") #error message4
    else:
        b=datastore[key]
        if b[1]!=0:
            if time.time()<b[1]: #comparing the present time with expiry time
                output=str(key)+":"+str(b[0]) 
                print (output)
            else:
                print("Error: time-to-live of",key,"has expired") #error message5
        else:
            output=str(key)+":"+str(b[0])
            print(output)

#DELETE#######################################################################################
class myThreaddelete (threading.Thread):
   def __init__(self,key):
      threading.Thread.__init__(self)
      self.key=key
   def run(self):
     threadLock.acquire(1)
     d(self.key) 
     threadLock.release()

def delete(key):
     t3=myThreaddelete(key) 
     t3.start()

def d(key):
    if key not in datastore:
        print("Error: given key does not exist in database. Please enter a valid key") #error message4
    else:
        b=datastore[key]
        if b[1]!=0:
            if time.time()<b[1]: #comparing the current time with expiry time
                del datastore[key]
                print("key is successfully deleted")
            else:
                print("Error: time-to-live of",key,"has expired") #error message5
        else:
            del datastore[key]
            print("key is successfully deleted")

       
def finishprocess():
    f= open("datastorenew.txt","x")
    f.write(str(datastore))
    absolute_path = os.path.abspath("datastorenew.txt")
    print("The file is saved in the following location:" + absolute_path)
    f.close()

