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
     threadLock.acquire(1)
     created(self.key,self.valuejson,self.timeout) 
     threadLock.release()

def create(key,valuejson,timeout=0):
     t1=myThreadcreate(key,valuejson,timeout)
     t1.start()
    
def created(key,value,timeout=0):
    if key in datastore:
        print("error: this key already exists") #error message1
    else:
        if len(datastore)<(1024*1020*1024) and value.__sizeof__()<=(16*1024*1024): #constraints for file size less than 1GB and Jasonobject value less than 16KB 
            if timeout==0:
                l=[value,timeout]
            else:
                l=[value,time.time()+timeout]
            if len(key)<=32: #constraints for input key_name capped at 32chars
                    datastore[key]=l
            else:
                print("error: Memory limit exceeded!! ")#error message2
        else:
            print("error: Invalind key_name!! key_name must contain only alphabets")#error message3

#READ ##########################################################################################
class myThreadread (threading.Thread):
   def __init__(self,key):
      threading.Thread.__init__(self)
      self.key=key
   def run(self):
     threadLock.acquire(1)
     reade(self.key) 
     threadLock.release()

def read(key):
     t2=myThreadread(key)
     t2.start()
         
def reade(key):
    if key not in datastore:
        print("error: given key does not exist in database. Please enter a valid key") #error message4
    else:
        b=datastore[key]
        if b[1]!=0:
            if time.time()<b[1]: #comparing the present time with expiry time
                output=str(key)+":"+str(b[0]) 
                print (output)
            else:
                print("error: time-to-live of",key,"has expired") #error message5
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
     delete1(self.key) 
     threadLock.release()

def delete(key):
     t3=myThreaddelete(key) 
     t3.start()

def delete1(key):
    if key not in datastore:
        print("error: given key does not exist in database. Please enter a valid key") #error message4
    else:
        b=datastore[key]
        if b[1]!=0:
            if time.time()<b[1]: #comparing the current time with expiry time
                del datastore[key]
                print("key is successfully deleted")
            else:
                print("error: time-to-live of",key,"has expired") #error message5
        else:
            del datastore[key]
            print("key is successfully deleted")

       
def finishprocess():
    f= open("datastore25.txt","x")
    f.write(str(datastore))
    absolute_path = os.path.abspath("datastore1.txt")
    print("The file is saved in the following location:" + absolute_path)
    f.close()

