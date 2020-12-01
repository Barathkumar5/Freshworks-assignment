#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datastore as d
import json # since the input should be in json object as per the requirement 
            #changing the dict into json object.If its already json object then we can pass directly without conversion#
            

# In[2]:


a ={"name":"John", 
   "age":31, 
    "Salary":25000,
      } #dictionary 
  
# conversion to JSON done by dumps() function 
b = json.dumps(a) 
    
d.create("key1",b,20) # arguments are ( Key , value(must be a json object) , timelimit in secs )


# In[3]:


d.read("key1") # argument is key


# In[4]:


d.read("key1") #After the time limit overs


# In[5]:


d.delete("key1") #After the time limit overs


# In[6]:


d.create("key2",b,15)


# In[7]:


d.delete("key2") # argument is key


# In[8]:


t1=d.myThreadcreate("key5",b,0) #Multiple Threads for create option can be done by using this myThreadcreate class


# In[9]:


t2=d.myThreadread("key5") #Multiple Threads for read option can be done by using this myThreadread class


# In[10]:


t3=d.myThreaddelete("key5") # Multiple Threads for delete option can be done by using this myThreaddelete class


# In[11]:


t1.start() #create using thread


# In[12]:


t2.start() #read using thread


# In[13]:


t3.start() #delete using thread


# In[14]:


d.read("key5") # checking whether it is deleted or not.(successfully deleted hence the below message)


# In[17]:


d.finishprocess() #This is used to save the file after the processing of datastore. 

