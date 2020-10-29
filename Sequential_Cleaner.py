#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
from pandas import read_csv
import time



# In[2]:



df = read_csv('/home/prateek/2000000.csv')
Attribute_select = input("Enter the attribute name for outlier detection") 
boxplot = df.boxplot(column=[Attribute_select])
#plt.hist(df[Attribute_select])
#plt.show()
lower_bound = input("Enter lower bound for the values in column-"+Attribute_select+" ")
lower_bound = float(lower_bound)
upper_bound = input("Enter upper bound for the values in column-"+Attribute_select+" ")
upper_bound = float(upper_bound)
choice = input("Enter the choice for outlier 1: Replace with average 2:Remove: ")
choice = int(choice)
if choice<1 or choice>2:
    print('Invalid Entry')
conflicting_attr1 = input("Enter 1st attribute name for conflict resolution: ")
conflicting_attr2 = input("Enter 2nd attribute name for conflict resolution: ")

most_pop = []
for x in df:
    most_pop.append(df[x].value_counts().nlargest(n=1).index[0])

start = time.perf_counter()
get_count = df.groupby([conflicting_attr1,conflicting_attr2]).size().reset_index().rename(columns={0:'count'})
get_conflict =get_count[get_count.groupby(conflicting_attr1)[conflicting_attr1].transform('size') > 1]
arr = get_conflict[conflicting_attr1].unique()
#thread_count = input("Data cleaning will start now.Enter number of threads to create")
#thread_count = int(thread_count)

# In[6]:


#ls =np.array_split(df, thread_count)
#dataframe=ls
    
def data_cleaner(data,tid):
    temp =0
    threadId =tid
    tid = str(tid)
    true_index = (lower_bound < data[Attribute_select].values) & (data[Attribute_select].values < upper_bound)
    if choice==1:
        false_index = ~true_index
        data[Attribute_select][false_index] = data[Attribute_select].mean()
    if choice==2:
        data = data[true_index]


    for x in data:
        data[x].fillna(most_pop[temp], inplace = True)
        temp=temp+1
    

    for y in arr: 
        df1= data[conflicting_attr2].where(data[conflicting_attr1]== y)
        most_pop_city = df1.value_counts().nlargest(n=1).index[0]
        true_index1 = data[conflicting_attr2] != most_pop_city
        true_index2 =data[conflicting_attr1] == y
        true_index3 = true_index1 & true_index2
        data.City[true_index3]= most_pop_city
        
    data['Date']=pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].dt.strftime("%d/%m/%Y")
    data['Date']=pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].dt.strftime("%m/%d/%Y")
    data['Date']=pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].dt.strftime("%d/%m/%Y")
    #dataframe[threadId]= data#.to_csv('/home/prateek/cleaned_data'+tid+'.csv')
    #data['Unitprice']= data['Unitprice']+1
    df=data
    print(f'executed')


#b = [0,1,2,3,4]
#for a in b:
a=1
data_cleaner(df,a)


#number = range(5)
#res = dataframe[0]
#for a in number:
#   if a<4:
#      res = pd.concat([res,dataframe[a+1]], axis=0, join='outer', ignore_index=True)


#ti = res['InvoiceID'].duplicated(keep = False)
#res = res[~ti]
df = df.drop_duplicates(subset ='InvoiceID',keep = 'first')
#df['Date']=pd.to_datetime(df['Date'])
df.sort_values(by=['InvoiceID'])
df.to_csv('/home/prateek/Desktop/CleanedData/Cleaned_Sequential.csv',index=False)
	

finish = time.perf_counter()
print(f'Finished in{round(finish-start,2)} seconds')





