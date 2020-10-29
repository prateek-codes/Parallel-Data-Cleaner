#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
from pandas import read_csv
import time
import concurrent.futures


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
choice = input("Enter the choice for outlier 1: Replace with average 2:Remove")
choice = int(choice)
if choice<1 or choice>2:
    print('Invalid Entry')
conflicting_attr1 = input("Enter 1st attribute name for conflict resolution: ")
conflicting_attr2 = input("Enter 2nd attribute name for conflict resolution: ")
thread_count = input("Data cleaning will start now.Enter number of process to create")
thread_count = int(thread_count)
most_pop = []
for x in df:
    most_pop.append(df[x].value_counts().nlargest(n=1).index[0])

start = time.perf_counter()
get_count = df.groupby([conflicting_attr1,conflicting_attr2]).size().reset_index().rename(columns={0:'count'})
get_conflict =get_count[get_count.groupby(conflicting_attr1)[conflicting_attr1].transform('size') > 1]
arr = get_conflict[conflicting_attr1].unique()






# In[6]:


ls =np.array_split(df, thread_count)
    
def data_cleaner(data,tid,ch,ar,lb,ub,Atr_sel,cf1,cf2, mo_pop):
    temp=0
    threadId =tid
    tid = str(tid)
    true_index = (lb < data[Atr_sel].values) & (data[Atr_sel].values < ub)
    if ch==1:
        false_index = ~true_index
        data[Atr_sel][false_index] = data[Atr_sel].mean()
    if ch==2:
        data = data[true_index]


    for x in data:
        #mo_pop =data[x].value_counts().nlargest(n=1).index[0]
        data[x].fillna(mo_pop[temp], inplace = True)
        temp=temp+1
        
    

    for y in ar: 
        df1= data[cf2].where(data[cf1]== y)
        most_pop_city = df1.value_counts().nlargest(n=1).index[0]
        true_index1 = data[cf2] != most_pop_city
        true_index2 =data[cf1] == y
        true_index3 = true_index1 & true_index2
        data.City[true_index3]= most_pop_city
   
    #data['Unitprice']= data['Unitprice']+1
    data['Date']=pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].dt.strftime("%d/%m/%Y")
    data['Date']=pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].dt.strftime("%m/%d/%Y")
    data['Date']=pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].dt.strftime("%d/%m/%Y")
    print(f'Process{tid} executed')
    return data#.to_csv('/home/prateek/cleaned_data'+tid+'.csv')





with concurrent.futures.ProcessPoolExecutor() as executor:
    for _ in range(thread_count):
        results = [executor.submit(data_cleaner,ls[_],_,choice,arr,lower_bound,upper_bound,Attribute_select,conflicting_attr1,conflicting_attr2,most_pop)] 
count=0;
for p in concurrent.futures.as_completed(results):
    if count==0:
        res= p.result()
    count= count+1
c =0
next(concurrent.futures.as_completed(results))
for f in results:
    if c<(thread_count-1):
        res = pd.concat([res,f.result()])
    c=c+1

res = res.drop_duplicates(subset ='InvoiceID',keep = 'first')
#res['Date']=pd.to_datetime(res['Date'])
res.sort_values(by=['InvoiceID'])
res.to_csv('/home/prateek/Desktop/CleanedData/cleaned_parallel_multiprocess.csv',index=False)
finish = time.perf_counter()
print(f'Finished in{round(finish-start,2)} seconds')

