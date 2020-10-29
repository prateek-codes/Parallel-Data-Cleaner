import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
from pandas import read_csv


USA = read_csv('/home/prateek/InputData/USA_sales500000.csv')
India = read_csv('/home/prateek/InputData/India_sales500000.csv')
#ist(USA.columns)
#ist(India.columns)
#for col in USA.columns: 
 #   print(col)
#for col in India.columns: 
 #   print(col)
ch1 = input('Do you want to convert value of any column to any specific standard 1.Yes, 2.No: ')
ch1 = int(ch1)
while(ch1==1):
    f= input('Select file for standardization 1:USA 2:India: ')
    f = int(f)
    if(f==1):
        attribute_convert =input('Enter the attribute name you want to standardise: ') 
        USA[attribute_convert]= USA[attribute_convert]*76
    if(f==2):
        attribute_convert =input('Enter the attribute name you want to standardise: ') 
        India[attribute_convert]= India[attribute_convert]/76
    ch1= input('Do you want to standardize other columns 1:Yes 2:No: ')
    ch1 = int(ch1)
ch2 = input ('Do you want to change the name of any attribute? 1:Yes 2:No')
ch2 = int(ch2)
while(ch2==1):
    f= input('Enter file name 1: USA 2:India, Enter attribute name: ')
    f=int(f)
    oldAttr= input('Enter attribute name you want to change')
    newAttr = input('Enter the new attribute value: ')
    if(f==1):
        USA.rename(columns = {oldAttr:newAttr}, inplace = True)
    if(f==2):
        India.rename(columns = {oldAttr:newAttr}, inplace = True)
    ch2 = input ('Do you want to change more attributes 1:Yes 2:No: ')
    ch2 = int(ch2)

    
USA.to_csv('/home/prateek/Int_USA.csv')
India.to_csv('/home/prateek/Int_India.csv')
res =pd.concat([USA,India], axis=0, join='outer', ignore_index=True)
res.to_csv('/home/prateek/InputData/IntegratedData_1000000.csv')
