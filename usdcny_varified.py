# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 15:29:34 2019

@author: chenchao
"""

import os
import math
import numpy as np
import pandas as pd

def read_list(path):
    csvs = []
    files = os.listdir(path)
    for i in files:
        if i.endswith('.csv'):
            csvs.append(path +"\\"+i)
    return csvs

def process_all(csvs):  
    total = len(csvs)
    count = 0
    results = []
    
    for i in csvs:
        if count == 0:
            df = pd.DataFrame(data=None,columns=['date','time','bid','ask','other1','other2','other3'])
        else:
            df = left_df
        data = pd.read_csv(i,header=None)
        columns_num = data.shape[1]
        if columns_num == 4:
            data.columns = ['date','time','bid','ask']
        elif columns_num == 5: 
            data.columns = ['date','time','bid','ask','other1']
        elif columns_num ==6:
            data.columns = ['date','time','bid','ask','other1','other2']
        elif columns_num ==7:
            data.columns = ['date','time','bid','ask','other1','other2','other3']
        else: 
            pass
        count += 1
        new_df = df.append(data,ignore_index=True,sort=False)
        left_df,res_list = process(new_df,count,total)  # df是未处理的部分,res是处理结果
        results.extend(res_list)
        print(f"\r已经完成{count}个csv,完成度{round(count/total*100,2)}%",end="")
    result_df = pd.DataFrame(data=results,columns=['start_time','end_time','mean_freq',"min_freq",
                             "max_freq","medium_freq",'bid_repeat_rate','ask_repeat_rate',"bid_stale_rate",
                             "ask_stale_rate",'count',"institute"])
    #result_df = result_df.dropna(axis="rows")
    print("结果的条数",len(result_df),"\n")
    result_df.to_csv(f"C:\\Users\\chenchao\\Desktop\\数据质量检查\\USDCNY&USDCNH\\USDCNY\\Results\\数据质量报告{total}.csv",index=False)

def process(df,count,total): 
    res_list = []
    # 每次接受一个csv文件读取的df,处理第一个日期的数据的数据,并返回最后一个日期的数据作为leaf_df  
    dates = df['date'].unique()
    if count == total:  # 这个if判断是为了保证处理到最后一个csv的数据时,不丢失;
        end_index = len(dates)
    else:
        end_index = len(dates) - 1
        
    for i in dates[:end_index]: 
        res = {"start_time":0,'end_time':0,"mean_freq":0,"min_freq":0,"max_freq":0,
               "medium_freq":0,"bid_repeat_rate":0,"ask_repeat_rate":0,"bid_stale_rate":0,
               "ask_stale_rate":0,"count":0,"institute":0}   
        each_day = df[df['date'].isin([i])]
        each_day = each_day.sort_values('time')
        
        res['start_time'] = str(each_day.values[0][0]) + " " + str(each_day.values[0][1])
        res['end_time'] = str(each_day.values[-1][0]) + " " + str(each_day.values[-1][1])
          
        f = lambda t : int(t[:2])*3600 + int(t[3:5])*60 + int(t[6:8])
        t = each_day['time'].apply(f)
        t1 = t[:-1]
        t2 = t[1:]
        freq = t2.values - t1.values
        try:
            res['min_freq'] = round(freq.min(),4)
            res['max_freq'] = round(freq.max(),4)
            res['mean_freq'] = round(freq.mean(),4)
            res['medium_freq'] = round(np.median(freq),4)
            
            each_day_total = len(each_day)
            bid_repeat = each_day_total - len(each_day['bid'].unique())
            res['bid_repeat_rate'] = "{}%".format(round(bid_repeat/each_day_total*100,2))
            ask_repeat = each_day_total - len(each_day['ask'].unique())
            res['ask_repeat_rate'] = "{}%".format(round(ask_repeat/each_day_total*100,2))
            bid_stale = sum(each_day['bid'][:-1].values-each_day['bid'][1:].values == 0)
            ask_stale = sum(each_day['ask'][:-1].values-each_day['ask'][1:].values == 0)
            res['bid_stale_rate'] = f"{round(bid_stale/each_day_total*100,2)}%"
            res['ask_stale_rate'] = f"{round(ask_stale/each_day_total*100,2)}%"
            
            res['count'] = len(each_day)
            res['institute'] = str(each_day['other1'].unique())+"|"+str(each_day['other2'].unique())+"|"+str(each_day['other3'].unique())
            res['institute'] = res['institute'].replace("|[nan]","")
            res['institute'] = res['institute'].replace("['","")
            res['institute'] = res['institute'].replace(r"']","")
            res['institute'] = res['institute'].replace(r"]","")
            res['institute'] = res['institute'].replace(r"'","")
            res['institute'] = res['institute'].replace(r"[nan","")
            res['institute'] = res['institute'].replace(r"nan]","")
            res['institute'] = res['institute'].replace(r"nan","")
            #print(res['institute'])
            res_list.append(res)
        except:
            pass
        #print("process函数结果",res)  each_day = df[df['date'].isin([i])]
    try:
        last_day = df [df['date'].isin([dates[end_index]])]
    except:
        last_day = pd.DataFrame(data=None,columns=df.columns)
    return last_day,res_list

if __name__=="__main__":
    path = r'C:\Users\chenchao\Desktop\数据质量检查\USDCNY&USDCNH\USDCNY'
    csvs = read_list(path)
    #csvs = csvs[:10]
    process_all(csvs)   # 最好指定一个结果存储的路径,以免重复运行本程序时读取了结果csv;  
