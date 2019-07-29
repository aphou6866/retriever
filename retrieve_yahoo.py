#!/usr/bin/env python3

# Global Market Retrieving programs.
# Design by Elliot Hou, Ao-ping 
# Date: Oct 11 2018 

import json
import os, sys
import time,random
import datetime
from datetime import datetime
from datetime import date, timedelta
import requests
import pandas as pds
from fake_useragent import UserAgent
import urllib




def retrieve_quoates(clas, name, code):
    
    parm= { "region":"US",
            "lang":"en-US",
            "includePrePost":"false",
            "interval":"1d",
            "range":"10y",
            "corsDomain": "finance.yahoo.com",
            ".tsrc": "finance"
        }
    
    rdt= datetime.today()
    ds= rdt.strftime("%Y-%m-%d")
    fn= "database/raw/yahoo/"+ clas+ "-"+ code+ "-"+ ds+ ".json"
    
    addr= "https://query1.finance.yahoo.com/v8/finance/%s?%s"%(urllib.parse.urlencode({'chart':code}).replace('=','/'),urllib.parse.urlencode(parm))
    print("%s --- %s"%(fn,addr))
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel MS OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    httpData= requests.get( addr, headers=headers, timeout=10)
    if httpData.status_code != 200:
        print("Data retrieve FAIL (network)")
    elif 8192 < len(httpData.content):
        with open(fn, "w") as f:
            f.write(httpData.text);
    else:
        print("Data retrieve FAIL: %d bytes"%( len(httpData.content) ))
        return
    time.sleep(random.randint(1,5))
    
    
    #if os.path.isfile(fn)== False:
        #addr= "https://query1.finance.yahoo.com/v8/finance/%s?%s"%(urllib.parse.urlencode({'chart':code}).replace('=','/'),urllib.parse.urlencode(parm))
        #print("%s --- %s"%(fn,addr))
        #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel MS OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        #httpData= requests.get( addr, headers=headers, timeout=10)
        #if httpData.status_code != 200:
            #print("Data retrieve FAIL (network)")
        #elif 8192 < len(httpData.content):
            #with open(fn, "w") as f:
                #f.write(httpData.text);
        #else:
            #print("Data retrieve FAIL: %d bytes"%( len(httpData.content) ))
            #return
        #time.sleep(random.randint(1,5))
    
    #fn= "database/raw/yahoo/"+ clas+ "-"+ code+ "-recent.json"
    #parm["range"]="1y"
    #addr= "https://query1.finance.yahoo.com/v8/finance/%s?%s"%(urllib.parse.urlencode({'chart':code}).replace('=','/'),urllib.parse.urlencode(parm))
    #print("%s --- %s"%(fn,addr))
    #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel MS OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    #httpData= requests.get( addr, headers=headers, timeout=10)
    #if httpData.status_code != 200:
        #print("Data retrieve FAIL (network)")
    #elif 8192 < len(httpData.content):
        #with open(fn, "w") as f:
            #f.write(httpData.text);
    #else:
        #print("Data retrieve FAIL: %d bytes"%( len(httpData.content) ))
        #return
    time.sleep(random.randint(1,5))


def import_raw_data(clas, name, code):

    ndf= pds.DataFrame([],index=[])
    ndf= pds.DataFrame([],index=[])
    idx= ndf.index
    print("Importing raw data of %s"%name)
    
    rdt= datetime.today()
    ds= rdt.strftime("%Y-%m-%d")
    ofn= "database/raw/yahoo/"+ clas+ "-"+ code+ "-"+ ds+ ".json"
    idx=[]
    if os.path.isfile(ofn)== True:
        with open(ofn) as f:
            njs= json.load(f)
            
        timestamp= njs['chart']['result'][0]['timestamp']
        tl= len(timestamp)
        for i in range(0, tl):
            ds= datetime.utcfromtimestamp(timestamp[i]).strftime('%Y-%m-%d')
            idx.append(ds)
        
        items=[]
        mtx= []
        for idc in njs['chart']['result'][0]['indicators']:
            for quo in njs['chart']['result'][0]['indicators'][idc][0]:
                items.append(code+'#'+quo)
                mtx.append(njs['chart']['result'][0]['indicators'][idc][0][quo])
        #print(item)
        ndf= pds.DataFrame(mtx, index=items, columns=idx).transpose()
        #print(ndf)
        
        dfFn="database/dfm/yho_"+ clas + ".xlsx"
        if os.path.isfile(dfFn)==False:
            ndf.to_excel(dfFn)
            dfFn="database/dfm/yho_"+ clas + ".csv"
            ndf.to_csv(dfFn)
        else:
            odf= pds.read_excel(dfFn)
            for it in items:
                if it in odf:
                    del odf[it]
            df= pds.concat( [odf, ndf], axis=1, sort=False)
            df.to_excel(dfFn)
            dfFn="database/dfm/yho_"+ clas + ".csv"
            df.to_csv(dfFn)

# retrieve_yahoo.py retrieve indices SPC "^GSPC"

# retrieve_yahoo.py import indices SPC "^GSPC"

# retrieve_yahoo.py retrieve indices database/yho_indices.json

# retrieve_yahoo.py import indices database/yho_indices.json


if __name__ == '__main__':
    
    argv= sys.argv
    if len(argv) ==5 and argv[1]=="retrieve":
        argv= argv
        retrieve_quoates(argv[2], argv[3], argv[4])
    
    elif len(argv) ==5 and argv[1]=="import":
        argv= argv
        import_raw_data(argv[2], argv[3], argv[4])
    
    elif len(argv) ==4 and argv[1]=="retrieve":
        argv= argv
        with open(argv[3]) as f:
            codeList= json.load(f)
        for code in codeList:
            retrieve_quoates(argv[2], codeList[code]["name"], code)
    
    elif len(argv) ==4 and argv[1]=="import":
        argv= argv
        with open(argv[3]) as f:
            codeList= json.load(f)
        for code in codeList:
            import_raw_data(argv[2], codeList[code]["name"], code)
        
    else:
        print("Usage:")
        print("  %s retrieve [class] [name] [code] "%argv[0])
        print("  %s retrieve [class] [list file(json)] "%argv[0])
        print("  %s import [class] [name] [code] "%argv[0])
        print("  %s import [class] [list file(json)] "%argv[0])
        
        
        
        
