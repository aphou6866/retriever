#!/usr/bin/env python3

# Taiwan Stock Retrieving programs.
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


TWSE_BASE_URL = 'http://www.twse.com.tw/'
TPEX_BASE_URL = 'http://www.tpex.org.tw/'
MOPS_BASE_URL = 'http://mops.twse.com.tw/'

def is_downloaded(fn):
    if os.path.isfile(fn)==True:
        
        with open(fn) as f:
            try:
                j= json.load(f)
            except ValueError as e:
                return False        
        if 0 < len(j['data']):
            return True
    
    else:
        return False


def retrieve_indices(name, code, bgn):
    os.system("mkdir -p database/raw/tw_indices/"+code)
    dm= "database/raw/tw_indices/%s/dummy"%(code)
    print("Retrieving indices %s %s..."%(name, code))
    if os.path.isfile(dm)== True:
        return
    
    end=  datetime.today() - timedelta(days=27)
    ds= end.strftime("%Y-%m")+"-01"
    fn1= "database/raw/tw_indices/%s/%s.json"%(code, ds)
    #os.system("rm -rf "+ fn1)
    end=  datetime.today()
    ds= end.strftime("%Y-%m")+"-01"
    fn2= "database/raw/tw_indices/%s/%s.json"%(code, ds)
    #os.system("rm -rf "+ fn2)
    
    day= timedelta(days=1)
    t= end
    et= datetime.strptime( bgn, '%Y-%m-%d')-day
    while et<=t:
        ds= t.strftime("%Y-%m-%d")
        retry=False
        if ds.split('-')[2]=="01":
            #print(ds)
            fn= "database/raw/tw_indices/%s/%s.json"%(code, ds)
            ds= t.strftime("%Y%m%d")
            addr= TWSE_BASE_URL +"indicesReport/%s?response=json&date=%s"%(code, ds)
            
            if is_downloaded(fn)== False or fn== fn1 or fn==fn2:
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel MS OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
                print("%s --- %s"%(fn,addr))
                httpData= requests.get( addr, headers=headers, timeout=10)
                try:
                    json.loads(httpData.text)
                except ValueError as e:
                    httpData.status_code= 0
                    
                if httpData.status_code != 200:
                    print("Data retrieve FAIL (network)")
                    with open(dm, "w") as f:
                        f.write("");
                    return # May reach 
                elif 100 < len(httpData.content):
                    with open(fn, "w") as f:
                        f.write(httpData.text);
                else:
                    print("Data retrieve FAIL: %d bytes"%( len(httpData.content) ))
                    with open(dm, "w") as f:
                        f.write("");
                    return # May reach 
                time.sleep(random.randint(1,5))
        if retry==False:
            t -= day
    
    with open(dm, "w") as f:
        f.write("");


def retrieve_stocks(name, code, bgn):
    os.system("mkdir -p database/raw/tw_stocks/"+code)
    dm= "database/raw/tw_stocks/%s/dummy"%(code)
    print("Retrieving stock %s %s..."%(name, code))
    if os.path.isfile(dm)== True:
        return
    
    end=  datetime.today() - timedelta(days=27)
    ds= end.strftime("%Y-%m")+"-01"
    fn1= "database/raw/tw_stocks/%s/%s.json"%(code, ds)
    os.system("rm -rf "+ fn1)
    end=  datetime.today()
    ds= end.strftime("%Y-%m")+"-01"
    fn2= "database/raw/tw_stocks/%s/%s.json"%(code, ds)
    os.system("rm -rf "+ fn2)
    
    day= timedelta(days=1)
    t= end
    et= datetime.strptime( bgn, '%Y-%m-%d')-day
    while et<=t:
        ds= t.strftime("%Y-%m-%d")
        retry=False
        if ds.split('-')[2]=="01":
            #print(ds)
            fn= "database/raw/tw_stocks/%s/%s.json"%(code, ds)
            ds= t.strftime("%Y%m%d")
            addr= TWSE_BASE_URL +"exchangeReport/STOCK_DAY?date=%s&stockNo=%s&response=json"%(ds, code)
            
            if is_downloaded(fn)== False or fn== fn1 or fn== fn2:
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel MS OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
                print("%s --- %s"%(fn,addr))
                httpData= requests.get( addr, headers=headers, timeout=10)
                try:
                    json.loads(httpData.text)
                except ValueError as e:
                    httpData.status_code= 0
                    
                if httpData.status_code != 200:
                    print("Data retrieve FAIL (network)")
                    with open(dm, "w") as f:
                        f.write("");
                    return # May reach 
                elif 100 < len(httpData.content):
                    with open(fn, "w") as f:
                        f.write(httpData.text);
                else:
                    print("Data retrieve FAIL: %d bytes"%( len(httpData.content) ))
                    with open(dm, "w") as f:
                        f.write("");
                    return # May reach 
                time.sleep(random.randint(1,5))
        if retry==False:
            t -= day
    
    with open(dm, "w") as f:
        f.write("");


def retrieve_finance(name, code, bgn):
    os.system("mkdir -p database/raw/tw_statements/"+code)
    
    end=  datetime.today().strftime("%Y-%m-%d")
    day= timedelta(days=1)
    t= datetime.strptime( end, '%Y-%m-%d')
    et= datetime.strptime( bgn, '%Y-%m-%d')-day
    print("Retrieving finance statment %s %s..."%(name, code))
    while et<=t:
        ds= t.strftime("%Y-%m-%d")
        retry=False
        dd= ds.split('-')
        if int(dd[1])%4== 0 and dd[2]=="01":
            #print(ds)
            fn= "database/raw/tw_statements/%s/finance_%s.html"%(code, ds)
            ds= t.strftime("%Y%m%d")
            yy= int(dd[0])- 1911
            sn= int(dd[1])/4
            
            if 101 < yy:
                addr= MOPS_BASE_URL +"mops/web/ajax_t164sb04"
                parm= "encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=%s&year=%d&season=0%d"%(code, yy, sn)
            else:
                addr= MOPS_BASE_URL +"mops/web/ajax_t05st32"
                parm= "encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=%s&year=%d&season=0%d"%(code, yy, sn)
                
            if os.path.isfile(fn)==False:
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel MS OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
                print("%s --- %s  with\n         %s"%(fn, addr, parm))
                
                httpData= requests.post( addr, headers=headers, data=parm)
                if httpData.status_code != 200:
                    print("Data retrieve FAIL (network)")
                    return # May reach 
                elif 4096 < len(httpData.content):
                    with open(fn, "w") as f:
                        f.write(httpData.text);
                else:
                    print("Data retrieve FAIL: %d bytes"%( len(httpData.content) ))
                    return # May reach 
                time.sleep(random.randint(1,5))
        if retry==False:
            t -= day
            
            
def retrieve_balance(name, code, bgn):
    os.system("mkdir -p database/raw/tw_statements/"+code)
    
    end=  datetime.today().strftime("%Y-%m-%d")
    day= timedelta(days=1)
    t= datetime.strptime( end, '%Y-%m-%d')
    et= datetime.strptime( bgn, '%Y-%m-%d')-day
    print("Retrieving balance sheet %s %s..."%(name, code))
    while et<=t:
        ds= t.strftime("%Y-%m-%d")
        retry=False
        dd= ds.split('-')
        if int(dd[1])%4== 0 and dd[2]=="01":
            #print(ds)
            fn= "database/raw/tw_statements/%s/balance_%s.html"%(code, ds)
            ds= t.strftime("%Y%m%d")
            yy= int(dd[0])- 1911
            sn= int(dd[1])/4
            
            if 101 < yy:
                addr= MOPS_BASE_URL +"mops/web/ajax_t164sb03"
                parm= "encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=%s&year=%d&season=0%d"%(code, yy, sn)
            else:
                addr= MOPS_BASE_URL +"mops/web/ajax_t05st31"
                parm= "encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=%s&year=%d&season=0%d"%(code, yy, sn)

            if os.path.isfile(fn)==False:
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel MS OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
                print("%s --- %s  with\n         %s"%(fn, addr, parm))
                
                httpData= requests.post( addr, headers=headers, data=parm)
                if httpData.status_code != 200:
                    print("Data retrieve FAIL (network)")
                    return # May reach 
                elif 1024 < len(httpData.content):
                    with open(fn, "w") as f:
                        f.write(httpData.text);
                else:
                    print("Data retrieve FAIL: %d bytes"%( len(httpData.content) ))
                    return # May reach 
                time.sleep(random.randint(1,5))
        if retry==False:
            t -= day
            

# =================================================================================================================================================================================================


def import_raw_data(tag, name, code, bgn):
    
    ndf= pds.DataFrame([],index=[])
    ndf= pds.DataFrame([],index=[])
    idx= ndf.index
    print("Importing raw data of %s"%name)
    
    day= timedelta(days=1)
    t= datetime.today()
    et= datetime.strptime( bgn, '%Y-%m-%d')-day
    while et<=t:
        ds= t.strftime("%Y-%m-%d")
        if ds.split('-')[2]=="01":
            if 'indices' in tag:
                fn= "database/raw/tw_indices/%s/%s.json"%(code, ds)
            else:
                fn= "database/raw/tw_stocks/%s/%s.json"%(code, ds)
            k=[]
            item=[]
            di=-1
            if os.path.isfile(fn)==True:
                print(fn)
                with open(fn) as f:
                    raw= json.load(f)
                
                for i in range(0, len(raw['fields'])):
                    fld = raw['fields'][i]
                    if "日期" not in fld:
                        it= "%s#%s"%(code, fld)
                        if it not in ndf:
                            ndf[it]= []
                            k.append(i)
                    else:
                        it='none'
                        di= i
                    item.append(it)
                    
                for d in raw['data']:
                    dt= d[di]
                    dd= dt.split('/')
                    ds="%d-%s-%s"%(int(dd[0])+1911, dd[1],dd[2])
                    if ds not in idx:
                        ndf.loc[ds]='NaN'
                    for i in range(0, len(d)):
                        if i !=di:
                            it= item[i]
                            if '0.0' in d[i]:
                                ndf[it][ds]=0.0
                            elif '--' in d[i]:
                                ndf[it][ds]='NaN'
                            else:
                                ndf[it][ds]=float(d[i].replace(',',''))
            t=t - timedelta(days=25)
            #print(i)
            #print(ndf)
            #print(item)
            #return
        t= t- day
    
    dfFn="database/dfm/tw_"+ tag + ".xlsx"
    if os.path.isfile(dfFn)==False:
        ndf.to_excel(dfFn)
        dfFn="database/dfm/tw_"+ tag + ".csv"
        ndf.to_csv(dfFn)
    else:
        odf= pds.read_excel(dfFn)
        for it in item:
            if it in odf:
                del odf[it]
        df= pds.concat( [odf, ndf], axis=1, sort=False)
        df.to_excel(dfFn)
        dfFn="database/dfm/tw_"+ tag + ".csv"
        df.to_csv(dfFn)
    #print(ndf)
    #odf.to_excel(dfFn)
    #dfFn="database/dfm/"+ tag + ".csv"
    #odf.to_csv(dfFn)
    return


# =================================================================================================================================================================================================



# Taiwan EFTs : https://www.jihsun.com.tw/md/event/jsun_school/ETF_stock10.html

# retrieve_tw_market.py indices  tw50 TAI50I 2008-01-01

# retrieve_tw_market.py stocks  TSMC 2330  2018-01-01

# retrieve_tw_market.py statement  TSMC 2330  2017-01-01

# retrieve_tw_market.py import  indices weigh MI_5MINS_HIST 2008-01-01

# retrieve_tw_market.py import  indices ./database/tw_indices.json 2008-01-01

# retrieve_tw_market.py import  etfs weigh 0050 2008-01-01

# retrieve_tw_market.py import  etfs ./database/tw_etfs.json 2008-01-01


# retrieve_tw_market.py indices ./database/tw_indices.json 2008-01-01

# retrieve_tw_market.py stocks  ./database/tw_stocks.json 2008-01-01

# retrieve_tw_market.py statements  ./database/tw_stocks.json 2008-01-01



if __name__ == '__main__':
    
    if len(sys.argv) ==5 and sys.argv[1]=="indices":
        argv= sys.argv
        retrieve_indices(argv[2], argv[3], argv[4])
    
    elif len(sys.argv) ==5 and sys.argv[1]=="stocks":
        argv= sys.argv
        retrieve_stocks(argv[2], argv[3], argv[4])
    
    elif len(sys.argv) ==5 and sys.argv[1]=="statement":
        argv= sys.argv
        retrieve_balance(argv[2], argv[3], argv[4])
        retrieve_finance(argv[2], argv[3], argv[4])
    
    elif len(sys.argv) ==6 and sys.argv[1]=="import":
        argv= sys.argv
        import_raw_data(argv[2], argv[3], argv[4], argv[5])
    
    elif len(sys.argv) ==4 and sys.argv[1]=="indices":
        argv= sys.argv
        with open(sys.argv[2]) as f:
            codeList= json.load(f)
        for code in codeList:
            retrieve_indices(codeList[code]["name"], code, argv[3])
    
    elif len(sys.argv) ==4 and sys.argv[1]=="stocks":
        argv= sys.argv
        with open(sys.argv[2]) as f:
            codeList= json.load(f)
        for code in codeList:
            retrieve_stocks(codeList[code]["name"], code, argv[3])
    
    elif len(sys.argv) ==4 and sys.argv[1]=="statements":
        argv= sys.argv
        with open(sys.argv[2]) as f:
            codeList= json.load(f)
        for code in codeList:
            retrieve_balance( codeList[code]["name"], code, argv[3])
            retrieve_finance( codeList[code]["name"], code, argv[3])
     
    elif len(sys.argv) ==5 and sys.argv[1]=="import":
        argv= sys.argv
        argv= sys.argv
        with open(sys.argv[3]) as f:
            codeList= json.load(f)
        for code in codeList:
            import_raw_data( sys.argv[2], codeList[code]["name"], code, argv[4])

    else:
        print("Usage:")
        print("  %s indices [name] [code] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s indices [json] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s stocks [name] [code] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s stocks [json] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s statements [name] [code] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s statements [json] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s import indices [name] [code] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s import indices [json] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s import etfs [name] [code] [ start:yyyy-mm-dd]"%sys.argv[0])
        print("  %s import etfs [json] [ start:yyyy-mm-dd]"%sys.argv[0])
        
        
