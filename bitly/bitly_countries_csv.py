import sys
import bitly_api
import os
import datetime
from datetime import date
from config import config
import time
from datetime import timedelta
import csv
import json
import urllib
from urllib import urlopen
import ast

#vars
load_date = date.today()

#start_dt = date(2014,3,18) #custom dates
#end_dt = date(2014,3,24) #custom dates
start_dt = date.today()
end_dt = date.today()

tzOffset = "America/Los_Angeles"

#funcs
def keyCheck(key, arr, default):
    if key in arr.keys():
        return arr[key]
    else:
        return default

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
        
#convert python dictionary (array) into json
#used to convert links from gist to json        
def link_list(list, prefix):
    lst=[]
    for l in list:
        links={}
        links[prefix]=l
        lst.append(links)
    return lst

def get_links():
    rawGist = urlopen(config['bitlyLinksOnGithub'])
    links=rawGist.read().splitlines()
    #print links
    return links
    
pop_links = get_links()

# print pop_links

#create csv and write header row
header =  ['load_date', 'click_date', 'link', 'country', 'clicks']
fo = open(config['outputFile'],'ab')
wr = csv.DictWriter(fo, header)

#connect to bitly
conn_btly = bitly_api.Connection(access_token=config['ACCESS_TOKEN'])

#print pop_links

print str(len(pop_links)) + ' links to process'
print "###### Starting at " + str(datetime.datetime.now())
print "###### for dates " + str(start_dt) + " to " + str(end_dt)

while start_dt <= end_dt:
    #convert start_dt to epoch timestamp
    ts = int(time.mktime(start_dt.timetuple())) - time.timezone
    print "* " + time.strftime("%m/%d/%y", time.localtime(ts))
    i=0
    #links loop
    for pop_link in pop_links: 
        try:
            print "*** Processing link " + str(i) + ": " + pop_link
            #get link info
            link = conn_btly.link_info(pop_link)
            #get country clicks for dates
            cc = conn_btly.link_countries(pop_link, tz_offset=tzOffset, rollup=False, unit='day', unit_reference_ts=ts, units=1 )
            #row =[]
            
            #loop through country details
            for country in cc:
                rowData={}
                rowData['load_date']=load_date.strftime("%m/%d/%y")
                rowData['click_date']=time.strftime("%m/%d/%y", time.localtime(ts))
                rowData['link']=pop_link
                rowData['country']=keyCheck('country',country,'#unknown')
                rowData['clicks']=keyCheck('clicks',country,0)
                #print country
                #print rowData
                #row.append(links)
                #print rowData
                #r = [load_date.strftime("%m/%d/%y"), start_dt.strftime("%m/%d/%y"), pop_link['link'], keyCheck('country',country,'#unknown'), keyCheck('clicks',country,0)]
                #print r
                wr.writerow(rowData)
            i += 1
            #write out details
            #print row
            #wr.writerow(row)
            
        except:
            print "* couldn't load link " + str(i) + " - "+ pop_link +": due to error:"
            print sys.exc_info()[1]
    start_dt += datetime.timedelta(days=1)
        
fo.close()
print "###### Finished at " + str(datetime.datetime.now())
