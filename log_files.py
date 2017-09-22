import urllib, requests, patoolib, os, re, csv
from urllib2 import HTTPError
from patoolib.util import log_error, log_internal_error, PatoolError
from collections import Counter
from bs4 import BeautifulSoup
import pandas as pd
#Import the Beautiful soup functions to parse the data returned from the website

url = "http://www.secrepo.com/self.logs/"
local = r"/Users/kristi/Desktop/Logs/2017/"
r= requests.get(url)

#Parse the html in the content variable, and store it in Beautiful Soup format
soup = BeautifulSoup(r.content)

#Get the href content only
links = soup.find_all("a")

#Append ids and urls from the page into a list
id= []
for link in links:
    if "access.log" in link.get("href"):
        id.append(link.get("href"))
                
urls = ['http://www.secrepo.com/self.logs/access.log.2017-01-01.gz', 'http://www.secrepo.com/self.logs/access.log.2017-01-18.gz']        
#for i in xrange(len(id)):
#    urls.append('http://www.secrepo.com/self.logs/{}'.format(id[i]))

#Downloading files to local folder:
def download_data(data_url):
        response = urllib.urlopen(data_url)
        data = response.read()
        data_str = str(data)
        lines = data.split("\\n")
        dest_url = local + i[33:].encode('string-escape')
        if not os.path.exists(dest_url):
            fx = open(dest_url, "w")
            for line in lines:
                fx.write(line + "\n")
                try:
                    patoolib.extract_archive(dest_url, outdir=local) #extract gz file
                except PatoolError as msg:
                    pass
            fx.close()
            

for i in urls:
    download_data(i)


#Counting IP addresses:
def count_ip(spath):
    spath= local + i[33:-3].encode('string-escape')
    if os.path.exists(spath):
   	with open(spath) as f:
  		data = f.read()
    cnt = Counter()
    ipre = re.compile(r'^(?P<ip>(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])) - -')
    with open(spath) as infile:
        for line in infile:
            ip_count = []
            m = ipre.match(line)
            if m is not None:
                ip = m.groupdict()['ip']
                cnt[ip] += 1
    d_view = [ (v,k) for k,v in cnt.iteritems() ]
    d_view.sort(reverse=True) #sort by first element
    for v,k in d_view:
        ip_count.append([spath[43:], "%s" % k, "%d" % v])   
    ip_count_df = pd.DataFrame(ip_count, columns = ['Date','IP', 'Count'])
    #print ip_count_df
    filename = local + "IP_Count.csv"
    ip_count_df.to_csv(filename, index=False, mode='a', header=(not os.path.exists(filename))) #add new DataFrame data onto the end of an existing csv file
   
        
for i in urls:
    count_ip(i)
 









