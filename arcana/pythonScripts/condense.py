import json
import requests
import urllib.request
import time
from bs4 import BeautifulSoup

urls = list()

with open('../rawdata/10_1_2019.txt') as f:
    json_object = json.load(f)
    for item in json_object:
        urls.append(item)
print("Requesting: " + urls[0])
try:
    page = urllib.request.urlopen(urls[0])
    soup = BeautifulSoup(page, 'html.parser')
    print(soup)
except:
    print("An error occured.")