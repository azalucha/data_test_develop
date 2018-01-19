
# coding: utf-8

# In[1]:


import requests
from lxml import etree
from datetime import datetime
import csv

################ user inputs ############
url='http://syndication.enterprise.websiteidx.com/feeds/BoojCodeTest.xml' #xml page of data to be parsed (str)
myyear=2016 #year to filter by (int)
#########################################


def filter_by_date(year,listing):           #filters listing by year
    for child in listing.find('ListingDetails'):
        if child.tag == "DateListed":
            date=child.text
            datetime_object = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            if datetime_object.year==year:
                return True, datetime_object, date
            else:
                return False, None, None
            
def extract_description(listing):       #extract first 200 characters if 'and' present in description
    for child in listing.find('BasicDetails'):
        if child.tag == 'Description':
            description=child.text
            description_split=description.split()
            if 'and' in description_split:
                hasand=True
                first2h=description[:200]
            else:
                hasand=False
                first2h=None
            return hasand,first2h
        
def get_required_fields(listing):
    MlsId,MlsName,Price=get_mls_price(listing)
    StreetAddress=get_addr(listing)
    Bedrooms,Bathrooms=get_bed_bath(listing)
    appliances,rooms=get_appliances_rooms(listing)
    return MlsId,MlsName,StreetAddress,Price,Bedrooms,Bathrooms,appliances,rooms
    

def get_mls_price(listing):
    for child in listing.find('ListingDetails'):
        if child.tag == "MlsId":
            MlsId=child.text
        elif child.tag == "MlsName":
            MlsName=child.text
        elif child.tag == "Price":
            Price=child.text
    return MlsId,MlsName,Price

def get_addr(listing):
    for child in listing.find('Location'):
        if child.tag == "StreetAddress":
            StreetAddress=child.text
    return StreetAddress
        
def get_bed_bath(listing):
    Bedrooms="0"
    Bathrooms=0 
    for child in listing.find('BasicDetails'): 
        if child.tag == "Bedrooms":
            Bedrooms=child.text
        elif child.tag == "FullBathrooms":
            if child.text is not None:
                Bathrooms+=int(child.text)
        elif child.tag == "HalfBathrooms":
            if child.text is not None:
                Bathrooms+=int(child.text)
        elif child.tag == "ThreeQuarterBathrooms":
            if child.text is not None:
                Bathrooms+=int(child.text)
    return Bedrooms,str(Bathrooms)

def get_appliances_rooms(listing):
    appliances=[]
    for child in listing.findall('RichDetails//Appliances//Appliance'):
        appliances.append(child.text)
    rooms=[]
    for child in listing.findall('RichDetails//Rooms//Room'):
        rooms.append(child.text)
    return appliances,rooms

def getKey(item):    #for sorting by date
    return item[-1]

################ Begin the procedure #######################
r = requests.get(url,  stream=True)
r.raw.decode_content = True
tree=etree.parse(r.raw)
root=tree.getroot()
allinfo=[]
for listing in root.findall('Listing'):
    isyear, datetime_object, date = filter_by_date(myyear,listing)   #filter listing by user-specified year
    hasand, first2h=extract_description(listing)  #extract first 200 characters if 'and' present in description
    if isyear and hasand:  #if the above conditions are met, proceed to extract the required fields
        MlsId,MlsName,StreetAddress,Price,Bedrooms,Bathrooms,appliances,rooms=get_required_fields(listing)
        info=[MlsId,MlsName,date,StreetAddress,Price,Bedrooms,Bathrooms]  #get all the required fields in order
        info.extend(appliances)
        info.extend(rooms)
        info.append(first2h)
        info.append(datetime_object) #for now, put datetime_object at the end so we can sort, then remove
        allinfo.append(info)  #make a list of lists for each entry
allinfo_sorted=sorted(allinfo, key=getKey)  #sort by date
info_sorted=[item[:-1] for item in allinfo_sorted] #get rid of datetime object
with open("output.csv", "wb") as f:  #output the csv file
    writer = csv.writer(f)
    writer.writerows(info_sorted)

