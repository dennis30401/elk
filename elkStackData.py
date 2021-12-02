import requests
import json
import time
from elasticsearch import Elasticsearch
import schedule
import random
from datetime import datetime, timezone, timedelta

def get_humidity():
    humidity_URL = "http://192.168.0.196/things/temp-hum-light-1/properties"
    response = requests.get(humidity_URL)
    property_text = response.text
    property_json = json.loads(property_text)
    # print("humidity:" , property_json["hum"],end=' ')
    # print("light:" , property_json["light"],end=' ')
    # print("temp:" , property_json["temp"])
    # hum = random.randint(0,100)
    hum = property_json["hum"]
    return hum

def get_temp():
    temp_URL = "http://192.168.0.196/things/temp-hum-light-1/properties"
    response = requests.get(temp_URL)
    property_text = response.text
    property_json = json.loads(property_text)
    # print("humidity:" , property_json["hum"],end=' ')
    # print("light:" , property_json["light"],end=' ')
    # print("temp:" , property_json["temp"])
    # hum = random.randint(0,100)
    temp = property_json["temp"]
    return temp

def get_people():
    people_URL = "http://140.134.25.64:16666/properties"
    response = requests.get(people_URL)
    property_text = response.text
    property_json = json.loads(property_text)
    # print("number:",property_json["amount"])
    return property_json["amount"]
    # hum = random.randint(0,100)
    # return hum

def get_datetime():   # get UTC+8 time format
    """ return formatted time(YYYY-MM-DD HH:MM:SS UTC+8) """
    # set time zone(TW->UTC+8)
    tz = timezone(timedelta(hours=+8))
    # get the current time、 time zone, and convert to ISO format(to second) e.g. "2021-09-19T16:50:00+08:00"
    gltime = datetime.now(tz).isoformat(timespec="seconds")
    return gltime

def send_number_to_elastucSearch(es):
    fmDate = get_datetime()
    peopleNumber = get_people()
    datas = {
        "time":fmDate,
        "PeopleNum":peopleNumber
    }
    # print("number:",peopleNumber,type(peopleNumber),fmDate)
    es.index(index='opentest',body = datas)

def send_humidity_to_elastucSearch(es):
    fmDate = get_datetime()
    humidity = get_humidity()
    datas = {
        "time":fmDate,
        "humidity":humidity
    }
    # print("hum:",humidity,type(humidity),fmDate)
    es.index(index='humtest',body = datas)

def send_temp_to_elastucSearch(es):
    fmDate = get_datetime()
    temp = get_temp()
    datas = {
        "time":fmDate,
        "temp":temp
    }
    # print("hum:",humidity,type(humidity),fmDate)
    es.index(index='temptest',body = datas)    
    
def multipleSchedulers():
    """ Manually set the tasks you want to schedule """
    es = Elasticsearch(hosts='140.134.25.64', port=19200)
    scheduler1 = schedule.Scheduler()  # to send people number to es 
    scheduler2 = schedule.Scheduler()  # to send humidity to es
    scheduler3 = schedule.Scheduler()  # to send temp to es
    scheduler1.every().hour.at(":00").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":05").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":10").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":15").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":20").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":25").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":30").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":35").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":40").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":45").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":50").do(send_number_to_elastucSearch,es)
    scheduler1.every().hour.at(":55").do(send_number_to_elastucSearch,es)

    scheduler2.every().hour.at(":00").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":05").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":10").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":15").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":20").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":25").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":30").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":35").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":40").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":45").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":50").do(send_humidity_to_elastucSearch,es)
    scheduler2.every().hour.at(":55").do(send_humidity_to_elastucSearch,es)

    scheduler3.every().hour.at(":00").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":05").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":10").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":15").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":20").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":25").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":30").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":35").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":40").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":45").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":50").do(send_temp_to_elastucSearch,es)
    scheduler3.every().hour.at(":55").do(send_temp_to_elastucSearch,es)
    while True:
        scheduler1.run_pending()
        scheduler2.run_pending()
        scheduler3.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # print("start scheduler")
    multipleSchedulers()
    # no scheduler test
    # es = Elasticsearch(hosts='140.134.25.64', port=19200)
    # send_humidity_to_elastucSearch(es)
    # send_number_to_elastucSearch(es)
