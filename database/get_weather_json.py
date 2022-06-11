import sqlalchemy as sqla
from sqlalchemy import create_engine
import traceback
import simplejson as json
from IPython.display import display
import pandas as pd
import requests
import traceback
import datetime
import time
from config import *


engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format(USER,PASSWORD,URL,PORT),echo=True)

# create dublin_bus table if not exist
sql = """
    CREATE DATABASE IF NOT EXISTS dublin_bus;
"""
engine.execute(sql)
# choose dublin bus table
sql1 = """
use dublin_bus;
"""
engine.execute(sql1)

# save data into raw txt
def write_to_file(text):
    # create new txt file in this path
    f = open("data/weather/weather__{}".format(now).replace(" ", "_"), "w")
    f.write(text)
    f.close()
    

# save data into database
def weather_to_db(text):
    # json analysis the text
    d_weather = json.loads(text)
    # get weather feature
    vals = (
        d_weather['dt'],d_weather['weather'][0]['description'],d_weather['weather'][0]['icon'],
        d_weather['main']['temp'], d_weather['main']['pressure'],d_weather['main']['humidity'],
        d_weather['visibility']
    )
    print(vals)
    # insert into weather table
    engine.execute("insert into weather values(%s, %s, %s, %s, %s, %s, %s)",vals)
    return


while True:
    try:
        # get request time
        now = datetime.datetime.now()
        # weather location
        lat = 53.343897
        lon = -6.29706
        # send request to the api
        r = requests.get(WEATHER, params={"lat": lat, "lon": lon,"appid": KEY})
        # save response to raw txt
        write_to_file(r.text)
        # save response to database
        weather_to_db(r.text)
        # 1 hour loop once
        time.sleep(60*60)
        
    except:
        print(traceback.format_exc())