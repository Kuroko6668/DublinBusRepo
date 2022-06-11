import sqlalchemy as sqla
from sqlalchemy import create_engine
import os
import simplejson as json
from IPython.display import display
import traceback
from config import *

engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format(USER,PASSWORD,URL,PORT),echo=True)

sql = """
    CREATE DATABASE IF NOT EXISTS dublin_bus;
"""
engine.execute(sql)

sql1 = """
use dublin_bus;

"""

# create columns of weather table
sql2 = """
    CREATE TABLE IF NOT EXISTS weather(
        dt VARCHAR(256),
        description VARCHAR(256),
        icon VARCHAR(256),
        temperture VARCHAR(256),
        pressure VARCHAR(256),
        humidity VARCHAR(256),
        visibility VARCHAR(256)
	);
"""

# weather data insert into database
def weather_to_db(text):
    d_weather = json.loads(text)
    vals = (
        d_weather['dt'],d_weather['weather'][0]['description'],d_weather['weather'][0]['icon'],
        d_weather['main']['temp'], d_weather['main']['pressure'],d_weather['main']['humidity'],
        d_weather['visibility']
    )
    print(vals)
    engine.execute("insert into weather values(%s, %s, %s, %s, %s, %s, %s)",vals)
    return

# create weather table
try:
    res = engine.execute(sql1)
    res = engine.execute("DROP TABLE IF EXISTS weather")
    res = engine.execute(sql2)
    print(res.fetchall())
except Exception as e:
    print(e)

# if there are raw txt in path "data/weather/", insert them into database
p = "data/weather/"
if(os.path.exists(p)):
    path_list = os.listdir(p)
    # enumerate all raw txt files 
    for i in range(len(path_list)):
        text = open(p + path_list[i] ,'r').read()
        weather_to_db(text)