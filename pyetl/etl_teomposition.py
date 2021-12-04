import os
import sqlalchemy
import pandas as pd
from dotenv import load_dotenv

#load .env file
load_dotenv()

#Credential DB Demo
hostdbdemo = os.getenv('hostdbdemo')
portdbdemo = os.getenv('portdbdemo')
userdbdemo = os.getenv('userdbdemo')
passdbdemo = os.getenv('passdbdemo')
dbdemoname = os.getenv('dbdemoname')

#Credential DB DWH
hostdbdwh = os.getenv('hostdbdwh')
portdbdwh = os.getenv('portdbdwh')
userdbdwh = os.getenv('userdbdwh')
passdbdwh = os.getenv('passdbdwh')
dbdwhname = os.getenv('dbdwhname')

#Open Connection DB Demo
connection_uri = "mariadb+mariadbconnector://"+userdbdemo+":"+passdbdemo+"@"+hostdbdemo+":"+portdbdemo+"/"+dbdemoname

#Open Connection DB DWH
connection_dbdwh = "mariadb+mariadbconnector://"+userdbdwh+":"+passdbdwh+"@"+hostdbdwh+":"+portdbdwh+"/"+dbdwhname
