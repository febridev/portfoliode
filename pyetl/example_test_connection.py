import os
import sqlalchemy
import pandas as pd
from sqlalchemy.exc import InterfaceError, OperationalError, ProgrammingError
from dotenv import load_dotenv

#load .env file
load_dotenv()

#Credential DB Demo
hostdbdemo = os.getenv('hostdbdemo')
portdbdemo = os.getenv('portdbdemo')
userdbdemo = os.getenv('userdbdemo')
passdbdemo = os.getenv('passdbdemo')
dbdemoname = "dbnemu"
# dbdemoname = os.getenv('dbdemoname')

#Credential DB DWH
hostdbdwh = os.getenv('hostdbdwh')
portdbdwh = os.getenv('portdbdwh')
userdbdwh = os.getenv('userdbdwh')
# passdbdwh = os.getenv('passdbdwh')
passdbdwh = "kkk"
dbdwhname = os.getenv('dbdwhname')

#Open Connection DB Demo
connection_uri = "mariadb+mariadbconnector://"+userdbdemo+":"+passdbdemo+"@"+hostdbdemo+":"+portdbdemo+"/"+dbdemoname

#Open Connection DB DWH
#connection_dbdwh = "mariadb+mariadbconnector://"+userdbdwh+":"+passdbdwh+"@"+hostdbdwh+":"+portdbdwh+"/"+dbdwhname

dbdemo_engine = sqlalchemy.create_engine(connection_uri,pool_pre_ping=True)
# dbdemo_engine.connect()


try:
    t = dbdemo_engine.connect()
except OperationalError:
    print("Wrong Username Or Password!! (Error Code : OperationalError")
except InterfaceError:
    print("Host Not Found !! (Error Code : InterfaceError)")
except ProgrammingError:
    print("Database Not Found (Error Code : ProgrammingError)")



# sqltext = "select grade_code,gradecategory_code,grade_name from teomjobgrade"

# #execute sql 
# esql = pd.read_sql(sqltext,dbdemo_engine)
# pd.connOperationalError