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


# Get ETL teomjobgrade & tdimjobgrade
def get_teomjobgrade():
    #open cursor to dbdemo
    dbdemo_engine = sqlalchemy.create_engine(connection_uri)

    #query Text teomjobgrade
    sqltext = "select grade_code,gradecategory_code,grade_name from teomjobgrade"

    #execute sql 
    esql = pd.read_sql(sqltext,dbdemo_engine)

    #set data to dataframe
    df_esql = pd.DataFrame(esql)

    #insert into tdimjobgrade dbddwh
    df_esql.to_sql("tdimjobgrade",con = connection_dbdwh, if_exists = 'append', index = False)

    # ls_esql = df_esql.values.tolist()
    # return ls_esql

# def insert_dimteomjobgrade():

if __name__ == "__main__":
    get_teomjobgrade()
    print(get_teomjobgrade())
