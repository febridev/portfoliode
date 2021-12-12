import os
import sqlalchemy
#library for handle exception
from sqlalchemy.exc import InterfaceError, OperationalError, ProgrammingError
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


# ETL teomjobgrade & tdimjobgrade
def etl_teomjobgrade():
    try:
        #open cursor to dbdemo
        dbdemo_engine = sqlalchemy.create_engine(connection_uri)
        #Test Connection dbdemo
        dbdemo_engine.connect()
        #query Text teomjobgrade
        sqltext = "select grade_code,gradecategory_code,grade_name from teomjobgrade"
        #execute sql 
        esql = pd.read_sql(sqltext,dbdemo_engine)
        #set data to dataframe
        df_esql = pd.DataFrame(esql)
        
        #remove Duplicate
        df_esql = df_esql.drop_duplicates()

        #insert into tdimjobgrade dbddwh
        try:
            #Test Connection dbdwh
            dbdwh_engine = sqlalchemy.create_engine(connection_dbdwh)
            dbdwh_engine.connect()
            df_esql.to_sql("tdimjobgrade",con = connection_dbdwh, if_exists = 'append', index = False)
            print("Successfully Insert To tdimjobgrade")
            #Close Connection
            dbdemo_engine.dispose()
            dbdwh_engine.dispose()
        except OperationalError:
            print("Wrong Username Or Password!! dbdwh_engine (Error Code : OperationalError)")
            exit()
        except InterfaceError:
            print("Host Not Found !! dbdwh_engine (Error Code : InterfaceError)")
            exit()
        except ProgrammingError:
            print("Database Not Found !! dbdwh_engine (Error Code : ProgrammingError)")
            exit()
        
    except OperationalError:
        print("Wrong Username Or Password!! dbdemo_engine (Error Code : OperationalError)")
        exit()
    except InterfaceError:
        print("Host Not Found !! dbdemo_engine (Error Code : InterfaceError)")
        exit()
    except ProgrammingError:
        print("Database Not Found !! dbdemo_engine (Error Code : ProgrammingError)")
        exit()
    # ls_esql = df_esql.values.tolist()
    # def insert_dimteomjobgrade():

if __name__ == "__main__":
    etl_teomjobgrade()
