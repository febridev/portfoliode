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

#ETL teomjobposition
def etl_teomjobposition():
    try:
                #open cursor to dbdemo
        dbdemo_engine = sqlalchemy.create_engine(connection_uri)
        #Test Connection dbdemo
        dbdemo_engine.connect()
        #query Text teomjobgrade
        sqltext1 = "select position_id, position_id, pos_code, pos_code, jobstatuscode from teomposition"
        #execute sql 
        esql1 = pd.read_sql(sqltext1,dbdemo_engine)
        #set data to dataframe
        df_esql1 = pd.DataFrame(esql1)

        #remove Duplicate
        df_esql1 = df_esql1.drop_duplicates()

        #insert into tdimemposition dbddwh
        try:
            #Test Connection dbdwh
            dbdwh_engine = sqlalchemy.create_engine(connection_dbdwh)
            dbdwh_engine.connect()
            df_esql1.to_sql("tdimempositon",con = connection_dbdwh, if_exists = 'append', index = False)
            print("Successfully Insert To tdimempositon")
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
    etl_teomjobposition()