#import libraries
import mysql.connector
import os
import pandas as pd
from dotenv import load_dotenv

#load .env file
load_dotenv()

# #Connection String dbdemo
dbcondemo = mysql.connector.connect(
    host=os.getenv('hostdbdemo'),
    port=os.getenv('portdbdemo'),
    user=os.getenv('userdbdemo'),
    passwd=os.getenv('passdbdemo')
)

# #Connection String dbdemo
dbcondwh = mysql.connector.connect(
    host=os.getenv('hostdbdwh'),
    port=os.getenv('portdbdwh'),
    user=os.getenv('userdbdwh'),
    passwd=os.getenv('passdbdwh')
)

# Check Connection Is Open
# if dbcondemo.is_connected():
#     print('Berhasil')

# #Close Connection
# dbcondemo.close()

#Check Connection Is Closed
# if dbcondemo.is_connected() == False:
#     print('Close')


#Function Get Data From DB Demo Table teomjobgrade
def get_teomjobgrade():
    cursor = dbcondemo.cursor()
    sqltext = "select grade_code,gradecategory_code,grade_name from dbdemo.teomjobgrade;"
    cursor.execute(sqltext)
    result = cursor.fetchall()
    dbcondemo.close()
    return result

#Function Insert Data To DB DWH Table tdimjobgrade
def insert_tdimjobgrade():
    pass


if __name__ == '__main__':
    # key = "grade_code"
    # rd = dict()
    for data in get_teomjobgrade():
        # rd[key] = data[0]
        print(data)
    