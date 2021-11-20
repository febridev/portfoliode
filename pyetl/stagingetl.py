#import libraries
import mysql.connector
import os
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

# Check Connection Is Open
# if dbcondemo.is_connected():
#     print('Berhasil')

# #Close Connection
# dbcondemo.close()

#Check Connection Is Closed
# if dbcondemo.is_connected() == False:
#     print('Close')


#Function Get Data From DB Demo Table 
def get_teomjobgrade():
    cursor = dbcondemo.cursor()
    sqltext = "select * from dbdemo.teomjobgrade;"
    cursor.execute(sqltext)
    result = cursor.fetchall()
    dbcondemo.close()
    return result

if __name__ == '__main__':
    for data in get_teomjobgrade():
        print(data[0])
    