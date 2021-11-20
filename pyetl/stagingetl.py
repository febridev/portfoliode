#import libraries
import mysql.connector
import os
from dotenv import load_dotenv

#load .env file
load_dotenv()

#Connection String dbdemo
db = mysql.connector.connect(
    host=os.getenv('hostdbdemo'),
    port=os.getenv('portdbdemo'),
    user=os.getenv('userdbdemo'),
    passwd=os.getenv('passdbdemo')
)

if db.is_connected():
    print('Berhasil')

#Close Connection
db.close()

if db.is_connected() == False:
    print('Close')

