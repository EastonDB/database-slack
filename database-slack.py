from sshtunnel import SSHTunnelForwarder
import slack
import pymysql.cursors
import os
import time
import pandas as pd
# verify access to internal database
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASS')
db_database = os.getenv('DB_ITIN')
ssh_key = os.getenv('KEY_LOC')
ssh_user = os.getenv('USER_ITIN')
ssh_host = os.getenv('HOST_ITIN')
time_string = time.strftime("%Y%m%d")
# add your file location
file_loc = ''

# confirm script starting
print("starting script")
# initialize ssh tunnel to remote server
server = SSHTunnelForwarder(
    (ssh_host, 1011),
    ssh_username=ssh_user,
    ssh_private_key=ssh_key,
    remote_bind_address=('127.0.0.1', 3306),
)
# confirmation that a connection has been made to the ssh tunnel
server.start()
print("connected")
# establish connect to mysql database
db = pymysql.connect(host="127.0.0.1",
                     user=db_user,
                     password=db_password,
                     port=server.local_bind_port,
                     database=db_database
                     )
# open saved sql file on my computer to open & run
sql_string = open('ADD SQL FILE LOCATION', 'r').read()
# return my sql query into a dataframe -> then write it into a csv -> adding the the location of my choice
# and YYYYMMDD to the file name removing the primary key column within the file
file = pd.read_sql_query(sql_string, db).to_csv(file_loc + time_string + 'report_name.csv', index=False)
# confirm that the file was created
print("file created")
# close and setup my connection to the server
db.close()
server.stop()

# Access and communicate with slack api
client = slack.WebClient(os.environ['ITINS_BOT'])
# create file name with date 
filename = time_string + 'report_name.csv'
filepath = file_loc + filename
# print the location my files have been written too
print("Results written to " + str(filepath))
# upload files to my slack instance
client.files_upload(
    channels='#reports',
    filetype='csv',
    filename=filepath,
    title='Report Name' + time_string,
    file=filepath
)
