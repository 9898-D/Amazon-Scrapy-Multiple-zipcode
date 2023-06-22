import pymysql
import numpy as np
from ugam_amazon_geo.config import *

db_name = 'ugam_amazon_geo'
db_host = '192.168.1.157'
Table_name = product_table
print("Choise Query to Pending data:")



print("Choise-1", f"select Id from {Table_name} where status='Pending'")
# print("Choise-2", f"select Id from {Table_name} where status='Pending' and id between no_start and no_end")
Choise = 1

if int(Choise) == 1:
    Query = f"select Id from {Table_name} where status='Pending'"
if int(Choise) == 2:
    print("Enter start and end id in input part")
    no_start = input("Enter Start_id:")
    no_end = input("Enetr End_id:")
    Query = f"select Id from {Table_name} where status='Pending' and id between {no_start} and {no_end}"
    # print(Query)

Query = Query

db_host = db_host
db_user = 'root'
db_passwd = 'xbyte'
db_name = db_name
Table_name = Table_name

con = pymysql.connect(host=db_host, user=db_user, passwd=db_passwd, database=db_name, autocommit=True,use_unicode=True, charset="utf8")

cursor = con.cursor()
cursor.execute(Query)
# Query = cursor.fetchall()
print(Query)
core_list = [column for column in cursor.fetchall()]
print(core_list)
C_ids = []
for itm in core_list:
    id = itm[0]
    C_ids.append(id)

Spider_Name = 'AmazonGeo'
Store_Name = 'scrapy'
print(C_ids, type(C_ids))
print("Total Pending",len(C_ids))
temp = 0
l = len(C_ids)
start = True

n = int(input("Enter the Part:"))
d = round(l / n)
x_ids = np.array_split(C_ids, n)

pending_bat = []
pending_bat.append("taskkill /im amazon_5.exe")
counter_timeout = 1
for parts in x_ids:
    commands = f'start amazon_5 crawl {Spider_Name} -a start={parts[0]} -a end={parts[-1]}'
    pending_bat.append(commands)
    if counter_timeout%10 == 0:
        pending_bat.append("timeout 10")
    counter_timeout+=1
    # print(commands)

pending_parts = "\n".join(pending_bat)
print("G",pending_parts)
# print(len(pending_parts))

bat_path = r'D:\Dhruv_Choubisa_Amazon_US_GEO\ugam_amazon_geo\ugam_amazon_geo\spiders\amazon.bat'

# bat_path1=bat_path.replace('\','/')

# Bat_FileName = input("Enter the Bat File Name:")
# filename = f'/{Bat_FileName}.bat'
# path = bat_path + filename
path = bat_path
path = path.replace("\\", "/")
with open(path, 'w') as f:
    f.write(pending_parts)
