"""
Python script for to extract the tables into binary .json files.
"""
import psycopg2
import json
import os

"""
Appends a given String to a given filename.
"""
def write_to_file(text, file_name):
    with open(os.path.join(os.getcwd() + '/out/', file_name), "a") as fp:
        fp.write(text+ "\n")


write_to_file(" /XXXXXXXX                                           /XX                        ", "logfile.txt")
write_to_file("| XX_____/                                          | XX                        ", "logfile.txt")
write_to_file("| XX       /XX   /XX  /XXXXXX   /XXXXXX   /XXXXXX  /XXXXXX    /XXXXXX   /XXXXXX ", "logfile.txt")
write_to_file("| XXXXX   |  XX /XX/ /XX__  XX /XX__  XX /XX__  XX|_  XX_/   /XX__  XX /XX__  XX", "logfile.txt")
write_to_file("| XX__/    \  XXXX/ | XX  \ XX| XX  \ XX| XX  \__/  | XX    | XXXXXXXX| XX  \__/", "logfile.txt")
write_to_file("| XX        >XX  XX | XX  | XX| XX  | XX| XX        | XX /XX| XX_____/| XX      ", "logfile.txt")
write_to_file("| XXXXXXXX /XX/\  XX| XXXXXXX/|  XXXXXX/| XX        |  XXXX/|  XXXXXXX| XX      ", "logfile.txt")
write_to_file("|________/|__/  \__/| XX____/  \______/ |__/         \___/   \_______/|__/      ", "logfile.txt")
write_to_file("                    | XX                                                        ", "logfile.txt")
write_to_file("                    | XX                                                        ", "logfile.txt")
write_to_file("                    |__/                                                        ", "logfile.txt")

#Connectiong to Postgres_DB and getting a cursor to run queries.
conn = psycopg2.connect(
    host="postgres_db",
    port=5432,
    database="dvdrental",
    user="postgres",
    password="1234")
cursor = conn.cursor()
print("Successfull connected to PostgresDB")
print("Start extracting tables to json files...")

#Executing an SQL query to collect all tablenames. Views will not be consideres by the query.
sql =  "SELECT tablename from pg_catalog.pg_tables WHERE schemaname = 'public'"
cursor.execute(sql)
table_names = []
for pair in cursor:
    table_names.append(pair[0])

#Iterating over the tablenames to export each table as a .json file /out directory.
for name in table_names:
    sql = f"Select array_to_json(array_agg(x)) From(Select * FROM {name})x"
    cursor.execute(sql)
    #array_agg() returns an array with a single element. Using fetchone and eliminate the element pair.
    json_data = json.dumps(cursor.fetchone()[0], indent=2)
    json_file_name = name + '.json'
    write_to_file(json_data, json_file_name)
    log_text = f"Extracting Select * From {name}; as json file."
    write_to_file(log_text, "logfile.txt")

print("Succesfully created .json files")
