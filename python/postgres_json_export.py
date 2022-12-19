# y%%y
import psycopg2
import json
import os



# %%
    
def write_to_file(text, file_name):
    with open(os.path.join(os.getcwd() + '/out/', file_name), "a") as fp:
        fp.write(text+ "\n")

#verbindung zur query
conn = psycopg2.connect(
    host="postgres_db",
    port=5432,
    database="dvdrental",
    user="postgres",
    password="1234")


string ="  /XXXXXXXX                                           /XX                        "
write_to_file(string, "logfile.txt")
string =" | XX_____/                                          | XX                        "
write_to_file(string, "logfile.txt")
string =" | XX       /XX   /XX  /XXXXXX   /XXXXXX   /XXXXXX  /XXXXXX    /XXXXXX   /XXXXXX "
write_to_file(string, "logfile.txt")
string =" | XXXXX   |  XX /XX/ /XX__  XX /XX__  XX /XX__  XX|_  XX_/   /XX__  XX /XX__  XX"
write_to_file(string, "logfile.txt")
string =" | XX__/    \  XXXX/ | XX  \ XX| XX  \ XX| XX  \__/  | XX    | XXXXXXXX| XX  \__/"
write_to_file(string, "logfile.txt")
string =" | XX        >XX  XX | XX  | XX| XX  | XX| XX        | XX /XX| XX_____/| XX      "
write_to_file(string, "logfile.txt")
string =" | XXXXXXXX /XX/\  XX| XXXXXXX/|  XXXXXX/| XX        |  XXXX/|  XXXXXXX| XX      "
write_to_file(string, "logfile.txt")
string =" |________/|__/  \__/| XX____/  \______/ |__/         \___/   \_______/|__/      "
write_to_file(string, "logfile.txt")
string ="                     | XX                                                        "
write_to_file(string, "logfile.txt")
string ="                     | XX                                                        "
write_to_file(string, "logfile.txt")
string ="                     |__/                                                        "
write_to_file(string, "logfile.txt")



cursor = conn.cursor()
print("Successfull connected to PostgresDB")
print("Start extracting tables to json files...")
# %%
#SQL to get all base tablenames, in dvdrental there are also views.
sql =  "SELECT tablename from pg_catalog.pg_tables WHERE schemaname = 'public'"
cursor.execute(sql)

# %%
table_names = []
for pair in cursor:
    table_names.append(pair[0])

# %%
for name in table_names:
    sql = f"Select array_to_json(array_agg(x)) From(Select * FROM {name})x"
    cursor.execute(sql)
    # aggregate function return only one element, use fetchone and eliminate the pair
    json_data = json.dumps(cursor.fetchone()[0], indent=2)
    json_file_name = name + '.json'
    write_to_file(json_data, json_file_name)
    log_text = f"Extracting Select * From {name}; as json file."
    write_to_file(log_text, "logfile.txt")

print("Succesfully created .json files")