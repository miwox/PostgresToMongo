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