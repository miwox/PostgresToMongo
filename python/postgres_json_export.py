# y%%y
import psycopg2
import json

# %%
#verbindung zur query
conn = psycopg2.connect(
    host="postgres_db",
    port=5432,
    database="dvdrental",
    user="postgres",
    password="1234")
cursor = conn.cursor()

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
    with open(name + '.json', 'w') as fp:
        fp.write(json_data)