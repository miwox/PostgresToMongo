# %%
# importing module
from pymongo import MongoClient
client = MongoClient("mongodb://mongodb:27017/")

# %%
mydatabase = client['mongodb']

# %%
collection_names = mydatabase.list_collection_names()

# %%
collection = mydatabase['film']
cur = collection.aggregate([{'$count': "amount of movies"}])

print(cur.next())
