"""
Python script for the mongo queries, creating the view and update delete.
"""
from pymongo import MongoClient
import hashlib
import os
import string
import random
import json
from time import localtime, strftime

"""
Generates a random password key. The key is sha256 encoded and uses a random salt. 
"""
def randomPassword():
    salt = os.urandom(32)
    password = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(15))
    key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    return key

"""
Writes a given String to the logfile.txt located in the /out directory.
"""
def write_to_file(text):
    with open(os.path.join(os.getcwd() + '/out/', "logfile.txt"), "a") as fp:
        fp.write(text+ "\n")

"""
Writes a query to the logfile.txt located in the /out directory. 
The function needs a task-String, a String to describe the used collection and function, 
an array of the query pipeline and an array of the query results.
"""
def write_query_to_file(task, collection_function, query_list, result):
    write_to_file(task)
    write_to_file("Corresponding MongoDB query:")
    write_to_file(collection_function + '(' + json.dumps(query_list, indent=1, ensure_ascii=False) + ')')
    write_to_file("Result of query:")
    write_to_file(json.dumps([q for q in result], indent=1, ensure_ascii=False))
    write_to_file("-------------------------------------------------------------------------------------------------")

write_to_file(" ▄▄▄▄▄▄▄▄▄▄▄  ▄         ▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄ ")
write_to_file("▐░░░░░░░░░░░▌▐░▌       ▐░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌")
write_to_file("▐░█▀▀▀▀▀▀▀█░▌▐░▌       ▐░▌▐░█▀▀▀▀▀▀▀▀▀ ▐░█▀▀▀▀▀▀▀█░▌ ▀▀▀▀█░█▀▀▀▀ ▐░█▀▀▀▀▀▀▀▀▀ ▐░█▀▀▀▀▀▀▀▀▀ ")
write_to_file("▐░▌       ▐░▌▐░▌       ▐░▌▐░▌          ▐░▌       ▐░▌     ▐░▌     ▐░▌          ▐░▌          ")
write_to_file("▐░▌       ▐░▌▐░▌       ▐░▌▐░█▄▄▄▄▄▄▄▄▄ ▐░█▄▄▄▄▄▄▄█░▌     ▐░▌     ▐░█▄▄▄▄▄▄▄▄▄ ▐░█▄▄▄▄▄▄▄▄▄ ")
write_to_file("▐░▌       ▐░▌▐░▌       ▐░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌     ▐░▌     ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌")
write_to_file("▐░█▄▄▄▄▄▄▄█░▌▐░▌       ▐░▌▐░█▀▀▀▀▀▀▀▀▀ ▐░█▀▀▀▀█░█▀▀      ▐░▌     ▐░█▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀█░▌")
write_to_file("▐░░░░░░░░░░░▌▐░▌       ▐░▌▐░▌          ▐░▌     ▐░▌       ▐░▌     ▐░▌                    ▐░▌")
write_to_file(" ▀▀▀▀▀▀█░█▀▀ ▐░█▄▄▄▄▄▄▄█░▌▐░█▄▄▄▄▄▄▄▄▄ ▐░▌      ▐░▌  ▄▄▄▄█░█▄▄▄▄ ▐░█▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄█░▌")
write_to_file("        ▐░▌  ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░▌       ▐░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌")
write_to_file("         ▀    ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀  ▀         ▀  ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀ ")

#Connectiong to the MongoDB.                                                     
client = MongoClient("mongodb://mongodb:27017/")
mydatabase = client['mongodb']

#Query A.
query_a = [
   {
     '$group': {
        '_id': 0,
        'Gesamtanzahl der verfügbaren Filme': { '$sum': 1 }
     }
   },
   {
      '$project': {
         '_id': 0
      }
   }
]
QUERY_A = mydatabase.inventory.aggregate(query_a)
write_query_to_file("a. Gesamtanzahl der verfügbaren Filme", "mydatabase.inventory.aggregate", query_a, QUERY_A)

#Query B.
query_b = [
    {"$group": {
        '_id': "$store_id",
        'film_set': {
            '$addToSet': "$film_id"
            }
        }
    },
    {"$project": {
        '_id': 0,
        'Store_ID': "$_id",
        'Verschiedene Filme': {
            '$size': "$film_set"
            }
        }
    }]
QUERY_B = mydatabase.inventory.aggregate(query_b)
write_query_to_file("b. Anzahl der unterschiedlichen Filme je Standort", "mydatabase.inventory.aggregate", query_b, QUERY_B)

#Query C.
query_c = [
   {
    '$lookup':{
        'from': "film_actor",
        'localField': "actor_id",
        'foreignField': "actor_id",
        'as': "actor_info"
    }
   },
   {
    '$project':{
        '_id':0,
        'Vorname':'$first_name',
        'Nachname':'$last_name',
        'Anzahl Auftritte': {
            '$size': "$actor_info"
        }
     }
    },
    {
    '$sort':{
        'Anzahl Auftritte':-1
    }
   },
   {
    '$limit':10
   }
]
QUERY_C = mydatabase.actor.aggregate(query_c)
write_query_to_file("c. Die Vor- und Nachnamen der 10 Schauspieler mit den meisten Filmen, absteigend sortiert.", "mydatabase.actor.aggregate", query_c, QUERY_C)

#Query D.
query_d = [
    {
        '$group':{
            '_id': "$staff_id",
            'total':{
                '$sum':"$amount"
            }
        }
    },
    {
        '$project':{
            '_id':0,
            'Mitarber ID':"$_id",
            'Erlös': {'$trunc':['$total', 2]}
        }
    }
]
QUERY_D = mydatabase.payment.aggregate(query_d)
write_query_to_file("d. Die Erlöse je Mitarbeiter", "mydatabase.payment.aggregate", query_d, QUERY_D)

#Query E.
query_e = [
    {
        '$group':{
            '_id': "$customer_id",
            'count':{
                '$sum':1
            }
        }
    },
    {
        '$sort':{
             'count' : -1
            } 
    },
    {
        '$limit':10
    },
    {
        '$project':{
            '_id':0,
            'Kunden ID':'$_id',
            'Anzahl Entleihungen':'$count'
        }
    }
]
QUERY_E = mydatabase.rental.aggregate(query_e)
write_query_to_file("e. Die IDs der 10 Kunden mit den meisten Entleihungen", "mydatabase.rental.aggregate", query_e, QUERY_E)
#Query F.
query_f = [
   {
    '$group':{
        '_id': "$customer_id",
        'sum': {
            '$sum':'$amount'
        }
    }
   },
   {
    '$sort':{
        'sum':-1
    }
   },
   {
    '$limit':10
   },
   {
    '$lookup':{
        'from': "customer",
        'localField': "_id",
        'foreignField': "customer_id",
        'as': 'customer_info'
    }
   },
    {
    '$lookup':{
        'from': "store",
        'localField': "customer_info.store_id",
        'foreignField': "store_id",
        'as': 'store_info'
    }
   },
    {
    '$lookup':{
        'from': "address",
        'localField': "store_info.address_id",
        'foreignField': "address_id",
        'as': 'address_info'
     }
    },
    {
    '$unwind': '$customer_info'
    },
    {
    '$unwind': '$address_info'
    },
    {
    '$project':{
        '_id':0,
        'Vorname':'$customer_info.first_name',
        'Nachname':'$customer_info.last_name',
        'Ausgabe': '$sum',
        'Niederlassung': "$address_info.district"
     }
    }
]
QUERY_F = mydatabase.payment.aggregate(query_f)
write_query_to_file("f.  Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, die das meiste Geld ausgegeben haben", "mydatabase.payment.aggregate", query_f, QUERY_F)

#Query G.
query_g = [
    {
    '$lookup':{
        'from': "inventory",
        'localField': "inventory_id",
        'foreignField': "inventory_id",
        'as': 'inventory_info'
     }
    },
   {
    '$lookup':{
        'from': "film",
        'localField': "inventory_info.film_id",
        'foreignField': "film_id",
        'as': "film_info"
    }
   },
   {
     '$group':{
        '_id': "$film_info.film_id",
        'title': {
            '$first': "$film_info.title"
        },
        'sum': {
            '$sum': 1
        }
     }
    },
    {
    '$sort':{
        'sum':-1
    }
   },
   {
    '$limit':10
   },
   {
    '$unwind': '$title'
   },
   {
    '$project':{
        '_id':0,
        'Anzahl der Ausleihe': '$sum',
        'Filmname':'$title'
     }
   }
]
QUERY_G = mydatabase.rental.aggregate(query_g)
write_query_to_file("g. Die 10 meistgesehenen Filme unter Angabe des Titels, absteigend sortiert", "mydatabase.rental.aggregate", query_g, QUERY_G)

#Query H.
query_h = [
    {
    '$lookup':{
        'from': "film_category",
        'localField': "category_id",
        'foreignField': "category_id",
        'as': 'film_category_info'
     }
    },
    {
    '$lookup':{
        'from': "inventory",
        'localField': "film_category_info.film_id",
        'foreignField': "film_id",
        'as': "inventory_info"
    }
   },
   {
    '$lookup':{
        'from': "rental",
        'localField': "inventory_info.inventory_id",
        'foreignField': "inventory_id",
        'as': "rental_info"
    }
   }, 
   {
    '$project':{
        '_id':0,
        'Kategoriename': '$name',
        'Anzahl':{
            '$size':"$rental_info"
        }
    }
   },
   {
    '$sort':{
        'Anzahl': -1
    }
   },
   {
    '$limit':3
   }
]
QUERY_H = mydatabase.category.aggregate(query_h)
write_query_to_file("h. Die 3 meistgesehenen Filmkategorien", "mydatabase.category.aggregate", query_h, QUERY_H)

#Creating the View customer_list as a collection.
CUSTOMER_LIST_PIPELINE = [
    {
    '$lookup':{
        'from': "address",
        'localField': "address_id",
        'foreignField': "address_id",
        'as': "address_info"
     }
    },
    {
    '$lookup':{
        'from': "city",
        'localField': "address_info.city_id",
        'foreignField': "city_id",
        'as': "city_info"
     }
    },
    {
    '$lookup':{
        'from': "country",
        'localField': "city_info.country_id",
        'foreignField': "country_id",
        'as': "country_info"
     }
    },
    {
        '$unwind': '$address_info'
    }, 
    {
        '$unwind': '$country_info'
    },
    {
        '$unwind': '$city_info'
    },
    {
    '$project':{
        'id': '$customer_id', 
        'name': {
            '$concat':['$first_name', ' ', '$last_name']
        },
        'address': '$address_info.address',
        'zip code': '$address_info.postal_code',
        'phone': '$address_info.phone',
        'city': '$city_info.city',
        'country': '$country_info.country',
        'notes': {
             '$cond': {
                 'if': '$activebool', 'then': 'active', 'else': ''
                  } 
                },
        'sid':'$store_id'
        }
    },
    {
        '$sort': {
            'id':1
        }
    }
    ]


mydatabase.create_collection(
    'customer_list',
    viewOn='customer',
    pipeline=CUSTOMER_LIST_PIPELINE
)

write_query_to_file("Customer_List View erzeugen", "mydatabase.create_collection", CUSTOMER_LIST_PIPELINE, mydatabase.customer_list.aggregate([{'$project':{'_id':0}}, {'$limit': 5}]))

#Changing the passwords of all staff.
write_to_file("Changing passwords ....")
for record in mydatabase.staff.find():
    mydatabase.staff.update_one({'_id': record['_id']}, {'$set': {'password': randomPassword()}})
    write_to_file(f"Password of {record['staff_id']} changed.")
write_to_file("All passwords changed")
write_to_file("-------------------------------------------------------------------------------------------------")

#Creating a new Store with a new Adress.
write_to_file("Creating new store ...")
address_id = mydatabase.address.find_one(sort=[('address_id', -1)])['address_id'] + 1
new_address = {
 'address_id': address_id,
 'address': '1120 Loja Avenue',
 'address2': '',
 'district': 'California',
 'city_id': 449,
 'postal_code': '17886',
 'phone': '110',
 'last_update': strftime("%Y-%m-%dT%H:%M:%S", localtime())}

write_to_file("Creating new address for the new store:") 
write_to_file(json.dumps(new_address, indent=1))
write_to_file("Inserting new address to mydatabase.adddress")
mydatabase.address.insert_one(new_address)
write_to_file("Creating new store with new address_id:")
store_id = mydatabase.store.find_one(sort=[('store_id', -1)])['store_id'] + 1

new_store = {
 'store_id': store_id,
 'manager_staff_id': 1,
 'address_id': address_id,
 'last_update': strftime("%Y-%m-%dT%H:%M:%S", localtime())}

write_to_file(json.dumps(new_store, indent=1))
write_to_file("Inserting new store to mydatabase.store")
mydatabase.store.insert_one(new_store)

#Updating the Inventory to set the new Store_ID.
write_to_file("Updating inventory to new store")
mydatabase.inventory.update_many({}, {"$set": {"store_id":store_id,
 'last_update': strftime("%Y-%m-%dT%H:%M:%S", localtime())}})
query_inventory = [{'$project':{'_id':0}}, {'$limit': 5}]
QUERY_INVENTORY =  mydatabase.inventory.aggregate([{'$project':{'_id':0}}, {'$limit': 5}])
write_query_to_file("Updated inventory", "mydatabase.inventory.aggregate", query_inventory,QUERY_INVENTORY)

#Deleting all films with a length less than 60 minutes.
write_to_file("Collecting films with length < 60 minutes")
film_ids_lt_60 = [k['film_id'] for k in mydatabase.film.find({'length': {'$lt': 60}})]

# We don't delete the films from the film collection, maybe in the future the boss is telling to us to buy this films again.
# Then I don't to put them again into the collection. If you like do it anyway.. uncomment the next line.
#mydatabase.film.delete_many({'film_id':{'$in': film_ids_lt_60}})

inventory_ids_lt_60 = [k['inventory_id'] for k in mydatabase.inventory.find({'film_id':{'$in': film_ids_lt_60}})]    
write_to_file("Film ids with length < 60 minutes :" + str(film_ids_lt_60))
write_to_file("Deleting corresponding inventory objects...")
deleted_result = mydatabase.inventory.delete_many({'film_id':{'$in': film_ids_lt_60}})
write_to_file(f"Deleted {deleted_result.deleted_count} objects from inventory.")

#Deleting all rental entries for films that are no longer part of the inventory.
write_to_file("Deleting corresponding rental objects...")
deleted_result = mydatabase.rental.delete_many({'inventory_id':{'$in': inventory_ids_lt_60}})
write_to_file(f"Deleted {deleted_result.deleted_count} objects from rental.")

write_to_file("████████▄   ▄██████▄  ███▄▄▄▄      ▄████████")
write_to_file("███   ▀███ ███    ███ ███▀▀▀██▄   ███    ███")
write_to_file("███    ███ ███    ███ ███   ███   ███    █▀ ")
write_to_file("███    ███ ███    ███ ███   ███  ▄███▄▄▄    ")
write_to_file("███    ███ ███    ███ ███   ███ ▀▀███▀▀▀    ")
write_to_file("███    ███ ███    ███ ███   ███   ███    █▄ ")
write_to_file("███   ▄███ ███    ███ ███   ███   ███    ███")
write_to_file("████████▀   ▀██████▀   ▀█   █▀    ██████████")