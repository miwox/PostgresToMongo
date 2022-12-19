# %%
# importing module
from pymongo import MongoClient
import hashlib
import os
import string
import random
from time import localtime, strftime
client = MongoClient("mongodb://mongodb:27017/")

#TODO Eine Pretty-Print-Logging-Methode schreiben

# %%
mydatabase = client['mongodb']

# %%
QUERY_A = mydatabase.inventory.aggregate([
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
])

print('Hello World')
print('Hello World')
# %%
QUERY_A.next()

# %%
QUERY_B = mydatabase.inventory.aggregate([
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
    }])


# %%
QUERY_B.next()

# %% [markdown]
# Select actor.actor_id, actor.first_name, Count(*) Anzahl, actor.last_name from film_actor
# join actor on film_actor.actor_id = actor.actor_id
# group by actor.actor_id
# Order by Anzahl DESC 
# Limit 10

# %%
QUERY_C = mydatabase.actor.aggregate([
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
])

# %%
QUERY_C.next()

# %% [markdown]
# Select sum(payment.amount) total, staff.staff_id, staff.first_name, staff.last_name  from payment
# join staff on payment.staff_id = Staff.staff_id
# group by staff.staff_id
# 
# total	staff_id	first_name	last_name
# 30252.12	1	Mike	Hillyer
# 31059.92	2	Jon	Stephens

# %%
QUERY_D = mydatabase.payment.aggregate([
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
])

# %%
QUERY_D.next()

# %% [markdown]
# Select customer_id, Count(*) from rental
# group by customer_id
# Order By Count(*) DESC
# Limit 10;

# %%
QUERY_E = mydatabase.rental.aggregate([
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
])

# %%
QUERY_E.next()


# %% [markdown]
# Select customer.customer_id, customer.first_name, customer.last_name,
# Sum(payment.amount) as total, address.district from payment
# join customer on payment.customer_id = customer.customer_id
# join store on store.store_id = customer.store_id
# join address on store.address_id = address.address_id
# Group by customer.customer_id, address.address_id
# Order by total DESC
# Limit 10
# 
# customer_id	first_name	last_name	total	district
# 148	Eleanor	Hunt	211.55	Alberta
# 526	Karl	Seal	208.58	QLD
# 178	Marion	Snyder	194.61	QLD
# 137	Rhonda	Kennedy	191.62	QLD
# 144	Clara	Shaw	189.60	Alberta
# 459	Tommy	Collazo	183.63	Alberta
# 181	Ana	Bradley	167.67	QLD
# 410	Curtis	Irby	167.62	QLD
# 236	Marcia	Dean	166.61	Alberta
# 403	Mike	Way	162.67	Alberta

# %%
QUERY_F = mydatabase.payment.aggregate([
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
])

# %%
QUERY_F.next()

# %% [markdown]
# g)
# Select Count(film.film_id) as Amount, film.title from film
# join inventory on film.film_id = inventory.film_id
# join rental on inventory.inventory_id = rental.inventory_id
# Group by film.film_id
# Order by Amount DESC
# 
# 34	Bucket Brotherhood
# 33	Rocketeer Mother
# 32	Forward Temple
# 32	Juggler Hardly
# 32	Ridgemont Submarine
# 32	Grit Clockwork
# 32	Scalawag Duck
# 31	Robbers Joon
# 31	Network Peak
# 31	Timberland Sky

# %%
QUERY_G = mydatabase.rental.aggregate([
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
])

# %%
QUERY_G.next()

# %% [markdown]
# Select category.name, Count(*) from rental 
# join inventory on rental.inventory_id = inventory.inventory_id
# join film_category on inventory.film_id = film_category.film_id
# join category on film_category.category_id = category.category_id
# GROUP BY category.category_id
# ORDER BY count DESC
# LIMIT 3
# 
# 
# name	count
# Sports	1179
# Animation	1166
# Action	1112
# 
# 

# %%
QUERY_H = mydatabase.category.aggregate([
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
])

# %%
QUERY_H.next()

# %% [markdown]
# Create View customer_list as collection

# %%
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
        'ssid':'$store_id'
        }
    },
    {
        '$sort': {
            'id':1
        }
    }
    ]

# %%
mydatabase.create_collection(
    'customer_list',
    viewOn='customer',
    pipeline=CUSTOMER_LIST_PIPELINE
)

# %% [markdown]
# Update employees passwords

# %%
def randomPassword():
    salt = os.urandom(32)
    password = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(15))
    key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    return key

# %%
for record in mydatabase.staff.find():
    mydatabase.staff.update_one({'_id': record['_id']}, {'$set': {'password': randomPassword()}})

# %% [markdown]
# Update location with inventory

# %%
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
 

# %%
mydatabase.address.insert_one(new_address)

# %%
store_id = mydatabase.store.find_one(sort=[('store_id', -1)])['store_id'] + 1
new_store = {
 'store_id': store_id,
 'manager_staff_id': 1,
 'address_id': address_id,
 'last_update': strftime("%Y-%m-%dT%H:%M:%S", localtime())} #TODO Konstante einführen

# %%
mydatabase.store.insert_one(new_store)

# %%
mydatabase.inventory.update_many({}, {"$set": {"store_id":store_id,
 'last_update': strftime("%Y-%m-%dT%H:%M:%S", localtime())}}) #TODO Konstante einführen

# %% [markdown]
# ### TODO Exemplarische Abfragen um zu zeigen, dass Inventory sich geändert hat.

# %% [markdown]
# Delete film lenght lt 60 minutes

# %%
film_ids_lt_60 = [k['film_id'] for k in mydatabase.film.find({'length': {'$lt': 60}})]
inventory_ids_lt_60 = [k['inventory_id'] for k in mydatabase.inventory.find({'film_id':{'$in': film_ids_lt_60}})]    
mydatabase.inventory.delete_many({'film_id':{'$in': film_ids_lt_60}})
mydatabase.rental.delete_many({'inventory_id':{'$in': inventory_ids_lt_60}})