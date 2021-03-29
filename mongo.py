import pymongo
from pymongo import MongoClient
import pprint

client = MongoClient()
db = client.project
col = db.blockchains


# 1. Find the nonce value of a block with a given seq_no

print('What\'s the nonce of block no1?')
print(next(col.find({'seq_no' : 1}, {'_id': 0, 'nonce' : 1})))

# 2. Find block with the smallest mining time

print('Which block was mined fastest?')
pprint.pprint(next(col.find().sort([('time', 1)]).limit(1)))

# 3. Avg mining time

print('What\'s the average mining time?')
print(next(col.aggregate([
    { "$group": { "_id": '$null', "avg_time": {'$avg':"$time"}}}
])))

# 4. Cumulative mining time for a given range of blocks

print('What\'s the total mining time for blocks 1 to 10?')
print(next(col.aggregate([
    {'$match': {'seq_no':{'$gt':0}, 'seq_no':{'$lt':10}}},
    {'$group': {'_id':'$null', 'total_time': {'$sum' : '$time'}}}
    ])))

# 5. Number of blocks with nonce in given range

print('How many blocks have nonce between 0 and 1bn?')
print(next(col.aggregate([
    {'$match': {'nonce':{'$gt':0}, 'nonce':{'$lt':1_000_000_000}}},
    {'$group': {'_id':'$null', 'total_blocks': {'$sum' : 1}}}
    ])))
