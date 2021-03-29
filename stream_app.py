from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pymongo import MongoClient
import json
import time
from operator import add
from hashlib import sha256

PORT = 9999
INTERVAL = 120  # dstream window 
r = 2**32 - 1  # the range for the nonce
splits = 4  # the no of range splits 
i = 1  # this will help later for the blockchain list reading
genesis_preblock = 0, 'Genesis block', '0'  # hardcoded genesis_block's initial values
trailing_zeros = '000000'  # the degree of difficulty

sc = SparkContext('local[4]', 'Block Chain Server')
ssc = StreamingContext(sc, INTERVAL)


def split_range(x, r, splits):
    ''' This function takes a variable x, a range r and the number of splits and
        returns a list of the form:
        [(x, range(0, a/splits), (x, range(a/splits, a*2/splits), (x, range(a*2/splits, a*3/splits),...]    
    '''
    my_list = []
    for i in range(splits):
        _ = range(int(r*i/splits), int(r*(i+1)/splits))
        my_list.append((x, _))        
    return my_list


def mine_block(block, trailing_zeros):
    ''' This function takes a tuple((seq_no, payload, previous_digest), range) 
        and returns a tuple(seq_no, (payload, nonce, digest, time_needed))
    '''
    no_zeros = len(trailing_zeros)
    s256 = sha256()  # the hash function used
    s256.update(str(block[0][0]).encode())  # encode the sequence number
    s256.update(block[0][1].encode())  # encode the payload
    s256.update(block[0][2].encode())  # encode the prev block digest
    start = time.perf_counter()  # start the counter
    for i in block[1]:  # loop until you find the nonce
        a = s256.copy()  # create a copy to work on so that s256 is not updated for every i
        a.update(str(i).encode())
        if a.hexdigest()[:no_zeros] == trailing_zeros:  # the condition for the digest
            stop = time.perf_counter()  # stop the counter
            return block[0][0], (block[0][1], i, a.hexdigest(), stop - start)
    return block[0][0], (block[0][1], -1, '-', 9999)  # nonce not found


def send_block(x):
    ''' This function sends the block's seq_no, nonce and time_needed
        to a listening mongoDB collection
    '''
    client = MongoClient()  # create client instance
    db = client.project  # open db
    blockchains = db.blockchains  # open db.collection
    blockchains.insert_one({"seq_no" : int(x[0]), "nonce" : int(x[2]), "time" : float(x[4])})
    client.close()

# Create the genesis block
# First parallelize into an RDD the preblock(seq_no, payload, prev_digest) with splitted ranges
# Map the mine_block function to each range split
# Keep only the fastest worker by sorting based on time
# Convert the RDD to a quintuple
genesis_block = sc.parallelize(split_range(genesis_preblock, r, splits))\
    .map(lambda x: mine_block(x, trailing_zeros))\
        .reduceByKey(lambda x, y: min((x, y), key=lambda x: x[3]))\
            .map(lambda x: (x[0], *x[1])).collect()
# Cache the block to the blockchain list
blockchain = [*genesis_block]
send_block(blockchain[0])  # send block's seq, nonce, time to mongoDB

# The streaming part
lines = ssc.socketTextStream('localhost', PORT)
parsed = lines.map(lambda line: json.loads(line))
strings = parsed.map(lambda x: x['string'])
concat_strings = strings.reduce(add)  # concat all the strings in the 120sec window
concat_strings.foreachRDD(lambda rdd: blockchain.append(rdd.collect()))  # append each RDD to the blockchain

ssc.start()

# This infinite loop is constantly checking the blockchain's list length
# When a new block is appended then it gets parallelized, mined and sent to mongoDB  
while(True):
    if len(blockchain) >= i + 1:  # initially len is 1 (only genesis_block is in the blockchain)
        block = i, *blockchain[i], blockchain[i-1][3]  # create the preblock with the prev_block digest
        block = sc.parallelize(split_range(block, r, splits))\
            .map(lambda x: mine_block(x, trailing_zeros))\
                .reduceByKey(lambda x, y: min((x, y), key=lambda x: x[3]))\
                    .map(lambda x: (x[0], *x[1])).collect()
        blockchain[i] = block[0]
        send_block(blockchain[i])
        print(blockchain)
        i += 1  # after mining is done, increment i so that the next block in line gets mined
        
ssc.awaitTermination()
