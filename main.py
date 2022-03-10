import unittest
from typing import Optional
import multiprocessing as mp
from itertools import repeat
from multiprocessing import Pool
import time

from elasticsearch import Elasticsearch

from cachesim import Cache, Obj, Status, Analyzer

import warnings
warnings.filterwarnings('ignore', 'Elasticsearch built-in security.*', ) # Ignore ES security warning


class NonCache(Cache):
    """
    Very basic example of a cache, which actually does not cache at all.
    """

    def _lookup(self, requested: Obj) -> Optional[Obj]:
        return None

    def _admit(self, fetched: Obj) -> bool:
        return True

    def _store(self, fetched: Obj):
        pass


class FIFOCache(Cache):
    """
    First in First out cache model.
    """

    def __init__(self, maxsize: int, logger=None):
        super().__init__(maxsize, logger)

        # implement a FIFO for the cache itself
        self._cache = []

    def _lookup(self, requested: Obj) -> Optional[Obj]:
        # check if object already in cache
        return next((x for x in self._cache if x == requested), None)

    def _admit(self, fetched: Obj) -> bool:
        return True

    def _store(self, fetched: Obj):
        # trigger cache eviction if needed
        while fetched.size <= self.maxsize < sum(self._cache) + fetched.size:
            self._cache.pop(0)

        # put the new object at the end of the cache
        self._cache.append(fetched)


class ProtectedFIFOCache(FIFOCache):
    """
    Same as FIFOCache, but big (> 10% of total cache size) object are not allowed to enter the cache.
    """

    def _admit(self, fetched: Obj) -> bool:
        # allow only small objects to enter the cache
        return fetched.size <= self.maxsize * 0.1


class TestCaches(unittest.TestCase):
    def setUp(self):
        # define objects
        self.x = Obj('x', 1000, 300)
        self.a = Obj('a', 100, 300)
        self.b = Obj('b', 100, 300)
        self.c = Obj('c', 100, 300)
        self.d = Obj('d', 30, 300)

    def test_noncache(self):
        # create cache
        cache = NonCache(200)

        # place requests
        self.assertEqual(cache.recv(0, self.x), Status.PASS)  # way too big, must be PASS
        self.assertEqual(cache.recv(1, self.a), Status.MISS)  # NonCache does not cache
        self.assertEqual(cache.recv(2, self.b), Status.MISS)  # NonCache does not cache
        self.assertEqual(cache.recv(3, self.c), Status.MISS)  # NonCache does not cache

    def test_fifocache(self):
        # create cache
        cache = FIFOCache(400)

        # place requests
        self.assertEqual(cache.recv(0, self.x), Status.PASS)  # way too big, must be PASS
        self.assertEqual(cache.recv(1, self.a), Status.MISS)  # MISS
        self.assertEqual(cache.recv(2, self.b), Status.MISS)  # MISS
        self.assertEqual(cache.recv(3, self.a), Status.HIT)  # 2nd request on a, must be HIT
        self.assertEqual(cache.recv(4, self.c), Status.MISS)  # MISS

    def test_protectedfifocache(self):
        # create cache
        cache = ProtectedFIFOCache(400)

        # place requests
        self.assertEqual(cache.recv(0, self.a), Status.PASS)  # size limit at cache admission
        self.assertEqual(cache.recv(1, self.b), Status.PASS)  # size limit at cache admission
        self.assertEqual(cache.recv(2, self.a), Status.PASS)  # size limit at cache admission
        self.assertEqual(cache.recv(3, self.d), Status.MISS)  # MISS
        self.assertEqual(cache.recv(3.1, self.d), Status.HIT)  # 2nd request on a, must be HIT
        self.assertEqual(cache.recv(3.2, self.d), Status.HIT)  # 3rd request on a, must be HIT
        self.assertEqual(cache.recv(1000, self.d), Status.MISS)  # expired, must be MISS


def connect_elasticsearch(domain, port):
    """
    Python Elasticsearch Client: connection to elasticsearch

    :param domain: Elasticsearch domain of the running instance
    :param port: Elasticsearch HTTP interface port 
    """
    host = "http://" + domain + ":" + str(port)
    print(host)
    es = Elasticsearch([host], request_timeout = 30, max_retries=10, retry_on_timeout=True)
    if es.ping():
        print('Elasticsearch is connected!')
    else:
        print('Error: Elasticsearch could not connect!')
    return es

def es_query(q, index_name, search_size=10000, stop_after=-1):
    """
    Fetch the logs data from Elasticsearch using search queries and scroll API. The logs are then sent to the main
    process to be replayed.

    :param q: multiprocessing queue used to send the logs' data to the main process
    :param index_name: name of the ES index used for running the search
    :param search_size: number of documents returned by each individual search (by default limited to 10,000 in ES)
    :param stop_after: the search stop after this number of data processed, -1 for not setting any limit
    """

    # Requests from ES cluster
    es = connect_elasticsearch("192.168.100.146", 9200)

    # End of task if the indices does not exist in ES 
    if not es.indices.exists(index=index_name, allow_no_indices=False):
            fail_message("Query failed: the index does not exist in Elasticsearch")
            q.send(None)
            q.close()
            return

    # The following query returns for each log in the ES cluster the Epoch time (in second), the path = ID of the object, the content lenght = size of the object and maxage = how long content will be cached
    search_results = es.search(index=index_name, scroll = '1m', _source=["path", "contentlength", "maxage"], query={"match_all": {}}, size=100000, docvalue_fields=[{"field": "@timestamp","format": "epoch_second"}], sort=[{"@timestamp": {"order": "asc"}}], track_total_hits=True, version=False)

    # ES limits the number of results to 10,000. Using the scroll API and scroll ID allows to surpass this limit and to distribute the results in manageable chunks
    sid = search_results['_scroll_id']

    print("Total number of logs: ", search_results['hits']['total']['value'])
    print("Count API: ", es.count(index=index_name)['count'])
    if(search_results['hits']['total']['value']!=es.count(index=index_name)['count']):
        fail_message("Query failed: the total number of logs that can be fetched is not consistent (number of results should be " + str(es.count(index=index_name)['count']) + " but the search returned " + str(search_results['hits']['total']['value']) + " documents)")
        q.send(None)
        q.close()
        return 
        

    total_processed=search_size # Total number of data already processed

    while len(search_results['hits']['hits']) > 0 and (stop_after==-1 or stop_after>total_processed):
        # Update the scroll ID
        sid = search_results['_scroll_id']
        q.send(search_results["hits"]["hits"])
        search_results = es.scroll(scroll_id = sid, scroll = '1m')
        total_processed+=search_size

    es.clear_scroll(scroll_id = sid)
    print("End of query")
    q.send(None)
    q.close()

def fail_message(message, write_in_file=True):
    print(message)
    if write_in_file:
        with open("./results/" + "fail.txt",'w',encoding = 'utf-8') as f:
            print(message, file=f)


def cache_simulation(search_results, maxage, cache):
    """
    Search results data are sent to the simulation.

    :param search_results: multiprocessing queue used to send the logs' data to the main process
    :param maxage: default maxage used if not indicated in HTTP cache header
    :param cache: cache used for the simulation
    """
    status_list=[] # list of status (hit, miss or pass) corresponding to the decisions made by the simulator
    for log in search_results:
        if isinstance(log["_source"]["maxage"], int): obj = Obj(int(log["_source"]["path"]), int(log["_source"]["contentlength"]), int(log["_source"]["maxage"]))
        else: obj = Obj(int(log["_source"]["path"]), int(log["_source"]["contentlength"]), maxage) # default value if maxage is not indicated
        status_list.append(cache.recv(int(log["fields"]["@timestamp"][0]), obj)) # keep trace of the status result from the cache simulation
    return status_list


def processes_coordination(index_name, default_maxage=0):
    """
    Manage and coordinate every processes used for running for this program (elasticsearch fetching process, cache simulation process, analyzer process).
    This function is in charge of creating the processes, establishing the communication of the data between the processes and terminating them.

    :param index_name: name of the ES index used for running the search
    :param default_maxage: default maxage value if not indicated in HTTP cache header

    """

    # create cache
    cache = ProtectedFIFOCache(50)
    cache2 = ProtectedFIFOCache(100)
    cache3 = ProtectedFIFOCache(200)
    cache4 = ProtectedFIFOCache(400)
    cache5 = ProtectedFIFOCache(800)
    cache6 = ProtectedFIFOCache(1600)
    cache7 = ProtectedFIFOCache(3000)
    cache8 = ProtectedFIFOCache(5000)
    cache9 = ProtectedFIFOCache(10000)
    cache10 = ProtectedFIFOCache(20000)
    cache11 = ProtectedFIFOCache(50000)
    cache12 = ProtectedFIFOCache(100000)
    caches = [cache, cache2, cache3, cache4, cache5, cache6, cache7, cache8, cache9, cache10, cache11, cache12]

    
    # define objects
    # x = Obj('x', 1000, 300)
    # a = Obj('a', 100, 300)
    # b = Obj('b', 100, 300)
    # c = Obj('c', 100, 300)
    # d = Obj('d', 30, 300)

    # place requests
    # cache.recv(0, a)
    # cache.recv(1, b)
    # cache.recv(2, a)
    # cache.recv(3, d)
    # cache.recv(3.1, d)
    # cache.recv(3.2, d)
    # cache.recv(1000, d)

    search_size=100000 # number of documents returned by each individual search

    # create the pipe and process in charge of fetching the data from elasticsearch
    parent_query, child_query = mp.Pipe()
    p_query = mp.Process(target=es_query, args=(child_query, index_name, search_size,))

    # create the queue and process in charge of analyzing the data resulting from the cache simulation
    analyzer_queues = [mp.Queue() for i in range(12)]

    p_analyzer = mp.Process(target=Analyzer, args=(analyzer_queues[0],30,1000000,True,"CHR_PFIFO50_time", "CHR_PFIFO50_regular", "CHR_PFIFO50_final",))
    p_analyzer2 = mp.Process(target=Analyzer, args=(analyzer_queues[1],30,1000000,True,"CHR_PFIFO100_time", "CHR_PFIFO100_regular", "CHR_PFIFO100_final",))
    p_analyzer3 = mp.Process(target=Analyzer, args=(analyzer_queues[2],30,1000000,True,"CHR_PFIFO200_time", "CHR_PFIFO200_regular", "CHR_PFIFO200_final",))
    p_analyzer4 = mp.Process(target=Analyzer, args=(analyzer_queues[3],30,1000000,True,"CHR_PFIFO400_time", "CHR_PFIFO400_regular", "CHR_PFIFO400_final",))
    p_analyzer5 = mp.Process(target=Analyzer, args=(analyzer_queues[4],30,1000000,True,"CHR_PFIFO800_time", "CHR_PFIFO800_regular", "CHR_PFIFO800_final",))
    p_analyzer6 = mp.Process(target=Analyzer, args=(analyzer_queues[5],30,1000000,True,"CHR_PFIFO1600_time", "CHR_PFIFO1600_regular", "CHR_PFIFO1600_final",))
    p_analyzer7 = mp.Process(target=Analyzer, args=(analyzer_queues[6],30,1000000,True,"CHR_PFIFO3000_time", "CHR_PFIFO3000_regular", "CHR_PFIFO3000_final",))
    p_analyzer8 = mp.Process(target=Analyzer, args=(analyzer_queues[7],30,1000000,True,"CHR_PFIFO5000_time", "CHR_PFIFO5000_regular", "CHR_PFIFO5000_final",))
    p_analyzer9 = mp.Process(target=Analyzer, args=(analyzer_queues[8],30,1000000,True,"CHR_PFIFO10000_time", "CHR_PFIFO10000_regular", "CHR_PFIFO10000_final",))
    p_analyzer10 = mp.Process(target=Analyzer, args=(analyzer_queues[9],30,1000000,True,"CHR_PFIFO20000_time", "CHR_PFIFO20000_regular", "CHR_PFIFO20000_final",))
    p_analyzer11 = mp.Process(target=Analyzer, args=(analyzer_queues[10],30,1000000,True,"CHR_PFIFO50000_time", "CHR_PFIFO50000_regular", "CHR_PFIFO50000_final",))
    p_analyzer12 = mp.Process(target=Analyzer, args=(analyzer_queues[11],30,1000000,True,"CHR_PFIFO100000_time", "CHR_PFIFO100000_regular", "CHR_PFIFO100000_final",))
    p_analyzers = [p_analyzer, p_analyzer2, p_analyzer3, p_analyzer4, p_analyzer5, p_analyzer6, p_analyzer7, p_analyzer8, p_analyzer9, p_analyzer10, p_analyzer11, p_analyzer12]
    

    # start the processes
    p_query.start()

    
    # receive the data from the process running the es queries, send them to the process in charge of the cache simulation and send the simulation data to the analyzer
    search_results = parent_query.recv() # data are received from the process fetching es data
    
    if search_results is None:
        print("Search failed: end of program")
        return 

    for analyzer_process in p_analyzers:
        analyzer_process.start()

    while search_results is not None:

        # Run all the cache simulations in parallel (Pool multiprocessing)
        with Pool() as pool:
            status_caches = pool.starmap(cache_simulation, zip(repeat(search_results), repeat(default_maxage), caches))
        
        # Send results to the analyzers (one individual analyzer for each simulation)
        for index, status in enumerate(status_caches):
            analyzer_queues[index].put([int(search_results[-1]["fields"]["@timestamp"][0]), status]) # last timestamp and list of status are sent to the analyzer at the end of the request

        search_results = parent_query.recv() # data are received from the process fetching es data

    for queue in analyzer_queues:
        queue.put(None) # notify to the analyzer the end of the incoming data


if __name__ == '__main__':
    start = time.time()

    processes_coordination(index_name="batch3-*", default_maxage=300)

    end = time.time()

    # Write on the disk and stdout the final time needed for running the entire program
    with open("./results/" + "perf.txt",'w',encoding = 'utf-8') as f:
        print("Running time: ", end-start, "s", file=f)
    print("Running time: ", end-start, "s")