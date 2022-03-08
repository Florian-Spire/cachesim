import unittest
from typing import Optional
import multiprocessing as mp
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

def es_query(q, index_name):
    """
    Fetch the logs data from Elasticsearch using search queries and scroll API. The logs are then sent to the main
    process to be replayed.

    :param q: multiprocessing queue used to send the logs' data to the main process
    :param index_name: name of the ES index used for running the search
    """

    # Requests from ES cluster
    es = connect_elasticsearch("192.168.100.146", 9200)

    # End of task if the indices does not exist in ES 
    if not es.indices.exists(index=index_name, allow_no_indices=False):
            q.put(None)
            q.close()
            return

    # The following query returns for each log in the ES cluster the Epoch time (in second), the path = ID of the object, the content lenght = size of the object and maxage = how long content will be cached
    search_results = es.search(index=index_name, scroll = '1m', _source=["path", "contentlength", "maxage"], query={"match_all": {}}, size=1, docvalue_fields=[{"field": "@timestamp","format": "epoch_second"}], sort=[{"@timestamp": {"order": "asc"}}], version=False)
    
    # ES limits the number of results to 10,000. Using the scroll API and scroll ID allows to surpass this limit and to distribute the results in manageable chunks
    sid = search_results['_scroll_id']

    print("Total number of logs: ", search_results['hits']['total']['value'])

    q.put(search_results["hits"]["hits"])

    while len(search_results['hits']['hits']) > 0:
        search_results = es.scroll(scroll_id = sid, scroll = '1m', request_timeout = 30)
        # Update the scroll ID
        sid = search_results['_scroll_id']
        q.put(search_results["hits"]["hits"])

    es.clear_scroll(scroll_id = sid)
    q.put(None)
    q.close()

def processes_coordination():
    """
    Manage and coordinate every processes used for running for this program (elasticsearch fetching process, cache simulation process, analyzer process).
    This function is in charge of creating the processes, establishing the communication of the data between the processes and terminating them.
    """

    # create cache
    cache = ProtectedFIFOCache(400)

    
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


    # create the queue and process in charge of fetching the data from elasticsearch
    query_queue = mp.Queue()
    p_query = mp.Process(target=es_query, args=(query_queue,"batch3-*"))

    # create the queue and process in charge of analyzing the data resulting from the cache simulation
    analyzer_queue = mp.Queue()
    p_analyzer = mp.Process(target=Analyzer, args=(analyzer_queue, 30, 1000000,))

    # start the processes
    p_query.start()
    p_analyzer.start()

    # receive the data from the process running the es queries, send them to the process in charge of the cache simulation and send the simulation data to the analyzer
    search_results = query_queue.get() # data are received from the process fetching es data
    while search_results is not None:
        status_list=[] # list of status (hit, miss or pass) corresponding to the decisions made by the simulator
        for log in search_results:
            if isinstance(log["_source"]["maxage"], int): obj = Obj(int(log["_source"]["path"]), int(log["_source"]["contentlength"]), int(log["_source"]["maxage"]))
            else: obj = Obj(int(log["_source"]["path"]), int(log["_source"]["contentlength"]), 300) # default value if maxage is not indicated (300)
            status_list.append(cache.recv(int(log["fields"]["@timestamp"][0]), obj)) # keep trace of the status result from the cache simulation
        analyzer_queue.put([int(search_results[-1]["fields"]["@timestamp"][0]), status_list]) # last timestamp and list of status are sent to the analyzer at the end of the request
        search_results = query_queue.get() # data are received from the process fetching es data

    analyzer_queue.put(None) # notify to the analyzer the end of the incoming data


if __name__ == '__main__':
    start = time.time()

    processes_coordination()

    end = time.time()

    # Write on the disk and stdout the final time needed for running the entire program
    with open("perf.txt",'w',encoding = 'utf-8') as f:
        print("Running time: ", end-start, "s", file=f)
    print("Running time: ", end-start, "s")