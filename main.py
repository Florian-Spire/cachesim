import multiprocessing as mp
from itertools import repeat
from multiprocessing import Pool
import time

from cachesim import FIFOCache, ProtectedFIFOCache, Clairvoyant, Analyzer,  LFUCache, LSOCache, RANCache, LRUCache, SSOCache
from cachesim import load
from logs_replayer import *

import warnings
warnings.filterwarnings('ignore', 'Elasticsearch built-in security.*', ) # Ignore ES security warning



def processes_coordination_single_simulation(index_name, host, port, default_maxage=0, pagination_technique="Scroll", stop_after=-1, clairvoyant=False):
    """
    Manage and coordinate the processes. Only one simulation can be done at the same time (see processes_coordination_parallel for running parallel simulations).
    This function is in charge of creating the processes, establishing the communication of the data between the processes and terminating them.

    :param index_name: name of the ES index used for running the search
    :param host: IP address of ES instance
    :param port: port of ES instance
    :param default_maxage: default maxage value if not indicated in HTTP cache header
    :param pagination_technique: pagination technique used for paginating the results: 'Scroll' or 'Search_after'
    :param stop_after: -1 means that we iterate over the whole index, other values stop the program after the number indicated (for example 100 to run the program only on the 100 first values from the index)
    :param clairvoyant: true if clairvoyant cache is used, false otherwise (warning: clairvoyant cannot be mixed with other type of caches as it requires knowledge of the future)
    """

    # create cache
    cache = Clairvoyant(10000, connect_elasticsearch(host, port), index_name)
    # cache = LSOCache(10000, write_log=True)

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

    search_size=1000 # number of documents returned by each individual search

     # create the pipe and process in charge of fetching the data from elasticsearch
    parent_query, child_query = mp.Pipe()
    if pagination_technique.lower()=="scroll":
        p_query = mp.Process(target=es_query_scroll, args=(child_query, index_name, host, port, search_size,stop_after))
    elif pagination_technique.lower() in ["search-after", "search_after", "searchafter"]:
        p_query = mp.Process(target=es_query_search_after, args=(child_query, index_name, host, port, search_size,stop_after,))
    else:
        fail_message("Pagination technique is invalid (should be scroll or search_after): please change parameter in main function")
        return 

    # create the queue and process in charge of analyzing the data resulting from the cache simulation
    analyzer_queue = mp.Queue()
    p_analyzer_clairvoyant = mp.Process(target=Analyzer, args=(analyzer_queue,1,1000,3600,True,True,"CHR_Clairvoyant_time", "CHR_Clairvoyant_regular", "CHR_Clairvoyant_final","CHR_Clairvoyant_movies", "trafic_served_from_cache_Clairvoyant",))
    # p_analyzer = mp.Process(target=Analyzer, args=(analyzer_queue,1,1000,3600, True,True,"CHR_LSO_time", "CHR_LSO_regular", "CHR_LSO_final", "CHR_LSO_movies", "trafic_served_from_cache_LSO",))

    # start the processes
    p_query.start()
    p_analyzer_clairvoyant.start()
    # p_analyzer.start()

    # receive the data from the process running the es queries, send them to the process in charge of the cache simulation and send the simulation data to the analyzer
    search_results = parent_query.recv() # data are received from the process fetching es data
    while search_results is not None:
        status_list=[] # list of status (hit, miss or pass) corresponding to the decisions made by the simulator
        group_ids=[] # list of the group of the object (for example movie identifier), group_ids[0] is the group of the object corresponding to the decision stored in status_list[0]
        for log in search_results:
            if log["_source"]["livechannel"] is None: log["_source"]["livechannel"] = -1
            if isinstance(log["_source"]["maxage"], int): obj = Obj(int(log["_source"]["path"]), int(log["_source"]["contentlength"]), int(log["_source"]["maxage"]), int(log["_source"]["livechannel"]))
            else: obj = Obj(int(log["_source"]["path"]), int(log["_source"]["contentlength"]), default_maxage, int(log["_source"]["livechannel"])) # default value if maxage is not indicated (300)
            if clairvoyant: status_list.append(cache.recv(float(log["fields"]["@timestamp"][0]), log["_id"], obj))
            else: status_list.append(cache.recv(float(log["fields"]["@timestamp"][0]), obj))
            group_ids.append(obj.group)
        analyzer_queue.put([float(search_results[-1]["fields"]["@timestamp"][0]), status_list, group_ids]) # last timestamp and list of status are sent to the analyzer at the end of the request
        search_results = parent_query.recv() # data are received from the process fetching es data

    analyzer_queue.put(None) # notify to the analyzer the end of the incoming data



def processes_coordination_parallel(index_name, host, port, default_maxage=0, pagination_technique="Scroll", stop_after=-1):
    """
    Manage and coordinate every processes used for running for this program (elasticsearch fetching process, cache simulation processes, analyzer processes).
    This function is in charge of creating the processes, establishing the communication of the data between the processes and terminating them.

    :param index_name: name of the ES index used for running the search
    :param host: IP address of ES instance
    :param port: port of ES instance
    :param default_maxage: default maxage value if not indicated in HTTP cache header
    :param pagination_technique: pagination technique used for paginating the results: 'Scroll' or 'Search_after'
    :param stop_after: -1 means that we iterate over the whole index, other values stop the program after the number indicated (for example 100 to run the program only on the 100 first values from the index)
    """
    
    caches = load.one_each_cache(10000)

    
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
    if pagination_technique.lower()=="scroll":
        p_query = mp.Process(target=es_query_scroll, args=(child_query, index_name, host, port, search_size,stop_after))
    elif pagination_technique.lower() in ["search-after", "search_after", "searchafter"]:
        p_query = mp.Process(target=es_query_search_after, args=(child_query, index_name, host, port, search_size,stop_after,))
    else:
        fail_message("Pagination technique is invalid (should be scroll or search_after): please change parameter in main function")
        return 

    analyzer_queues, p_analyzers = load.one_each_analyzers()

    assert len(caches) == len(analyzer_queues) == len(p_analyzers), f"The number of caches should be equal to the number of analyzers!"
    
    # start the processes
    p_query.start()

    
    # receive the data from the process running the es queries, send them to the process in charge of the cache simulation and send the simulation data to the analyzer
    search_results = parent_query.recv() # data are received from the process fetching es data
    
    if search_results is None:
        fail_message("Search failed (no search result returned): end of program")
        return 

    for analyzer_process in p_analyzers:
        analyzer_process.start()

    while search_results is not None:

        # Run all the cache simulations in parallel (Pool multiprocessing)
        with Pool() as pool:
            status_caches = pool.starmap(cache_simulation, zip(repeat(search_results), repeat(default_maxage), caches))
        
        # Send results to the analyzers (one individual analyzer for each simulation)
        for index, status in enumerate(status_caches):
            analyzer_queues[index].put([float(search_results[-1]["fields"]["@timestamp"][0]), status[0], status[1], status[2]]) # last timestamp and list of status (status[0]: status of the simulation, status[1]: group ids, status[2]: sizes of the objects] are sent to the analyzer at the end of the request

        search_results = parent_query.recv() # data are received from the process fetching es data

    for queue in analyzer_queues:
        queue.put(None) # notify to the analyzer the end of the incoming data



if __name__ == '__main__':
    start = time.time()

    # Check parameter doc from process_coordination function for more details
    processes_coordination_parallel(index_name="batch3-*", host="192.168.100.147", port=9200, default_maxage=300, pagination_technique="Scroll", stop_after=-1)
    # processes_coordination_single_simulation(index_name="clairvoyant", host="192.168.100.147", port=9200, default_maxage=300, pagination_technique="Scroll", stop_after=-1, clairvoyant=True)

    end = time.time()

    # Write on the disk and stdout the final time needed for running the entire program
    with open("./results/" + "perf.txt",'w',encoding = 'utf-8') as f:
        print("Running time: ", end-start, "s", file=f)
    print("Running time: ", end-start, "s")