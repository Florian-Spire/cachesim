import multiprocessing as mp
from itertools import repeat
from multiprocessing import Pool
import time

from cachesim import FIFOCache, ProtectedFIFOCache, Clairvoyant, Analyzer
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
    :pagination_technique: pagination technique used for paginating the results: 'Scroll' or 'Search_after'
    :stop_after: -1 means that we iterate over the whole index, other values stop the program after the number indicated (for example 100 to run the program only on the 100 first values from the index)
    :param: true if clairvoyant cache is used, false otherwise (warning: clairvoyant cannot be mixed with other type of caches as it requires knowledge of the future)
    """

    # create cache
    cache = Clairvoyant(800, connect_elasticsearch(host, port))

    
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
    p_analyzer = mp.Process(target=Analyzer, args=(analyzer_queue,1,1000,True,"CHR_Clairvoyant_time", "CHR_Clairvoyant_regular", "CHR_Clairvoyant_final",))

    # start the processes
    p_query.start()
    p_analyzer.start()

    # receive the data from the process running the es queries, send them to the process in charge of the cache simulation and send the simulation data to the analyzer
    search_results = parent_query.recv() # data are received from the process fetching es data
    while search_results is not None:
        status_list=[] # list of status (hit, miss or pass) corresponding to the decisions made by the simulator
        for log in search_results:
            if isinstance(log["_source"]["maxage"], int): obj = Obj(int(log["_source"]["path"]), int(log["_source"]["contentlength"]), int(log["_source"]["maxage"]))
            else: obj = Obj(int(log["_source"]["path"]), int(log["_source"]["contentlength"]), default_maxage) # default value if maxage is not indicated (300)
            if clairvoyant: status_list.append(cache.recv(int(log["fields"]["@timestamp"][0]), log["_id"], obj))
            else: status_list.append(cache.recv(int(log["fields"]["@timestamp"][0]), obj))
        analyzer_queue.put([int(search_results[-1]["fields"]["@timestamp"][0]), status_list]) # last timestamp and list of status are sent to the analyzer at the end of the request
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
    :pagination_technique: pagination technique used for paginating the results: 'Scroll' or 'Search_after'
    :stop_after: -1 means that we iterate over the whole index, other values stop the program after the number indicated (for example 100 to run the program only on the 100 first values from the index)
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
    if pagination_technique.lower()=="scroll":
        p_query = mp.Process(target=es_query_scroll, args=(child_query, index_name, host, port, search_size,stop_after))
    elif pagination_technique.lower() in ["search-after", "search_after", "searchafter"]:
        p_query = mp.Process(target=es_query_search_after, args=(child_query, index_name, host, port, search_size,stop_after,))
    else:
        fail_message("Pagination technique is invalid (should be scroll or search_after): please change parameter in main function")
        return 

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
            analyzer_queues[index].put([int(search_results[-1]["fields"]["@timestamp"][0]), status]) # last timestamp and list of status are sent to the analyzer at the end of the request

        search_results = parent_query.recv() # data are received from the process fetching es data

    for queue in analyzer_queues:
        queue.put(None) # notify to the analyzer the end of the incoming data



if __name__ == '__main__':
    start = time.time()

    # Check parameter doc from process_coordination function for more details
    processes_coordination_parallel(index_name="batch3-*", host="192.168.100.146", port=9200, default_maxage=300, pagination_technique="Scroll", stop_after=-1)
    # processes_coordination_single_simulation(index_name="batch3-*", host="192.168.100.146", port=9200, default_maxage=300, pagination_technique="Scroll", stop_after=-1, clairvoyant=True)

    end = time.time()

    # Write on the disk and stdout the final time needed for running the entire program
    with open("./results/" + "perf.txt",'w',encoding = 'utf-8') as f:
        print("Running time: ", end-start, "s", file=f)
    print("Running time: ", end-start, "s")