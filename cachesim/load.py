import multiprocessing as mp
from cachesim import FIFOCache, ProtectedFIFOCache, LRUCache, ProtectedLRUCache, Clairvoyant, Analyzer

def protected_FIFO_caches():
    # create cache
    cache = ProtectedFIFOCache(50)
    cache2 = ProtectedFIFOCache(100)
    cache3 = ProtectedFIFOCache(200)
    cache4 = ProtectedFIFOCache(500)
    cache5 = ProtectedFIFOCache(1000)
    cache6 = ProtectedFIFOCache(2000)
    cache7 = ProtectedFIFOCache(5000)
    cache8 = ProtectedFIFOCache(10000)
    cache9 = ProtectedFIFOCache(20000)
    cache10 = ProtectedFIFOCache(50000)
    cache11 = ProtectedFIFOCache(100000)
    cache12 = ProtectedFIFOCache(1000000)
    return [cache, cache2, cache3, cache4, cache5, cache6, cache7, cache8, cache9, cache10, cache11, cache12]

def protected_LRU_caches():
    # create cache
    cache = ProtectedLRUCache(50)
    cache2 = ProtectedLRUCache(100)
    cache3 = ProtectedLRUCache(200)
    cache4 = ProtectedLRUCache(500)
    cache5 = ProtectedLRUCache(1000)
    cache6 = ProtectedLRUCache(2000)
    cache7 = ProtectedLRUCache(5000)
    cache8 = ProtectedLRUCache(10000)
    cache9 = ProtectedLRUCache(20000)
    cache10 = ProtectedLRUCache(50000)
    cache11 = ProtectedLRUCache(100000)
    cache12 = ProtectedLRUCache(1000000)
    return [cache, cache2, cache3, cache4, cache5, cache6, cache7, cache8, cache9, cache10, cache11, cache12]

def analyzers(cache_name):
    # create the queue and process in charge of analyzing the data resulting from the cache simulation
    analyzer_queues = [mp.Queue() for i in range(12)]

    p_analyzer = mp.Process(target=Analyzer, args=(analyzer_queues[0],30,1000000,True,"CHR_PLRU_"+cache_name+"_time", "CHR_"+cache_name+"_50_regular", "CHR_"+cache_name+"_50_final",))
    p_analyzer2 = mp.Process(target=Analyzer, args=(analyzer_queues[1],30,1000000,True,"CHR_"+cache_name+"_100_time", "CHR_"+cache_name+"_100_regular", "CHR_"+cache_name+"_100_final",))
    p_analyzer3 = mp.Process(target=Analyzer, args=(analyzer_queues[2],30,1000000,True,"CHR_"+cache_name+"_200_time", "CHR_"+cache_name+"_200_regular", "CHR_"+cache_name+"_200_final",))
    p_analyzer4 = mp.Process(target=Analyzer, args=(analyzer_queues[3],30,1000000,True,"CHR_"+cache_name+"_500_time", "CHR_"+cache_name+"_500_regular", "CHR_"+cache_name+"_500_final",))
    p_analyzer5 = mp.Process(target=Analyzer, args=(analyzer_queues[4],30,1000000,True,"CHR_"+cache_name+"_1000_time", "CHR_"+cache_name+"_1000_regular", "CHR_"+cache_name+"_1000_final",))
    p_analyzer6 = mp.Process(target=Analyzer, args=(analyzer_queues[5],30,1000000,True,"CHR_"+cache_name+"_2000_time", "CHR_"+cache_name+"_2000_regular", "CHR_"+cache_name+"_2000_final",))
    p_analyzer7 = mp.Process(target=Analyzer, args=(analyzer_queues[6],30,1000000,True,"CHR_"+cache_name+"_5000_time", "CHR_"+cache_name+"_5000_regular", "CHR_"+cache_name+"_5000_final",))
    p_analyzer8 = mp.Process(target=Analyzer, args=(analyzer_queues[7],30,1000000,True,"CHR_"+cache_name+"_10000_time", "CHR_"+cache_name+"_10000_regular", "CHR_"+cache_name+"_10000_final",))
    p_analyzer9 = mp.Process(target=Analyzer, args=(analyzer_queues[8],30,1000000,True,"CHR_"+cache_name+"_20000_time", "CHR_"+cache_name+"_20000_regular", "CHR_"+cache_name+"_20000_final",))
    p_analyzer10 = mp.Process(target=Analyzer, args=(analyzer_queues[9],30,1000000,True,"CHR_"+cache_name+"_50000_time", "CHR_"+cache_name+"_50000_regular", "CHR_"+cache_name+"_50000_final",))
    p_analyzer11 = mp.Process(target=Analyzer, args=(analyzer_queues[10],30,1000000,True,"CHR_"+cache_name+"_100-000_time", "CHR_"+cache_name+"_100-000_regular", "CHR_"+cache_name+"_100-000_final",))
    p_analyzer12 = mp.Process(target=Analyzer, args=(analyzer_queues[11],30,1000000,True,"CHR_"+cache_name+"_1-000-000_time", "CHR_"+cache_name+"_1-000-000_regular", "CHR_"+cache_name+"_1-000-000_final",))
    return analyzer_queues, [p_analyzer, p_analyzer2, p_analyzer3, p_analyzer4, p_analyzer5, p_analyzer6, p_analyzer7, p_analyzer8, p_analyzer9, p_analyzer10, p_analyzer11, p_analyzer12]
