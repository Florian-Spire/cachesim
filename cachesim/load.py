import multiprocessing as mp
from cachesim import FIFOCache, ProtectedFIFOCache, LRUCache, ProtectedLRUCache, Clairvoyant, LFUCache, ProtectedLFUCache, Analyzer

def protected_FIFO_caches():
    # create cache
    cache = ProtectedFIFOCache(50,  write_log=False)
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
    cache = ProtectedLRUCache(50,  write_log=False)
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

def protected_LFU_caches():
    # create cache
    cache = ProtectedLFUCache(50,  write_log=False)
    cache2 = ProtectedLFUCache(100)
    cache3 = ProtectedLFUCache(200)
    cache4 = ProtectedLFUCache(500)
    cache5 = ProtectedLFUCache(1000)
    cache6 = ProtectedLFUCache(2000)
    cache7 = ProtectedLFUCache(5000)
    cache8 = ProtectedLFUCache(10000)
    cache9 = ProtectedLFUCache(20000)
    cache10 = ProtectedLFUCache(50000)
    cache11 = ProtectedLFUCache(100000)
    cache12 = ProtectedLFUCache(1000000)
    return [cache, cache2, cache3, cache4, cache5, cache6, cache7, cache8, cache9, cache10, cache11, cache12]

def one_each_cache(size_cache):
    # create cache
    cachePFIFO = ProtectedFIFOCache(size_cache)
    cachePLRU = ProtectedLRUCache(size_cache)
    cachePLFU = ProtectedLFUCache(size_cache)
    return [cachePFIFO, cachePLRU, cachePLFU]


def all_protected_caches():
    # Protected FIFO
    cache = ProtectedFIFOCache(50,  write_log=False)
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

    # Protected LRU
    cache13 = ProtectedLRUCache(50,  write_log=False)
    cache14 = ProtectedLRUCache(100)
    cache15 = ProtectedLRUCache(200)
    cache16 = ProtectedLRUCache(500)
    cache17 = ProtectedLRUCache(1000)
    cache18 = ProtectedLRUCache(2000)
    cache19 = ProtectedLRUCache(5000)
    cache20 = ProtectedLRUCache(10000)
    cache21 = ProtectedLRUCache(20000)
    cache22 = ProtectedLRUCache(50000)
    cache23 = ProtectedLRUCache(100000)
    cache24 = ProtectedLRUCache(1000000)

    # PROTECTED LFU
    cache25 = ProtectedLFUCache(50,  write_log=False)
    cache26 = ProtectedLFUCache(100)
    cache27 = ProtectedLFUCache(200)
    cache28 = ProtectedLFUCache(500)
    cache29 = ProtectedLFUCache(1000)
    cache30 = ProtectedLFUCache(2000)
    cache31 = ProtectedLFUCache(5000)
    cache32 = ProtectedLFUCache(10000)
    cache33 = ProtectedLFUCache(20000)
    cache34 = ProtectedLFUCache(50000)
    cache35 = ProtectedLFUCache(100000)
    cache36 = ProtectedLFUCache(1000000)
    return [cache, cache2, cache3, cache4, cache5, cache6, cache7, cache8, cache9, cache10, cache11, cache12, cache13, cache14, cache15, cache16, cache17, cache18, cache19, cache20, cache21, cache22, cache23, cache24, cache25, cache26, cache27, cache28, cache29, cache30, cache31, cache32, cache33, cache34, cache35, cache36]

def analyzers(cache_name):
    # create the queue and process in charge of analyzing the data resulting from the cache simulation
    analyzer_queues = [mp.Queue() for i in range(12)]

    p_analyzer  = mp.Process(target=Analyzer, args=(analyzer_queues[0],30,1000000,0,True,"CHR_"+cache_name+"_50_time", "CHR_"+cache_name+"_50_regular", "CHR_"+cache_name+"_50_final",))
    p_analyzer2 = mp.Process(target=Analyzer, args=(analyzer_queues[1],30,1000000,0,True,"CHR_"+cache_name+"_100_time", "CHR_"+cache_name+"_100_regular", "CHR_"+cache_name+"_100_final",))
    p_analyzer3 = mp.Process(target=Analyzer, args=(analyzer_queues[2],30,1000000,0,True,"CHR_"+cache_name+"_200_time", "CHR_"+cache_name+"_200_regular", "CHR_"+cache_name+"_200_final",))
    p_analyzer4 = mp.Process(target=Analyzer, args=(analyzer_queues[3],30,1000000,0,True,"CHR_"+cache_name+"_500_time", "CHR_"+cache_name+"_500_regular", "CHR_"+cache_name+"_500_final",))
    p_analyzer5 = mp.Process(target=Analyzer, args=(analyzer_queues[4],30,1000000,0,True,"CHR_"+cache_name+"_1000_time", "CHR_"+cache_name+"_1000_regular", "CHR_"+cache_name+"_1000_final",))
    p_analyzer6 = mp.Process(target=Analyzer, args=(analyzer_queues[5],30,1000000,0,True,"CHR_"+cache_name+"_2000_time", "CHR_"+cache_name+"_2000_regular", "CHR_"+cache_name+"_2000_final",))
    p_analyzer7 = mp.Process(target=Analyzer, args=(analyzer_queues[6],30,1000000,0,True,"CHR_"+cache_name+"_5000_time", "CHR_"+cache_name+"_5000_regular", "CHR_"+cache_name+"_5000_final",))
    p_analyzer8 = mp.Process(target=Analyzer, args=(analyzer_queues[7],30,1000000,0,True,"CHR_"+cache_name+"_10000_time", "CHR_"+cache_name+"_10000_regular", "CHR_"+cache_name+"_10000_final",))
    p_analyzer9 = mp.Process(target=Analyzer, args=(analyzer_queues[8],30,1000000,0,True,"CHR_"+cache_name+"_20000_time", "CHR_"+cache_name+"_20000_regular", "CHR_"+cache_name+"_20000_final",))
    p_analyzer10 = mp.Process(target=Analyzer, args=(analyzer_queues[9],30,1000000,0,True,"CHR_"+cache_name+"_50000_time", "CHR_"+cache_name+"_50000_regular", "CHR_"+cache_name+"_50000_final",))
    p_analyzer11 = mp.Process(target=Analyzer, args=(analyzer_queues[10],30,1000000,0,True,"CHR_"+cache_name+"_100000_time", "CHR_"+cache_name+"_100000_regular", "CHR_"+cache_name+"_100000_final",))
    p_analyzer12 = mp.Process(target=Analyzer, args=(analyzer_queues[11],30,1000000,0,True,"CHR_"+cache_name+"_1000000_time", "CHR_"+cache_name+"_1000000_regular", "CHR_"+cache_name+"_1000000_final",))
    return analyzer_queues, [p_analyzer, p_analyzer2, p_analyzer3, p_analyzer4, p_analyzer5, p_analyzer6, p_analyzer7, p_analyzer8, p_analyzer9, p_analyzer10, p_analyzer11, p_analyzer12]

def one_each_analyzers():
    analyzer_queues = [mp.Queue() for i in range(3)]

    p_analyzer_PFIFO  = mp.Process(target=Analyzer, args=(analyzer_queues[0],30,1000000,21600,True,"CHR_PFIFO_time", "CHR_PFIFO_regular", "CHR_PFIFO_final", "CHR_PFIFO_movies"))
    p_analyzer_PLRU = mp.Process(target=Analyzer, args=(analyzer_queues[1],30,1000000,21600,True,"CHR_PLRU_time", "CHR_PLRU_regular", "CHR_PLRU_final","CHR_PLRU_movies"))
    p_analyzer_PLFU = mp.Process(target=Analyzer, args=(analyzer_queues[2],30,1000000,21600,True,"CHR_PLFU_time", "CHR_PLFU_regular", "CHR_PLFU_final","CHR_PLFU_movies"))
    return analyzer_queues, [p_analyzer_PFIFO, p_analyzer_PLRU, p_analyzer_PLFU]

def all_analyzers(cache_names=["PFIFO", "PLRU", "PLFU"]):
        # create the queue and process in charge of analyzing the data resulting from the cache simulation
    analyzer_queues = [mp.Queue() for i in range(36)]

    cache_name = cache_names[0]
    p_analyzer  = mp.Process(target=Analyzer, args=(analyzer_queues[0],30,1000000,0,True,"CHR_"+cache_name+"_50_time", "CHR_"+cache_name+"_50_regular", "CHR_"+cache_name+"_50_final",))
    p_analyzer2 = mp.Process(target=Analyzer, args=(analyzer_queues[1],30,1000000,0,True,"CHR_"+cache_name+"_100_time", "CHR_"+cache_name+"_100_regular", "CHR_"+cache_name+"_100_final",))
    p_analyzer3 = mp.Process(target=Analyzer, args=(analyzer_queues[2],30,1000000,0,True,"CHR_"+cache_name+"_200_time", "CHR_"+cache_name+"_200_regular", "CHR_"+cache_name+"_200_final",))
    p_analyzer4 = mp.Process(target=Analyzer, args=(analyzer_queues[3],30,1000000,0,True,"CHR_"+cache_name+"_500_time", "CHR_"+cache_name+"_500_regular", "CHR_"+cache_name+"_500_final",))
    p_analyzer5 = mp.Process(target=Analyzer, args=(analyzer_queues[4],30,1000000,0,True,"CHR_"+cache_name+"_1000_time", "CHR_"+cache_name+"_1000_regular", "CHR_"+cache_name+"_1000_final",))
    p_analyzer6 = mp.Process(target=Analyzer, args=(analyzer_queues[5],30,1000000,0,True,"CHR_"+cache_name+"_2000_time", "CHR_"+cache_name+"_2000_regular", "CHR_"+cache_name+"_2000_final",))
    p_analyzer7 = mp.Process(target=Analyzer, args=(analyzer_queues[6],30,1000000,0,True,"CHR_"+cache_name+"_5000_time", "CHR_"+cache_name+"_5000_regular", "CHR_"+cache_name+"_5000_final",))
    p_analyzer8 = mp.Process(target=Analyzer, args=(analyzer_queues[7],30,1000000,0,True,"CHR_"+cache_name+"_10000_time", "CHR_"+cache_name+"_10000_regular", "CHR_"+cache_name+"_10000_final",))
    p_analyzer9 = mp.Process(target=Analyzer, args=(analyzer_queues[8],30,1000000,0,True,"CHR_"+cache_name+"_20000_time", "CHR_"+cache_name+"_20000_regular", "CHR_"+cache_name+"_20000_final",))
    p_analyzer10 = mp.Process(target=Analyzer, args=(analyzer_queues[9],30,1000000,0,True,"CHR_"+cache_name+"_50000_time", "CHR_"+cache_name+"_50000_regular", "CHR_"+cache_name+"_50000_final",))
    p_analyzer11 = mp.Process(target=Analyzer, args=(analyzer_queues[10],30,1000000,0,True,"CHR_"+cache_name+"_100000_time", "CHR_"+cache_name+"_100000_regular", "CHR_"+cache_name+"_100000_final",))
    p_analyzer12 = mp.Process(target=Analyzer, args=(analyzer_queues[11],30,1000000,0,True,"CHR_"+cache_name+"_1000000_time", "CHR_"+cache_name+"_1000000_regular", "CHR_"+cache_name+"_1000000_final",))

    cache_name = cache_names[1]
    p_analyzer13  = mp.Process(target=Analyzer, args=(analyzer_queues[12],30,1000000,0,True,"CHR_"+cache_name+"_50_time", "CHR_"+cache_name+"_50_regular", "CHR_"+cache_name+"_50_final",))
    p_analyzer14 = mp.Process(target=Analyzer, args=(analyzer_queues[13],30,1000000,0,True,"CHR_"+cache_name+"_100_time", "CHR_"+cache_name+"_100_regular", "CHR_"+cache_name+"_100_final",))
    p_analyzer15 = mp.Process(target=Analyzer, args=(analyzer_queues[14],30,1000000,0,True,"CHR_"+cache_name+"_200_time", "CHR_"+cache_name+"_200_regular", "CHR_"+cache_name+"_200_final",))
    p_analyzer16 = mp.Process(target=Analyzer, args=(analyzer_queues[15],30,1000000,0,True,"CHR_"+cache_name+"_500_time", "CHR_"+cache_name+"_500_regular", "CHR_"+cache_name+"_500_final",))
    p_analyzer17 = mp.Process(target=Analyzer, args=(analyzer_queues[16],30,1000000,0,True,"CHR_"+cache_name+"_1000_time", "CHR_"+cache_name+"_1000_regular", "CHR_"+cache_name+"_1000_final",))
    p_analyzer18 = mp.Process(target=Analyzer, args=(analyzer_queues[17],30,1000000,0,True,"CHR_"+cache_name+"_2000_time", "CHR_"+cache_name+"_2000_regular", "CHR_"+cache_name+"_2000_final",))
    p_analyzer19 = mp.Process(target=Analyzer, args=(analyzer_queues[18],30,1000000,0,True,"CHR_"+cache_name+"_5000_time", "CHR_"+cache_name+"_5000_regular", "CHR_"+cache_name+"_5000_final",))
    p_analyzer20 = mp.Process(target=Analyzer, args=(analyzer_queues[19],30,1000000,0,True,"CHR_"+cache_name+"_10000_time", "CHR_"+cache_name+"_10000_regular", "CHR_"+cache_name+"_10000_final",))
    p_analyzer21 = mp.Process(target=Analyzer, args=(analyzer_queues[20],30,1000000,0,True,"CHR_"+cache_name+"_20000_time", "CHR_"+cache_name+"_20000_regular", "CHR_"+cache_name+"_20000_final",))
    p_analyzer22 = mp.Process(target=Analyzer, args=(analyzer_queues[21],30,1000000,0,True,"CHR_"+cache_name+"_50000_time", "CHR_"+cache_name+"_50000_regular", "CHR_"+cache_name+"_50000_final",))
    p_analyzer23 = mp.Process(target=Analyzer, args=(analyzer_queues[22],30,1000000,0,True,"CHR_"+cache_name+"_100000_time", "CHR_"+cache_name+"_100000_regular", "CHR_"+cache_name+"_100000_final",))
    p_analyzer24 = mp.Process(target=Analyzer, args=(analyzer_queues[23],30,1000000,0,True,"CHR_"+cache_name+"_1000000_time", "CHR_"+cache_name+"_1000000_regular", "CHR_"+cache_name+"_1000000_final",))

    cache_name = cache_names[2]
    p_analyzer25  = mp.Process(target=Analyzer, args=(analyzer_queues[24],30,1000000,0,True,"CHR_"+cache_name+"_50_time", "CHR_"+cache_name+"_50_regular", "CHR_"+cache_name+"_50_final",))
    p_analyzer26 = mp.Process(target=Analyzer, args=(analyzer_queues[25],30,1000000,0,True,"CHR_"+cache_name+"_100_time", "CHR_"+cache_name+"_100_regular", "CHR_"+cache_name+"_100_final",))
    p_analyzer27 = mp.Process(target=Analyzer, args=(analyzer_queues[26],30,1000000,0,True,"CHR_"+cache_name+"_200_time", "CHR_"+cache_name+"_200_regular", "CHR_"+cache_name+"_200_final",))
    p_analyzer28 = mp.Process(target=Analyzer, args=(analyzer_queues[27],30,1000000,0,True,"CHR_"+cache_name+"_500_time", "CHR_"+cache_name+"_500_regular", "CHR_"+cache_name+"_500_final",))
    p_analyzer29 = mp.Process(target=Analyzer, args=(analyzer_queues[28],30,1000000,0,True,"CHR_"+cache_name+"_1000_time", "CHR_"+cache_name+"_1000_regular", "CHR_"+cache_name+"_1000_final",))
    p_analyzer30 = mp.Process(target=Analyzer, args=(analyzer_queues[29],30,1000000,0,True,"CHR_"+cache_name+"_2000_time", "CHR_"+cache_name+"_2000_regular", "CHR_"+cache_name+"_2000_final",))
    p_analyzer31 = mp.Process(target=Analyzer, args=(analyzer_queues[30],30,1000000,0,True,"CHR_"+cache_name+"_5000_time", "CHR_"+cache_name+"_5000_regular", "CHR_"+cache_name+"_5000_final",))
    p_analyzer32 = mp.Process(target=Analyzer, args=(analyzer_queues[31],30,1000000,0,True,"CHR_"+cache_name+"_10000_time", "CHR_"+cache_name+"_10000_regular", "CHR_"+cache_name+"_10000_final",))
    p_analyzer33 = mp.Process(target=Analyzer, args=(analyzer_queues[32],30,1000000,0,True,"CHR_"+cache_name+"_20000_time", "CHR_"+cache_name+"_20000_regular", "CHR_"+cache_name+"_20000_final",))
    p_analyzer34 = mp.Process(target=Analyzer, args=(analyzer_queues[33],30,1000000,0,True,"CHR_"+cache_name+"_50000_time", "CHR_"+cache_name+"_50000_regular", "CHR_"+cache_name+"_50000_final",))
    p_analyzer35 = mp.Process(target=Analyzer, args=(analyzer_queues[34],30,1000000,0,True,"CHR_"+cache_name+"_100000_time", "CHR_"+cache_name+"_100000_regular", "CHR_"+cache_name+"_100000_final",))
    p_analyzer36 = mp.Process(target=Analyzer, args=(analyzer_queues[35],30,1000000,0,True,"CHR_"+cache_name+"_1000000_time", "CHR_"+cache_name+"_1000000_regular", "CHR_"+cache_name+"_1000000_final",))

    return analyzer_queues, [p_analyzer, p_analyzer2, p_analyzer3, p_analyzer4, p_analyzer5, p_analyzer6, p_analyzer7, p_analyzer8, p_analyzer9, p_analyzer10, p_analyzer11, p_analyzer12, p_analyzer13, p_analyzer14, p_analyzer15, p_analyzer16, p_analyzer17, p_analyzer18, p_analyzer19, p_analyzer20, p_analyzer21, p_analyzer22, p_analyzer23, p_analyzer24, p_analyzer25, p_analyzer26, p_analyzer27, p_analyzer28, p_analyzer29, p_analyzer30, p_analyzer31, p_analyzer32, p_analyzer33, p_analyzer34, p_analyzer35, p_analyzer36]