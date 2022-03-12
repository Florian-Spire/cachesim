from elasticsearch import Elasticsearch
from cachesim import Obj
import sys

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
        fail_message('Error: Elasticsearch could not connect!')
    return es


def fail_message(message, write_in_file=True):
    print(message, file=sys.stderr)
    if write_in_file:
        with open("./results/" + "fail.txt",'w',encoding = 'utf-8') as f:
            print(message, file=f)

def es_query_scroll(q, index_name, host, port, search_size=10000, stop_after=-1):
    """
    Fetch the logs data from Elasticsearch using search queries and scroll API. The logs are then sent to the main
    process to be replayed.
    :param q: multiprocessing queue used to send the logs' data to the main process
    :param index_name: name of the ES index used for running the search
    :param host: IP address of ES instance
    :param port: port of ES instance
    :param search_size: number of documents returned by each individual search (by default limited to 10,000 in ES)
    :param stop_after: the search stop after this number of data processed, -1 for not setting any limit
    """

    # Requests from ES cluster
    es = connect_elasticsearch(host, port)

    # End of task if the indices does not exist in ES 
    if not es.indices.exists(index=index_name, allow_no_indices=False):
            fail_message("Query failed: the index does not exist in Elasticsearch")
            q.send(None)
            q.close()
            return

    # The following query returns for each log in the ES cluster the Epoch time (in second), the path = ID of the object, the content lenght = size of the object and maxage = how long content will be cached
    search_results = es.search(index=index_name, scroll = '2m', _source=["path", "contentlength", "maxage"], query={"match_all": {}}, size=search_size, docvalue_fields=[{"field": "@timestamp","format": "epoch_second"}], sort=[{"@timestamp": {"order": "asc"}}], track_total_hits=True, version=False)

    # ES limits the number of results to 10,000. Using the scroll API and scroll ID allows to surpass this limit and to distribute the results in manageable chunks
    sid = search_results['_scroll_id']

    print("Total number of logs: ", search_results['hits']['total']['value'])
    print("Count API: ", es.count(index=index_name)['count'])
    if(search_results['hits']['total']['value']!=es.count(index=index_name)['count']):
        fail_message("Query failed: the total number of logs that can be fetched is not consistent (number of results should be " + str(es.count(index=index_name)['count']) + " but the search returned " + str(search_results['hits']['total']['value']) + " documents)")
        q.send(None)
        q.close()
        return 
        
    total_processed=0
    while len(search_results['hits']['hits']) > 0 and (stop_after==-1 or stop_after>total_processed):
        # Update the scroll ID
        sid = search_results['_scroll_id']
        q.send(search_results["hits"]["hits"])
        total_processed+=len(search_results['hits']['hits'])
        search_results = es.scroll(scroll_id = sid, scroll = '2m')

    es.clear_scroll(scroll_id = sid)
    print("End of query")
    q.send(None)
    q.close()


def es_query_search_after(q, index_name, host, port, search_size=10000, stop_after=-1):
    """
    Fetch the logs data from Elasticsearch using search queries and scroll API. The logs are then sent to the main
    process to be replayed.
    :param q: multiprocessing queue used to send the logs' data to the main process
    :param index_name: name of the ES index used for running the search
    :param host: IP address of ES instance
    :param port: port of ES instance
    :param search_size: number of documents returned by each individual search (by default limited to 10,000 in ES)
    :param stop_after: the search stop after this number of data processed, -1 for not setting any limit
    """

    # Requests from ES cluster
    es = connect_elasticsearch(host, port)

    # End of task if the indices does not exist in ES 
    if not es.indices.exists(index=index_name, allow_no_indices=False):
            q.send(None)
            q.close()
            return

    #ES point-in-time
    pit = es.open_point_in_time(index=index_name, keep_alive="2m")['id']

    # The following query returns for each log in the ES cluster the Epoch time (in second), the path = ID of the object, the content lenght = size of the object and maxage = how long content will be cached
    search_results = es.search(_source=["path", "contentlength", "maxage"], query={"match_all": {}}, size=search_size, docvalue_fields=[{"field": "@timestamp","format": "epoch_second"}], sort=[{"@timestamp": {"order": "asc"}}], pit={"id":  pit, "keep_alive": "2m"}, track_total_hits=True, version=False)

    print("Total number of logs: ", search_results['hits']['total']['value'])
    print("Count API: ", es.count(index=index_name)['count'])
    if(search_results['hits']['total']['value']!=es.count(index=index_name)['count']):
        fail_message("Query failed: the total number of logs that can be fetched is not consistent (number of results should be " + str(es.count(index=index_name)['count']) + " but the search returned " + str(search_results['hits']['total']['value']) + " documents)")
        q.send(None)
        q.close()
        return 
    total_processed=0 # Total number of data already processed

    while len(search_results['hits']['hits']) > 0 and (stop_after==-1 or stop_after>total_processed):
        # Update the scroll ID
        last_value = search_results["hits"]["hits"][-1]["sort"]
        q.send(search_results["hits"]["hits"])
        total_processed+=len(search_results['hits']['hits'])
        search_results = es.search(_source=["path", "contentlength", "maxage"], search_after=last_value, query={"match_all": {}}, size=search_size, docvalue_fields=[{"field": "@timestamp","format": "epoch_second"}], sort=[{"@timestamp": {"order": "asc"}}], pit={"id":  pit, "keep_alive": "5m"}, track_total_hits=True, version=False)

    es.close_point_in_time(body={"id": pit})
    print("End of query")
    q.send(None)
    q.close()



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