from asyncore import write
from cachesim import Obj, Status
import logging
import unittest
from typing import Optional
from abc import ABC, abstractmethod
from elasticsearch import Elasticsearch


class Cache(ABC):
    """
    Abstract class to provide structure and basic functionalities. Use this to implement your own cache model.
    """

    def __init__(self, maxsize: int, logger: logging.Logger = None, write_log=False):
        """
        Cache initialization. Overload the init method for custom initialization.

        :param maxsize: Maximum size of the cache.
        :param logger: If not None, use this logger, otherwise create one.
        :param write_log: write log on the output
        """

        # max cache size
        assert maxsize > 0 and isinstance(maxsize, int), f"Cache must have positive integer size: '{maxsize}' received!"
        self.__maxsize = maxsize

        # keep track of the time
        self.__clock = None

        self.__write_log = write_log

        # setup logging
        if logger is None:
            self.__logger = logging.getLogger(name=self.__class__.__name__)
            # self._logger.setLevel(logging.DEBUG)  TODO: FIX this
        else:
            self.__logger = logger

    @property
    def maxsize(self) -> int:
        """Total size of the cache."""
        return self.__maxsize

    @property
    def clock(self) -> float:
        """Current time."""
        return self.__clock

    @clock.setter
    def clock(self, time: float):
        """Update current time."""
        assert self.__clock is None or time >= self.__clock, f"Time passes, you will never become younger!"
        self.__clock = time

    def recv(self, time: float, obj: Obj) -> Status:
        """
        Call this function to place a request to the cache.

        :param time: Time (epoch) of the object request.
        :param obj: The object (Obj) requested.
        :return: Request status (Status).
        """

        # update the internal clock
        self.clock = time

        # try to get the object from cache
        stored = self._lookup(obj)
        if stored is not None:

            # retrieved from cache, check expires
            if not stored.isexpired(self.clock):
                # HIT, "serv" object from cache
                self.__log(stored, Status.HIT)
                return Status.HIT

        # MISS: not in cache or expired --> just simulate fetch!
        obj.fetched = True

        # cache admission
        if obj.cacheable and obj.size <= self.maxsize and self._admit(obj):

            # store
            obj.enter = self.clock
            self._store(obj)

            self.__log(obj, Status.MISS)
            return Status.MISS

        else:
            self.__log(obj, Status.PASS)
            return Status.PASS

    @abstractmethod
    def _admit(self, fetched: Obj) -> bool:
        """
        Implement this method to provide a cache admission policy.

        :param fetched: Object fetched.
        :return: True, if object may enter the cache, False for bypass the cache and go for PASS.
        """
        pass

    @abstractmethod
    def _lookup(self, requested: Obj) -> Optional[Obj]:
        """
        Implement this method to provide a caching function. In this state, the content of the object is not known.
        Return the cached object.

        :param requested: Object requested.
        :return: The object from the cache.
        """
        pass

    @abstractmethod
    def _store(self, fetched: Obj):
        """
        Implement this method to store objects.

        :param fetched: Object fetched from origin.
        """
        pass

    def __log(self, obj, status: Status):
        """Basic logging"""
        if self.__write_log:
            self.__logger.warning(f"{self.clock} {status} {obj}")


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

    def __init__(self, maxsize: int, logger=None, write_log=False):
        super().__init__(maxsize, logger, write_log)

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

class Clairvoyant(Cache):
    """
    Clairvoyant (Belady) cache model. This model uses knowledge of the future and is the optimal caching method (unsusable in practice).
    """

    def __init__(self, maxsize: int, es_instance: Elasticsearch, logger=None, write_log=True):
        """ Init function
        :param es_instance: instance used for running the ES searches"""
        super().__init__(maxsize, None, write_log)

        # list of the elements contained in the cache
        self._cache = []

        # ES IDs of the documents already processed
        self._es_ids = []

        self._es = es_instance

        self.__write_log = write_log

        # setup logging
        if logger is None:
            self.__logger = logging.getLogger(name=self.__class__.__name__)
            # self._logger.setLevel(logging.DEBUG)  TODO: FIX this
        else:
            self.__logger = logger

    def _lookup(self, requested: Obj) -> Optional[Obj]:
        # check if object already in cache
        return next((x for x in self._cache if x == requested), None)

    def _admit(self, fetched: Obj) -> bool:
        return fetched.size <= self.maxsize * 0.1

    def _store(self, fetched: Obj):
        self._cache.append(fetched)

        # trigger cache eviction if needed
        while fetched.size <= self.maxsize < sum(self._cache) + fetched.size:
            # We search the object with the furthest access time
            max_timestamp = 0 # furthest access time discovered
            obj_to_drop=self._cache[0] # next object to drop (initialization)
            for cache_element in self._cache:
                # For cache_element we search the next time access (search the next timestamp for the object with the objects already processed (ES doc IDs) excluded)
                query = {"bool": {"filter": [{"term": {"path": cache_element.index}}], "must_not": [{"terms": {"_id": self._es_ids}}]}} 
                search_results = self._es.search(index="performance-test", query=query, size=1, docvalue_fields=[{"field": "@timestamp","format": "epoch_second"}], sort=[{"@timestamp": {"order": "asc"}}], version=False)
                if len(search_results["hits"]["hits"])==0:
                    obj_to_drop=cache_element
                    break
                timestamp = int(search_results["hits"]["hits"][0]["fields"]["@timestamp"][0])
                if timestamp>max_timestamp:
                    max_timestamp=timestamp # new furthest access time
                    obj_to_drop=cache_element # new object to drop

                elif timestamp==max_timestamp and cache_element.size>obj_to_drop.size: # drop the biggest object if furthest access time is the same
                    obj_to_drop=cache_element

            self._cache.remove(obj_to_drop)

    def recv(self, time: float, es_id, obj: Obj, ) -> Status:
        """
        Call this function to place a request to the cache.

        :param time: Time (epoch) of the object request.
        :param obj: The object (Obj) requested.
        :param es_id: id of the document referenced in ES
        :return: Request status (Status).
        """

        # update the internal clock
        self.clock = time

        # update the es list IDs with the coming document ID
        self._es_ids.append(es_id)

        # try to get the object from cache
        stored = self._lookup(obj)
        if stored is not None:

            # retrieved from cache, check expires
            if not stored.isexpired(self.clock):
                # HIT, "serv" object from cache
                self.__log(stored, Status.HIT)
                return Status.HIT

        # MISS: not in cache or expired --> just simulate fetch!
        obj.fetched = True

        # cache admission
        if obj.cacheable and obj.size <= self.maxsize and self._admit(obj):

            # store
            obj.enter = self.clock
            self._store(obj)

            self.__log(obj, Status.MISS)
            return Status.MISS

        else:
            self.__log(obj, Status.PASS)
            return Status.PASS

    def __log(self, obj, status: Status):
        """Basic logging"""
        if(self.__write_log):
            self.__logger.warning(f"{self.clock} {status} {obj}")



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