from multiprocessing import Process, Queue
import csv
import datetime as DT

class Measurement:
    """
    Class to provide various measurements related to the cache performance.
    """
    
    def __init__(self, cache_queue, writing_frequency=0, writing_time=60):
        """
        Measurement initialization.
        :param writing_frequency: frequency used to write the measurements results in txt file (0 for not writing anything in file), e.g: 1,000 will write the results once every 1,000 objects processed 
        :param writing_time: frequency in time to write results in file (in seconds)
        """

        self.__hit = 0 # Number of times the cache returns a "hit" answer
        self.__miss = 0 # Number of times the cache returns a "miss" answer
        self.__pass = 0 # Number of times the cache returns a "pass" answer
        self.__previous = [0,0,0] # Previous values for hit, miss and pass
        self.__last_time = 0 # Current time
        self.__file = None # File for writing the measurements
        self.__writer = None # CSV writer
        self.__writring_frequency = writing_frequency
        self.__q = cache_queue
        self.__writring_time = writing_time
        if self.__writring_frequency!=0:
            self.__file = open("measurements.csv", "w", encoding='UTF8') # Open txt file for writing the measurements results
            self.__writer = csv.writer(self.__file)
            self.__writer.writerow(['Record', 'Hit', 'Miss', 'Pass', 'CHR'])
        if self.__writring_time!=0:
            self.__file_time = open("measurements_time.csv", "w", encoding='UTF8') # Open txt file for writing the measurements results
            self.__writer_time = csv.writer(self.__file_time)
            self.__writer_time.writerow(['Time', 'Total', 'Hit', 'Miss', 'Pass', 'CHR'])
        self.receive_status()

    def __del__(self):
        """
        Measurement destructor.
        """
        if self.__writring_frequency!=0:
            self.__file.close() # Close the file used for writing the measurements results
        if self.__writring_time!=0:
            self.__file_time.close() # Close the file used for writing the measurements results

    def receive_status(self):
        status = self.__q.get()

        while len(status)==2:
            status = self.__q.get()
            if status[1]=='h':
                self.hit()
            elif status[1] == 'p':
                self.pass_()
            elif status[1] == 'm':
                self.miss()

            if (self.__hit+self.__miss+self.__pass)%self.__writring_frequency==0:
                self.write_in_file()

            if status[0]-self.__last_time>=self.__writring_time:
                self.__last_time = status[0]
                self.write_time()


    def hit(self):
        """
        Increment the counter everytime a "hit" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__hit+=1

    
    def miss(self):
        """
        Increment the counter everytime a "miss" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__miss+=1

    def pass_(self):
        """
        Increment the counter everytime a "miss" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__pass+=1


    def reset(self):
        """
        Reset all the counters used for measurements.
        """
        self.__hit = 0 
        self.__miss = 0

    def cache_hit_ratio(self) -> float:
        """
        Compute and return the cache hit ratio.
        """
        return self.__hit/(self.__hit+self.__miss+self.__pass)

    def write_in_file(self):
        """
        Write the measurements in the file if writing frequency condition is matched.
        """
        self.__writer.writerow([self.__hit+self.__miss+self.__pass, self.__hit, self.__miss, self.__pass, round(self.cache_hit_ratio()*100,3)])
        self.__file.flush()

    def write_time(self):
        """
        Write the measurements in the file if writing frequency condition is matched.
        """
        hit = self.__hit-self.__previous[0]
        miss = self.__miss-self.__previous[1]
        pass_ = self.__pass-self.__previous[2]
        self.__writer_time.writerow([DT.datetime.utcfromtimestamp(self.__last_time).isoformat(), hit+miss+pass_, hit, miss, pass_, round((hit/(hit+miss+pass_))*100,3)])
        self.__file_time.flush()
        self.__previous = [self.__hit, self.__miss, self.__pass]