import csv

class Measurement:
    """
    Class to provide various measurements related to the cache performance.
    """
    
    def __init__(self, writing_frequency=0):
        """
        Measurement initialization.
        :param writing_frequency: frequency used to write the measurements results in txt file (0 for not writing anything in file), e.g: 1,000 will write the results once every 1,000 objects processed 
        """

        self.__hit = 0 # Number of times the cache returns a "hit" answer
        self.__miss = 0 # Number of times the cache returns a "miss" answer
        self.__pass = 0 # Number of times the cache returns a "pass" answer
        self.__file = None # File for writing the measurements
        self.__writer = None # CSV writer
        self.__writring_frequency = writing_frequency
        if self.__writring_frequency!=0:
            self.__file = open("measurements.csv", "w", encoding='UTF8') # Open txt file for writing the measurements results
            self.__writer = csv.writer(self.__file)
            self.__writer.writerow(['Record', 'Hit', 'Miss', 'Pass', 'CHR'])

    def __del__(self):
        """
        Measurement destructor.
        """
        if self.__writring_frequency!=0:
            self.__file.close() # Close the file used for writing the measurements results

    def hit(self):
        """
        Increment the counter everytime a "hit" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__hit+=1
        if self.__writring_frequency!=0:
            self.write_in_file()

    
    def miss(self):
        """
        Increment the counter everytime a "miss" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__miss+=1
        if self.__writring_frequency!=0:
            self.write_in_file()

    def pass_(self):
        """
        Increment the counter everytime a "miss" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__pass+=1
        if self.__writring_frequency!=0:
            self.write_in_file()


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
        if (self.__hit+self.__miss+self.__pass)%self.__writring_frequency==0:
            self.__writer.writerow([self.__hit+self.__miss+self.__pass, self.__hit, self.__miss, self.__pass, round(self.cache_hit_ratio()*100,3)])
            self.__file.flush()