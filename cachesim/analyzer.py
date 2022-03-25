from cachesim import Status
import csv
import datetime as dt
import multiprocessing
from collections import Counter


class Analyzer:
    """
    Class to provide various measurements and analyzes related to the cache performance.
    """

    def __init__(self, cache_queue: multiprocessing.Queue, writing_frquency_time=60, writing_frequency_number=0, movies_time_interval = 0, CHR_final = True,
                 file_name_frequency_time="CHR_by_time", file_name_frequency_number="CHR_regular", file_name_CHR_final="CHR_final", file_name_CHR_by_movie = "CHR_movies"):
        """
        Analyzer initialization.
        :param cache_queue: queue between the process in charge of the caching simulation and the analyzer process
        :param writing_frequency_number: frequency used to write the measurements results in txt file (0 for not writing anything in file), e.g: 1,000 will write the results once every 1,000 objects processed, 0 to disable
        :param writing_frquency_time: frequency in time to write results in file (in seconds), e.g: 60 means that the results will be written once every minutes (approximately, results are only sent periodically to the analyzer), 0 to disable
        :param movies_time_interval: interval in time to which statistics about the cache hit ratio of the different movies should be written in file
        :param file_name_frequency_time: name of the file where the analyzes by time should be written
        :param file_name_frequency_number: name of the file where the analyzes by frequency should be written
        :param CHR_final: True if the final cache hit ratio should be written in file at the end, false otherwise
        """
        self.__q = cache_queue  # Queue between the process managing the analyzer process and the cache simulation process (data are received to this analyzer from the cache simulation process)

        self.__hit = 0  # Number of times the cache returns a "hit" answer
        self.__miss = 0  # Number of times the cache returns a "miss" answer
        self.__pass = 0  # Number of times the cache returns a "pass" answer
        self.__previous = [0,0,0] # Previous values for hit, miss and pass
        self.__movies = {} # Dictionnary containing the simulator answers (hit, miss, pass) by movies. Key: name of the movie, value: tuple with number of (hit, miss, pass)

        self.__last_time = 0  # Last timestamp registered by the analyzer object
        self.__last_time_movie = 0  # Last timestamp registered by the analyzer object for movie results
        self.__last_total = 0  # Keep trace of the last total number of analyzes done
        
        self.__CHR_final = CHR_final  # Look at CHR_final parameter description for more info
        self.__frequency_number = writing_frequency_number  # Look at writing_frequency_number parameter description for more info
        self.__frequency_time = writing_frquency_time  # Look at writing_frquency_time parameter description for more info
        self.__movies_time_interval = movies_time_interval # Look at movies_time_interval parameter description for more info
        self.__file_name_CHR_final = file_name_CHR_final  # Look at file_name_CHR_final parameter description for more info

        # Creation of storing files
        # Cache hit ratio by frequency
        if self.__frequency_number != 0:
            self.__file = open("./results/" + file_name_frequency_number + ".csv", "w",
                               encoding='UTF8')  # Open txt file for writing the analyzes results
            self.__writer = csv.writer(self.__file)  # Open CSV file
            self.__writer.writerow(['Record', 'Hit', 'Miss', 'Pass', 'CHR'])  # Write CSV header

        # Cache hit ratio by time
        if self.__frequency_time != 0:
            self.__file_time = open("./results/" + file_name_frequency_time + ".csv", "w",
                                    encoding='UTF8')  # Open txt file for writing the analyzes results
            self.__writer_time = csv.writer(self.__file_time)  # Open CSV file
            self.__writer_time.writerow(['Time', 'Total', 'Hit', 'Miss', 'Pass', 'CHR'])  # Write CSV header

        # Cache hit ratio by movie
        if self.__movies_time_interval != 0:
            self.__file_movie = open("./results/" + file_name_CHR_by_movie + ".csv", "w",
                                    encoding='UTF8')  # Open txt file for writing the analyzes results
            self.__writer_movie = csv.writer(self.__file_movie)  # Open CSV file
            self.__writer_movie.writerow(['MovieID', 'Epoch_second', 'Hit', 'Miss', 'Pass', 'CHR'])  # Write CSV header

        # Launch function managing the receiving of the data from the cache simulation process and launching the corresponding analyzes tasks when received
        self.receive_status()
        cache_queue.close()

    def __del__(self):
        """
        Measurement destructor.
        """
        # Close the file used for writing the measurements results
        if self.__frequency_number != 0:
            self.__file.close()

            # Close the file used for writing the measurements results
        if self.__frequency_time != 0:
            self.__file_time.close()

    def receive_status(self):
        """
        Receive the fata from the cache simulation process and launch the analyzes on these data. 
        :param cache_queue:
        """
        status = self.__q.get()  # Receive the data from the cache simulation process
        self.__last_time_movie = status[0]

        while status is not None:  # None is sent by the cache simulation when the simulation is over
            timestamp = status[0]
            count_status = Counter(status[1])  # Count the number of hit, pass and miss received

            # Corresponding status counter are incremented accordingly to the data received
            self.__hit += count_status[Status.HIT]
            self.__pass += count_status[Status.PASS]
            self.__miss += count_status[Status.MISS]

            # If frequency conditions are met, start to write the CHR results
            if (self.__hit + self.__miss + self.__pass) - self.__last_total >= self.__frequency_number != 0:
                self.__last_total = self.__hit + self.__miss + self.__pass
                self.save_frequency_results()

            # If time conditions are met, start to write the CHR results
            if timestamp - self.__last_time >= self.__frequency_time != 0:
                self.__last_time = timestamp
                self.save_time_results()

            if self.__movies_time_interval != 0:
                for index, movie_name in enumerate(status[2]):
                    if movie_name == -1: continue # -1 means that the movie name is not documented
                    if status[1][index] == Status.HIT:
                        self.__movies[movie_name] = tuple(map(sum, zip(self.__movies.get(movie_name, (0,0,0)), (1,0,0))))
                    elif status[1][index] == Status.PASS:
                        self.__movies[movie_name] = tuple(map(sum, zip(self.__movies.get(movie_name, (0,0,0)), (0,0,1))))
                    else:
                        self.__movies[movie_name] = tuple(map(sum, zip(self.__movies.get(movie_name, (0,0,0)), (0,1,0))))

                if timestamp - self.__last_time_movie >= self.__movies_time_interval:
                    self.__last_time_movie = timestamp
                    self.save_movies_results()

            status = self.__q.get()

        # End of the data: write the last analyzes before end of the function
        if (self.__hit + self.__miss + self.__pass) != self.__last_total and self.__frequency_number != 0:
                self.__last_total = self.__hit + self.__miss + self.__pass
                self.save_frequency_results()

        if timestamp != self.__last_time and self.__frequency_time != 0:
            self.__last_time = timestamp
            self.save_time_results()
        
        if self.__CHR_final and self.__last_total!=0:
            with open("./results/" + self.__file_name_CHR_final + ".csv",'w',encoding = 'utf-8') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(['Total', 'CHR', 'Hit', 'Miss', 'Pass'])
                csv_writer.writerow([self.__hit + self.__miss + self.__pass, self.cache_hit_ratio()*100, self.__hit, self.__miss, self.__pass])

    def hit(self):
        """
        Increment the counter everytime a "hit" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__hit += 1

    def miss(self):
        """
        Increment the counter everytime a "miss" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__miss += 1

    def pass_(self):
        """
        Increment the counter everytime a "miss" result is emitted by the cache. Call function to write in file if necessary.
        """
        self.__pass += 1

    def reset(self):
        """
        Reset all the counters used for measurements.
        """
        self.__hit = 0
        self.__miss = 0
        self.__pass = 0

    def cache_hit_ratio(self) -> float:
        """
        Compute and return the current cache hit ratio.
        """
        return self.__hit / (self.__hit + self.__miss + self.__pass)

    def save_frequency_results(self):
        """
        Write the analyzes results on the disk.
        """
        self.__writer.writerow([self.__hit + self.__miss + self.__pass, self.__hit, self.__miss, self.__pass,
                                round(self.cache_hit_ratio() * 100, 3)])  # cache hit ratio (CHR) writing
        self.__file.flush()

    def save_time_results(self):
        """
        Write the analyzes results on the disk.
        """
        hit = self.__hit - self.__previous[0]
        miss = self.__miss - self.__previous[1]
        pass_ = self.__pass - self.__previous[2]
        self.__writer_time.writerow(
            [dt.datetime.utcfromtimestamp(self.__last_time).isoformat(), hit + miss + pass_, hit, miss, pass_,
             round((hit / (hit + miss + pass_)) * 100, 3)])  # cache hit ratio (CHR) writing
        self.__file_time.flush()
        self.__previous = [self.__hit, self.__miss, self.__pass]
    
    def save_movies_results(self):
        """
        Write the analyzes results on the disk.
        """
        for movie_name, simulation_result in self.__movies.items():
            self.__writer_movie.writerow([movie_name, self.__last_time_movie, simulation_result[0], simulation_result[1], simulation_result[2], round((simulation_result[0] / (simulation_result[0] + simulation_result[1] + simulation_result[2])) * 100)])
        self.__file_movie.flush()
        self.__movies.clear()