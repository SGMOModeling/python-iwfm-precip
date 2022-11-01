import time

class Timer:
    def __init__(self):
        self._start_time = None 
        self._end_time = None
        self._timer_started = False
        
    def start(self):
        self._start_time = time.time()
        self._timer_started = True
        
    def stop(self):
        if self._timer_started:
            self._end_time = time.time()
            self._timer_started = False
            
    def print_run_time(self):
        run_time = self._end_time - self._start_time

        # convert duration to hours, minutes, seconds
        minutes, seconds = divmod(run_time, 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            msg = "TOTAL RUN TIME: {} HOURS {} MINUTES {:6.3f} SECONDS".format(hours, minutes, seconds)
        elif minutes > 0:
            msg = "TOTAL RUN TIME: {} MINUTES {:6.3f} SECONDS".format(minutes, seconds)
        else:
            msg = "TOTAL RUN TIME: {:6.3f} SECONDS".format(seconds)

        print(msg)