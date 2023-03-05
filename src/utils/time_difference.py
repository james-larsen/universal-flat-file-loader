from datetime import datetime

def getDuration(then, now = datetime.now()):#, interval = "default"):

    # Returns a duration as specified by variable interval
    # Functions, except totalDuration, returns [quotient, remainder]

    duration = now - then # For build-in functions
    duration_in_s = duration.total_seconds() 
    
    def years():
      return divmod(duration_in_s, 31536000) # Seconds in a year=31536000.

    def days(seconds = None):
      return divmod(seconds if seconds != None else duration_in_s, 86400) # Seconds in a day = 86400

    def hours(seconds = None):
      return divmod(seconds if seconds != None else duration_in_s, 3600) # Seconds in an hour = 3600

    def minutes(seconds = None):
      return divmod(seconds if seconds != None else duration_in_s, 60) # Seconds in a minute = 60

    def seconds(seconds = None):
      if seconds != None:
        return divmod(seconds, 1)   
      return duration_in_s

    def totalDuration():
        years_list = years()
        days_list = days(years_list[1]) # Use remainder to calculate next variable
        hours_list = hours(days_list[1])
        mins_list = minutes(hours_list[1])
        secs_list = seconds(mins_list[1])

        return_string = ''

        if int(days_list[0]) > 0:
            return_string += f'{int(days_list[0])} days, '

        if int(hours_list[0]) > 0:
            return_string += f'{int(hours_list[0])} hours, '

        if int(mins_list[0]) > 0:
            return_string += f'{int(mins_list[0])} minutes, '

        return_string += f'{int(secs_list[0])} seconds'
        
        #return "Time between dates: {} days, {} hours, {} minutes and {} seconds".format(int(d[0]), int(h[0]), int(m[0]), int(s[0]))
        return return_string

    return (
        int(days()[0]),
        int(hours()[0]),
        int(minutes()[0]),
        int(seconds()),
        totalDuration()
    )

    # return {
    #     #'years': int(years()[0]),
    #     'days': int(days()[0]),
    #     'hours': int(hours()[0]),
    #     'minutes': int(minutes()[0]),
    #     'seconds': int(seconds()),
    #     'display_string': totalDuration()
    # }#[interval]