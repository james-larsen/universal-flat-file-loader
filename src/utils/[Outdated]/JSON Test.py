"""Test"""
#%%
from pathlib import Path
import json
import datetime
import time
# import time_difference

#%%
# Job performance json
job_metrics = {}
job = {}
folders = []

#datetime.datetime.fromtimestamp(job_start_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")x
file_start_time = datetime.datetime.now()
file_end_time = datetime.datetime.now()
load_folder = ''
load_file = ''

#%%
job_start_time = datetime.datetime.now()
job['jobStart'] = datetime.datetime.fromtimestamp(job_start_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")

# Begin folder loop here
folder = {}
files = []
folder['folderPath'] = r'Y:\Python Upload Data Files\Upload\product'#load_folder
folder_start_time = datetime.datetime.now()
folder['folderStart'] = datetime.datetime.fromtimestamp(folder_start_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")

# Begin file loop here
file = {}
file['fileName'] = r'product_01.txt'#load_file
file_start_time = datetime.datetime.now()
file['fileStart'] = datetime.datetime.fromtimestamp(file_start_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
time.sleep(1)
file_end_time = datetime.datetime.now()
file['fileEnd'] = datetime.datetime.fromtimestamp(file_end_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
# days, hours, minutes, seconds, display_string = time_difference.getDuration(file_end_time, file_start_time)
# file['totalDuration'] = display_string
loaded_record_count = 123
file['recordsLoaded'] = loaded_record_count
files.append(dict(file))

file = {}
file['fileName'] = r'product_02.txt'#load_file
file_start_time = datetime.datetime.now()
file['fileStart'] = datetime.datetime.fromtimestamp(file_start_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
time.sleep(2)
file_end_time = datetime.datetime.now()
file['fileEnd'] = datetime.datetime.fromtimestamp(file_end_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
# days, hours, minutes, seconds, display_string = time_difference.getDuration(file_end_time, file_start_time)
# file['totalDuration'] = display_string
loaded_record_count = 456
file['recordsLoaded'] = loaded_record_count
files.append(dict(file))

folder_end_time = datetime.datetime.now()
folder['folderEnd'] = datetime.datetime.fromtimestamp(folder_end_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
folder['files'] = files
folders.append(dict(folder))

folder = {}
files = []
folder['folderPath'] = r'Y:\Python Upload Data Files\Upload\product2'.replace('\\\\', '\\') #load_folder
folder_start_time = datetime.datetime.now()
folder['folderStart'] = datetime.datetime.fromtimestamp(folder_start_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")

# Begin file loop here
file = {}
file['fileName'] = r'product_01.txt'#load_file
file_start_time = datetime.datetime.now()
file['fileStart'] = datetime.datetime.fromtimestamp(file_start_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
time.sleep(1)
file_end_time = datetime.datetime.now()
file['fileEnd'] = datetime.datetime.fromtimestamp(file_end_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
# days, hours, minutes, seconds, display_string = time_difference.getDuration(file_end_time, file_start_time)
# file['totalDuration'] = display_string
loaded_record_count = 123
file['recordsLoaded'] = loaded_record_count
files.append(dict(file))

file = {}
file['fileName'] = r'product_02.txt'#load_file
file_start_time = datetime.datetime.now()
file['fileStart'] = datetime.datetime.fromtimestamp(file_start_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
time.sleep(2)
file_end_time = datetime.datetime.now()
file['fileEnd'] = datetime.datetime.fromtimestamp(file_end_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
# days, hours, minutes, seconds, display_string = time_difference.getDuration(file_end_time, file_start_time)
# file['totalDuration'] = display_string
loaded_record_count = 456
file['recordsLoaded'] = loaded_record_count
files.append(dict(file))

folder_end_time = datetime.datetime.now()
folder['folderEnd'] = datetime.datetime.fromtimestamp(folder_end_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
folder['files'] = files
folders.append(dict(folder))

job_end_time = datetime.datetime.now()
job['jobEnd'] = datetime.datetime.fromtimestamp(job_end_time.timestamp()).strftime("%Y-%m-%d %H:%M:%S")
job_metrics['job'] = job
job['folders'] = folders

print(json.dumps(job_metrics, indent=4))
#%%

json_path = Path(r'C:\Users\206429242\Desktop\output.json')
with open(json_path, 'w') as f:
    json.dump(job_metrics, f, indent=4)

with open(json_path, 'r') as f:
    data = f.read()
    data = data.replace("\\\\", "\\")
with open(json_path, 'w') as f:
    f.write(data)

#%%
file_start_time = datetime.datetime.now()
#result = format_datetime_difference(file_start_time)
