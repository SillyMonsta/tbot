import datetime
import requests
import data2sql
import action
import os

figi_list = action.prepare_stream_connection()
#pid = os.getpid()
#data2sql.update_pid('stream_connection', pid)
requests.stream_connection(figi_list)

