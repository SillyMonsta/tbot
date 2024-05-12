
import tinkoff_requests
import data2sql
import sql2data
import action
import os

events_extraction_works = sql2data.pid_from_sql('events_extraction')
if not events_extraction_works:

    figi_list = action.prepare_stream_connection()
    pid = os.getpid()
    data2sql.update_pid('stream_connection', pid)
    tinkoff_requests.stream_connection(figi_list)

