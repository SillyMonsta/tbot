import sql2data
import os
import signal
import data2sql
import datetime
import write2file

pid_to_kill = sql2data.pid_from_sql('stream_connection')
if pid_to_kill:
    os.kill(pid_to_kill, signal.SIGINT)
    data2sql.update_pid('stream_connection', 0)
    write2file.write(str(datetime.datetime.now())[:19] + ' STOP stream_connection', 'log.txt')
