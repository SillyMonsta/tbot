import sql2data
import os
import signal
import data2sql

pid_to_kill = sql2data.pid_from_sql('stream_connection')
os.kill(pid_to_kill, signal.SIGINT)
data2sql.update_pid('stream_connection', 0)
