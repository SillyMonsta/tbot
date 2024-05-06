import sql2data
import os
import signal
import data2sql
import datetime
import write2file

pid_to_kill = sql2data.pid_from_sql('telegram')
if pid_to_kill:
    os.kill(pid_to_kill, signal.SIGINT)
    data2sql.update_pid('telegram', 0)
    write2file.write(str(datetime.datetime.now())[:19] + ' STOP telegram', 'log.txt')
