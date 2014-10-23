#!/opt/python-2.7.6/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
import time
import pwd

class Task_record(object):
    """
    Класс для хранения атрибутов задач, полученных
    из файла со статистикой.
    """
    def __init__(self):
        self.job_id=None
        self.job_name=None
        self.time_submit=None
        self.time_start=None
        self.time_end=None
        self.user_name=None
        self.group_name=None
        self.time_limit=None
        self.required_nodes=None
        self.partition=None
        self.task_state=None
        
        #
        # Cловарь с прочими атрибутами, ассоциированными с задачей на кластере.
        # ключ - имя атрибута, например имя поля в таблице [cluster_name]_job_table
        #
        self.other= {}   
    

def main(argv=None):
    """
    Главная функция программы
    """
    if argv == None:
        argv=sys.argv
    
    parser= argparse.ArgumentParser(\
            description="""
            Данная программа делает выборку за некоторый период времени 
            из статистики запуска задач на вычислительном кластере 
            управляемым системой ведения очередей Slurm. 
            Результат помещается в несколько текстовых файлов.
            """,\
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,\
            epilog="Например можно запустить так:\n \""+sys.argv[0]\
    )
    
    parser.add_argument('--from',dest='time_from',required=True, help="Дата и время с которых выбирать статистику")
    parser.add_argument('--to',dest='time_to', required=True, help="Дата и время до кoторых выбирать статистику")
    parser.add_argument('--cluster',dest='cluster',required=True, help="Имя кластера в базе данных slurm")
    parser.add_argument('--prefix',dest='prefix',required=False, default="./slurm_stat", help="префикс, по которому сохранять выборку")
    parser.add_argument('--masquerade-users',dest='masquerade_users',required=False,default="yes", help="Если включено, все пользователи будут маскироваться под именами типа 'user123'")
    
    args=parser.parse_args()
   
    
    time_obj=time.mktime(time.strptime(args.time_from,"%Y-%m-%d %H:%M"))
    print(time_obj)
    from_unix_time=datetime.datetime.fromtimestamp(time_obj)
      

    return 0
            

if __name__ == "__main__":
    sys.exit(main())
