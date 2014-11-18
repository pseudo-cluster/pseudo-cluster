#!/opt/python-2.7.6/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
import pwd
import grp

from pseudo_cluster.task import  Task_record
from pseudo_cluster.tasks_list import  Tasks_list
from pseudo_cluster.extended_task import Extended_task_record

def get_submit_string(self,time_limit,duration):
    """
    Функция генерирует строку, для submit
    задачи в очередь slurm
    """
    s="sbatch "
    s+="--account=\"%s\" " % self.task_class
    s+="--comment=\"Pseudo cluster emulating task\" "
    s+="--job-name=\"pseudo_cluster|%s|%s\" " % (self.job_id, self.job_name)
    try:
        limit=self.other["memory_limit"]
    except KeyError:
        limit="0"
    if int(limit) > 0:
        s+="--mem=\"%s\" "   % limit
    s+="--ntasks=\"%d\" "    % self.required_cpus
    s+="--partition=\"%s\" " % self.partition
    if self.priority !=0:
        s+="--priority=\"\" " % self.priority
    
    if time_limit > 0:
        s+="--time=\"%d\" " % time_limit

    s+="./pseudo_cluster_task.sh -t %d -s \"%s\""

    return s

def get_cancel_string(self):
    return "scancel %s" % actual_task_id

def main(argv=None):
    """
    То, с чего начинается программа
    """
    if argv == None:
        argv=sys.argv
    
    parser= argparse.ArgumentParser(
            description="""
            Данная программа осуществляет постановку задач в очередь Slurm.
            Список задач получается из файла статистики. При этом программа
            ставит задачу в очередь с идентификатором пользователя и группы,
            как они были указаны в статистике. Всё используемое задачами 
            время сжимается согласно коэффициента, и вместо реалного кода 
            программы, запускавшейся задачи запускается скрипт, который ничего
            не делает определённое количество секунд.
            """,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            epilog="Например можно запустить так:\n \""+sys.argv[0]
    )
    
    parser.add_argument(
            '--time-compress',
            dest='compress_times',
            required=True,
            help="Во сколько раз сжимать время. Напиример: 10"
    )
    parser.add_argument(
            '--time-interval',
            dest='interval',
            required=False,
            default=2,
            help="Раз во сколько минут обращаться к системе ведения очередей"            
    )

    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=False,
            default="./",
            help="префикс, по которому находится файл со статистикой"
    )

    args=parser.parse_args()
    
    #
    # Регистрация методов, которые будут вызываться для объекта
    # класса Extended_task_record
    #
    Extended_task_record.get_submit_string=get_submit_string
    Extended_task_record.get_cancel_string=get_cancel_string

    tasks_list=Tasks_list()
    tasks_list.read_statistics_from_file(args.prefix)

    extended_tasks={}

    begin_time=tasks_list[0].time_submit
    end_time=begin_time+datatime.timedelta(minutes=args.interval*args.compress_times)
    num_tasks=len(tasks_list)
    last_task=0;
    
    actions_list=Action_list()

    while last_task < num_tasks:
        for i in xrange(last_task,num_tasks):
            task=tasks_list[i]
            if task.time_submit > end_time:
                last_task=i
                break
            else:
                actions_list.register_action(Extended_task_record(task,"submit"))
            #TODO
            # добавить действие по остановке задачи
            #
           




if __name__ == "__main__":
    sys.exit(main())
