#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
import time
import gettext


from pseudo_cluster.task import  Task_record
from pseudo_cluster.tasks_list import  Tasks_list
from pseudo_cluster.extended_task import Extended_task_record
from pseudo_cluster.actions_list import Action_list

def get_submit_string(self,time_limit,duration):
    """
    Функция генерирует строку для submit
    задачи в очередь slurm
    """
    s=list()
    s.append("sbatch")
    #
    # Uncomment for debug slurm
    #
    #s.append("-vv")
    s.append("--account=%s" % self.task_class)
    s.append("--comment=\"Pseudo cluster emulating task\"")
    s.append("--job-name=\"pseudo_cluster|%s|%s\"" % (self.job_id, self.job_name))
    try:
        limit=self.other["memory_limit"]
    except KeyError:
        limit="0"
    if int(limit) > 0:
        s.append("--mem=%d" % int(limit))
    s.append("--ntasks=%d" % self.required_cpus)
    s.append("--partition=%s" % self.partition)
    if self.priority !=0:
        s.append("--priority=%d" % self.priority)
    
    if time_limit > 0:
        s.append("--time=%d" % time_limit)
    #
    # Path to this script must be available 
    # from environment variable PATH  
    #
    s.append(self.path_to_task)
    s.append("-t")
    s.append(str(duration))
    s.append("-s")
    s.append(self.task_state)

    return s

def get_cancel_string(self):
    return [ "scancel" , str(self.actual_task_id) ]

def parse_task_id(self,f,first_line):
    """
    Выковыривает ID задачи из файла и первой строчки,
    которая была до этого прочитана в файле.
    файл не закрывает.
    """
    try:
        tup=first_line.split(' ')
    except:
        return False

    if (tup[0] == "Submitted") and (tup[1] == "batch"):
        self.actual_task_id=int(tup[3])
        return True
    return False
    

def main(argv=None):
    """
    То, с чего начинается программа
    """
    if argv == None:
        argv=sys.argv

    gettext.install('pseudo-cluster')
    
    parser= argparse.ArgumentParser(
            description=_("""
            Данная программа осуществляет постановку задач в очередь Slurm.
            Список задач получается из файла статистики. При этом программа
            ставит задачу в очередь с идентификатором пользователя и группы,
            как они были указаны в статистике. Всё используемое задачами 
            время сжимается согласно коэффициента, и вместо реалного кода 
            программы, запускавшейся задачи запускается скрипт, который ничего
            не делает определённое количество секунд.
            """),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            epilog=_("Например можно запустить так:\n ")+argv[0]+" --time-compress 30"
    )
    
    parser.add_argument(
            '--time-compress',
            dest='compress_times',
            type=int,
            required=True,
            help=_("Во сколько раз сжимать время. Напиример: 10")
    )
    parser.add_argument(
            '--time-interval',
            dest='interval',
            type=int,
            required=False,
            default=2,
            help=_("Раз во сколько минут обращаться к системе ведения очередей")
    )

    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=False,
            default="./",
            help=_("префикс, по которому находится файл со статистикой")
    )

    parser.add_argument(
            '--path-to-task-script',
            dest='path_to_task',
            required=False,
            default="/usr/local/bin/pseudo_cluster_task.sh",
            help=_("""
                    Путь до скрипта, который реализует тело задачи
                    в псевдокластере.
                 """)
    )

    args=parser.parse_args()

    if os.geteuid() != 0:
        print _("""
                Данная программа требует 
                полномочий пользователя root.

                Запустите её от имени пользователя root,
                либо с использованием команды sudo.
              """)
        return 2
    
    #
    # Регистрация методов, которые будут вызываться для объекта
    # класса Extended_task_record
    #
    Extended_task_record.get_submit_string=get_submit_string
    Extended_task_record.get_cancel_string=get_cancel_string
    Extended_task_record.parse_task_id=parse_task_id

    tasks_list=Tasks_list()
    tasks_list.read_statistics_from_file(args.prefix)

    extended_tasks=dict()

    num_tasks=len(tasks_list)
    begin_time=tasks_list[0].time_submit
    last_task=0;
    
    actions_list=Action_list()

    while last_task != num_tasks-1:
        end_time=begin_time+datetime.timedelta(minutes=args.interval*args.compress_times)
        begin_actions_time=datetime.datetime.utcnow()
        for i in xrange(0,num_tasks):
            if i < last_task:
                continue
            task=tasks_list[i]
            if task.time_submit < begin_time:
               last_task=i
            if task.time_submit < end_time:
                if task.job_id not in extended_tasks:
                    extended_task=Extended_task_record()
                    extended_task.fill_by_task(task,args.path_to_task)
                    actions_list.register_action(extended_task,"submit")
                    extended_tasks[task.job_id]=extended_task
            
            if (task.time_end < end_time) and (task.task_state == "canceled"):
                actions_list.register_action(extended_tasks[task.job_id],"cancel")

        actions_list.do_actions(args.compress_times)
               
        print begin_time
        print end_time
        print "last_task=%d, num_tasks=%d" % (last_task,num_tasks)
        
        delay_value = datetime.datetime.utcnow()- begin_actions_time
        if delay_value < datetime.timedelta(minutes=args.interval):
            how_much_sleep=args.interval*60-delay_value.total_seconds()
            print (_("will sleep %d") % how_much_sleep)
            time.sleep(how_much_sleep)
            
        begin_time=end_time
       



if __name__ == "__main__":
    sys.exit(main())
