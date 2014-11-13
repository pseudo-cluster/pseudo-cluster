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
            '--prefix',
            dest='prefix',
            required=False,
            default="./",
            help="префикс, по которому находится файл со статистикой"
    )

    args=parser.parse_args()

    tasks_list=Tasks_list()
    tasks_list.read_statistics_from_file(args.prefix)


if __name__ == "__main__":
    sys.exit(main())
