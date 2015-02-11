#!/bin/python
# -*- coding: utf-8 -*- 

import os
import sys
import time 
import argparse
import datetime
import pwd
#import matplotlib.pyplot as plt
import gettext

from pseudo_cluster.task import Task_record
from pseudo_cluster.tasks_list import Tasks_list
from pseudo_cluster.statistics_analyzer import Statistics_analyzer

def main(argv=None):
    """
        Главная функция программы
    """
    if argv == None:
        argv=sys.argv

    gettext.install('pseudo-cluster')

    parser = argparse.ArgumentParser(
            description=_(
            """
            Данная программа вычисляет метрики по файлу статистики с задачами.
            """),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    argumets_group=parser.add_mutually_exclusive_group(required=True)

    argumets_group.add_argument(
            '--metric',
            dest='metric',
            required=False,
            help=_("""
            Имя метрики, по которой будут производиться вычисления 
            для файла со статистикой.
            """)
    )

    argumets_group.add_argument(
            '--show-available-metrics',
            dest='show_metrics',
            required=False,
            default=False,
            action='store_true',
            help=_("""
                  печатает список доступных метрик, 
                  которые мотом можно указать опции
                  --metric.
                """)
    )

   
    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=False,
            default='./',
            help=_('префикс, по которому находится файл со статистикой')
    )

    parser.add_argument(
            '--result-file',
            dest='result_file',
            required=False,
            default='result.csv',
            help=_('файл, в который помещается результат вычисления метрики')
    )

    parser.add_argument(
            '--show-metric-description',
            dest='show_metric_description',
            required=False,
            default=False,
            action='store_true',
            help=_("""
                  Печатает описание 
                  конкретной метрики.
                """)
    )

    parser.add_argument(
            '--metric-arguments',
            dest='metric_arguments',
            required=False,
            default="",
            help=_(
            """
               Строка с параметрами, по которым
               вычисляется конкретная метрика.
               Желательный формат:
                --metric-arguments="unit-time='3600',count_mode='user'"
            """)
    )

    parser.add_argument(
            '--compression',
            dest='compress',
            required=False,
            type=int,
            default=1,
            help=_('Во сколько раз увеличивать все промежутки времени')
    )

    parser.add_argument(
            '--plot-file',
            dest='plot',
            required=False,
            default="", 
            help=_('Выводит график получившейся метрики в файл.')
    )


    args=parser.parse_args()

    analyzer=Statistics_analyzer()
    
    if args.show_metrics:
        metrics_list=analyzer.get_metrics_list()
        for metric in metrics_list.keys():
            print "metric '%s' - %s " % ( metric, metrics_list[metric])
        return 0

    if args.show_metric_description:
        print analyzer.get_metric_description(args.metric)
        return 0


    
    tasks_list = Tasks_list()
    tasks_list.read_statistics_from_file(args.prefix)

    analyzer.tasks_list=tasks_list
    analyzer.time_compression=args.compress

    if args.plot != "":
        args.metric_arguments+=",plot_format='true'"
    else:
        args.metric_arguments+=",plot_format='false'"

    args.metric_arguments = args.metric_arguments.strip(',')

    analyzer.register_metric_counter(args.metric, args.metric_arguments)
    analyzer.count_metric()
    analyzer.print_values(args.result_file)

    if args.plot != "":
        analyzer.plot_values(args.metric,args.plot)

    return 0
            

if __name__ == "__main__":
    sys.exit(main())
