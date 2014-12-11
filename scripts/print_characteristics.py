#!/opt/python-2.7.6/bin/python
# -*- coding: utf-8 -*- 

import os
import sys
import time 
import argparse
import datetime
import pwd
import matplotlib.pyplot as plt

from pseudo_cluster.task import Task_record
from pseudo_cluster.tasks_list import Tasks_list
from pseudo_cluster.statistics_analyzer import Statistics_analyzer

class StatisticsCounter(object):
    """
        Class, that is used to extract statistics about tasks.
    """
    AVAILABLE_METRICS = ['average_queue_time', 'average_congestion',\
            'used_time', 'cpus_median_deviation', 'queue_to_limit_time',\
            'number_of_tasks']
    AVAILABLE_TYPES = ['per_day', 'per_user', 'total']

    def __init__(self, task_list, unit_time, compress):
        self._tasks = task_list.tasks_list
        self._unit_time = unit_time
        self._compress = compress

    def calc_average_congestion(self, tasks):
        """
            Returns average congestion.
        """
        events = []
        for task in tasks:
            events.append((time.mktime(task.time_start.timetuple()) * self._compress, task.required_cpus))
            events.append((time.mktime(task.time_end.timetuple()) * self._compress, -task.required_cpus))

        events = sorted(events)

        if len(events) == 0:
            return 0

        intervals, sum_area = 0, 0 
        prev_time = events[0][0]
        cur_procs = 0

        for event in events:
            t = (event[0] - prev_time) / self._unit_time
            intervals += t

            sum_area += t * cur_procs

            cur_procs += event[1]
            prev_time = int(event[0])
        
        return sum_area / intervals

    def calc_used_time(self, tasks):
        """
            Returns sum of task.running_time / task.time_limit for completed task in 'tasks',
            divided by number of completed tasks.
        """
        success = 0
        time_portion = 0
        aborted = 0

        for task in tasks:
            if task.task_state.strip().lower() != 'completed':
                aborted += 1
            else:
                time_portion += float((task.time_end - task.time_start).total_seconds())\
                        / (task.time_limit * 60)
                success += 1

        if success > 0:
            time_portion /= success

        return (time_portion, aborted)

    def calc_queue_time_to_limit(self, tasks):
        """
            Returns sum of task.queue_time / task.time_limit for all tasks, divided by len(tasks)
        """
        result = 0.0

        for task in tasks:
            if task.time_limit == 0:
                continue

            queue_time = float((task.time_start - task.time_submit).total_seconds())
            time_limit = task.time_limit * 60 # in seconds for better precision

            result += queue_time / time_limit

        result /= len(tasks)

        return result


    def calc_median_deviation(self, tasks):
        """
            Returns average deviation from median of number of cpu-s required for each task.
        """
        cpus = [x.required_cpus for x in tasks]
        cpus.sort()

        if len(cpus) % 2:
            median = cpus[len(cpus) // 2]
        else:
            median = (cpus[len(cpus) // 2] + cpus[len(cpus) // 2 - 1]) // 2

        deviation = 0

        for cpu in cpus:
            deviation += abs(cpu - median)

        deviation /= float(len(cpus))

        return deviation

    def calc_total_time_in_queue(self, tasks):
        """
            Returns total time spent in queue by any task in 'tasks'.
            
            Task is queued on interval [task.time_submit, task.time_start].
        """
        return sum([(x.time_start - x.time_submit).total_seconds() * self._compress / 60.0 for x in tasks])

    def _conv_local_res_to_string(self, local_result):
        """
            If @local_result is an iterable, converts it to list. Otherwise, makes a list
            out of it. Then returns '\t'.join(list).
        """
        try:
            local_result = list(local_result)
        except:
            local_result = [local_result]

        return '\t'.join(map(str, local_result))

    def _calc_metric_by_list(self, lst, func):
        """
            Returns string, that represents metric calculation
        """
        if len(lst) == 0:
            return ""

        ret = ""
        try:
            n = len(lst[0])
        except:
            # 'total' case
            return self._conv_local_res_to_string(func(lst))

        for entity, ls in lst:
            local_result = self._conv_local_res_to_string(func(ls))
            ret += '%s\t%s\n' % (entity, local_result)

        return ret

    def calc_metric(self, metric, m_type, need_plot):
        """
            Calculated desired metric, subject to metric type.
            It goes as follows:
                1. Get tasks, on which we do calculations (defined by @m_type);
                2. Get function, which performs calculations (defined by @metric);
                3. Also form a header string on previous two steps;
                4. Append to header the string, which holds results of calculations.
        """
        if metric not in StatisticsCounter.AVAILABLE_METRICS:
            raise ValueError('calc_metric: Uknown metric to calculate - %s' % metric)
        if m_type not in StatisticsCounter.AVAILABLE_TYPES:
            raise ValueError('calc_metric: Unknown metric type - %s' % m_type)
        if need_plot and m_type == 'total':
            raise ValueError('calc_metric: cannot plot a single value (m_type = total)')
        if need_plot and metric == 'cpus_median_deviation':
            raise ValueError('calc_metric: cannot plot functions, that return tuples')

        result = ''
        tasks = None
        func = None
        plt.title(u'Расчёт метрики')

        users = set([x.user_name for x in self._tasks])
        days = set([x.time_start.date() for x in self._tasks])

        # tasks will hold the list of (entity, task_list), so then we calculate metric for
        # each entity. This is not true for @metric == 'total' case, in which there is only one list.
        if m_type == 'per_user':
            plt.xlabel(u'Пользователь')
            result += 'user\t'
            tasks = [(user, list(filter(lambda x: x.user_name == user, self._tasks)))\
                    for user in sorted(users)]
        elif m_type == 'per_day':
            plt.xlabel(u'День')
            result += 'day\t'
            tasks = [(day, list(filter(lambda x: x.time_start.date() == day, self._tasks)))\
                    for day in sorted(days)]
        elif m_type == 'total':
            result += 'total '
            tasks = self._tasks

        if metric == 'average_queue_time':
            plt.ylabel(u'Среднее время в очереди')
            result += 'average queue time (minutes)\n'
            func = lambda x: float(self.calc_total_time_in_queue(x)) / len(x)
        elif metric == 'average_congestion':
            plt.ylabel(u'Средняя загруженность')
            func = self.calc_average_congestion
            result += 'average congestion\n'
        elif metric == 'used_time':
            plt.ylabel(u'Среднее использованное время')
            result += 'used portion of time limit\t# of aborted tasks\n'
            func = self.calc_used_time
        elif metric == 'cpus_median_deviation':
            plt.ylabel(u'Среднее медианное отклонение кол-ва процессоров')
            result += 'average cpu deviation\n'
            func = self.calc_median_deviation
        elif metric == 'queue_to_limit_time':
            plt.ylabel(u'Среднее отношение ожидания в очереди к таймлимиту')
            result += 'wait time / time limit\n'
            func = self.calc_queue_time_to_limit
        elif metric == 'number_of_tasks':
            plt.ylabel(u'Количество задач')
            result += 'tasks\n'
            func = lambda x: len(x)

        if need_plot:
            calculated = self._calc_metric_by_list(tasks, func).strip().split('\n')

            y = []
            for line in calculated:
                x0, y0 = line.split('\t')
                y.append(y0)

            plt.plot(y)
            plt.show()
            sys.exit(0)
        else:
            result += self._calc_metric_by_list(tasks, func)
            return result

def main(argv=None):
    """
        Главная функция программы
    """
    if argv == None:
        argv=sys.argv
    
    parser = argparse.ArgumentParser(
            description=\
            """
            Данная программа вычисляет метрики по файлу статистики с задачами.
            """,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
 
    parser.add_argument(
            '--metric',
            dest='metric',
            required=True,
            help="""
            Имя метрики, по которой будут производиться вычисления 
            для файла со статистикой.
            """
    )
   
    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=False,
            default='./',
            help='префикс, по которому находится файл со статистикой'
    )

    parser.add_argument(
            '--show-available-metrics',
            dest='show_metrics',
            required=False,
            default=False,
            action='store_true',
            help="""
                  печатает список доступных метрик, 
                  которые мотом можно указать опции
                  --metric.
                """
    )

    parser.add_argument(
            '--show-metric-description',
            dest='show_metric_description',
            required=False,
            default=False,
            action='store_true',
            help="""
                  Печатает описание 
                  конкретной метрики.
                """
    )

    parser.add_argument(
            '--metric-arguments',
            dest='metric_arguments',
            required=False,
            default="",
            help=\
              """
               Строка с параметрами, по которым
               вычисляется конкретная метрика.
               Желательный формат:
                --metric-arguments="unit-time='3600',count_mode='user'"
              """
    )

    parser.add_argument(
            '--compression',
            dest='compress',
            required=False,
            default=1.0,
            help='Во сколько раз увеличивать все промежутки времени'
    )

    parser.add_argument(
            '--plot',
            dest='plot',
            required=False,
            default=False,
            action='store_true', 
            help='Если опция включена, то выводит график получившейся метрики.'
    )


    args=parser.parse_args()

    analyzer=Statistics_analyzer()
    
    if args.show_metrics:
        print analyzer.get_metrics_list()
        return 0

    if args.show_metric_description:
        analyzer.get_metric_description(args.metric)
        return 0


    args.unit_time = float(args.unit_time)
    args.compress = float(args.compress)
    
    task_list = Tasks_list()
    task_list.read_statistics_from_file(args.prefix)

    counter = StatisticsCounter(task_list, args.unit_time, args.compress)

    print counter.calc_metric(args.metric, args.m_type, args.plot == 'Yes')

    return 0
            

if __name__ == "__main__":
    sys.exit(main())
