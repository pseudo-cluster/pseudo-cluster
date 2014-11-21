#!/opt/python-2.7.6/bin/python
# -*- coding: utf-8 -*- 

import os
import sys
import time 
import argparse
import datetime
import pwd

from pseudo_cluster.task import Task_record
from pseudo_cluster.tasks_list import Tasks_list

class StatisticsCounter(object):
    """
        Class, that is used to extract statistics about tasks.
    """
    AVAILABLE_METRICS = ['average_queue_time', 'average_congestion',\
            'used_time', 'cpus_median_deviation']

    def __init__(self, task_list, unit_time):
        self._tasks = task_list.tasks_list
        self._unit_time = unit_time

    def calc_average_congestion(self):
        """
            Returns average congestion.
        """
        events = []
        for task in self._tasks:
            events.append((time.mktime(task.time_start.timetuple()), task.required_cpus))
            events.append((time.mktime(task.time_end.timetuple()), -task.required_cpus))

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
            Returns sum of task.running_time / task.time_limit for completed task in 'tasks'.
        """
        success = 0
        time_portion = 0
        aborted = 0

        for task in tasks:
            if task.task_state.strip().lower() != 'completed':
                aborted += 1
            else:
                time_portion += float((task.time_end - task.time_start).total_seconds()\
                        ) / task.time_limit
                success += 1

        if success > 0:
            time_portion /= success

        return (time_portion, aborted)

    def calc_median_deviation(self, tasks):
        """
            Returns average deviation from median of number of cpu-s required for each task.
        """
        cpus = [x.required_cpus for x in tasks]
        cpus.sort()

        median = cpus[len(cpus) // 2]

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
        return sum([(x.time_start - x.time_submit).total_seconds() / 60.0 for x in tasks])

    def calc_metric(self, metric):
        if metric not in StatisticsCounter.AVAILABLE_METRICS:
            raise ValueError('calc_metric: Uknown metric to calculate - %s' % metric)

        if metric == 'average_queue_time':
            users = set([x.user_name for x in self._tasks])

            result = 'user\taverage queue time (minutes)\n'

            for user in users:
                tasks_by_user = list(filter(lambda x: x.user_name == user, self._tasks))

                av_time = float(self.calc_total_time_in_queue(tasks_by_user)) / \
                        len(tasks_by_user)

                result += '%s\t%f\n' % (user, av_time)

            return result
        elif metric == 'average_congestion':
            return 'Average congestion: %f' % self.calc_average_congestion()
        elif metric == 'used_time':
            users = set([x.user_name for x in self._tasks])

            result = 'user\tused portion of time limit\t# of aborted tasks\n'

            for user in users:
                tasks_by_user = list(filter(lambda x: x.user_name == user, self._tasks))

                portion, aborted = self.calc_used_time(tasks_by_user)
                    
                result += '%s\t%f\t%d\n' % (user, portion, aborted)

            return result
        elif metric == 'cpus_median_deviation':
            users = set([x.user_name for x in self._tasks])

            result = 'user\taverage cpu deviation\n'

            for user in users:
                tasks_by_user = list(filter(lambda x: x.user_name == user, self._tasks))

                result += '%s\t%f\n' % (user, self.calc_median_deviation(tasks_by_user))

            result += '\nday\taverage cpu deviation\n'

            days = set([x.time_start.date() for x in self._tasks])

            for day in sorted(list(days)):
                tasks_by_day = list(filter(lambda x: x.time_start.date() == day, self._tasks))

                result += '%s\t%f\n' % (day, self.calc_median_deviation(tasks_by_day))

            result += '\ntotal\taverage cpu deviation\n'
            result += '---\t%f\n' % self.calc_median_deviation(self._tasks)

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
            choices=StatisticsCounter.AVAILABLE_METRICS,
            help='Какую метрику следует вычислять'
    )
   
    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=False,
            default='./',
            help='префикс, по которому находится файл со статистикой'
    )

    parser.add_argument(
            '--unit-time',
            dest='unit_time',
            required=False,
            default=3600.0,
            help=\
            '''
             Сколько минут считать за единицу времени при подсчёте средней загруженности.
             Например: 3600.0
            '''
    )


    args=parser.parse_args()

    task_list = Tasks_list()
    task_list.read_statistics_from_file(args.prefix)

    counter = StatisticsCounter(task_list, args.unit_time)

    print counter.calc_metric(args.metric)

    return 0
            

if __name__ == "__main__":
    sys.exit(main())
