#!/opt/python-2.7.6/bin/python
# -*- coding: utf-8 -*- 

import os
import sys
import argparse
import datetime
import pwd

class TaskRecord(object):
    """
          Класс для храненияатрибутов задач, полученных
        из файла со статистикой.
    """
    def __init__(self, info=dict()):
        """
              Creates and initialises TaskRecord with $info dict, 
            which holds task's attributes.
        """
        self.job_id = info.get('JobID', None)
        self.time_submit = int(info.get('Queue Date', None))
        self.time_start = int(info.get('Start Time', None))
        self.time_end = int(info.get('Completion Date', None))
        self.user_name = info.get('UserID', None)
        self.group_name = info.get('Class', None)
        self.time_limit = info.get('Time limit (seconds)', None)
        self.required_nodes = int(info.get('Size Requested', None))
        self.partition = info.get('Partition Allocated', None)
        self.task_state = info.get('Status', None)

        #
        # Cловарь с прочими атрибутами, ассоциированными с задачей на кластере.
        # ключ - имя атрибута, например имя поля в таблице [cluster_name]_job_table
        #
        self.other= {}   
    
    def to_string(self):
        """
            Returns row-representation for task.
        """
        return '\t'.join(map(str, [self.job_id, self.user_name, self.time_submit,\
                self.time_start, self.time_end, self.required_nodes, \
                self.task_state, self.partition, self.group_name, self.time_limit]))

class StatisticsCounter(object):
    """
        Class, that is used to extract statistics about tasks.
    """
    _CSV_FILENAME = 'statistics_parsed.csv'

    def __init__(self, mask_users, prefix):
        """
            $mask_users - whether the masqueration is needed
            $prefix - prefix of output filename
        """
        # array of TaskRecords
        self._tasks = StatisticsCounter._load_tasks(\
               StatisticsCounter._CSV_FILENAME, mask_users)

        # prefix of output filename
        self._prefix = prefix

    @staticmethod
    def _load_tasks(fname, mask_users):
        """
              Method returns an array of TaskRecord objects, taking
            information from file with name $fname. 
            $mask_users = True if masqueration is needed.
        """
        # array of TaskRecords
        result = []

        # mapping from real UserID to masked (if masqueration is used)
        user_to_dummy = dict()

        # read rows from .csv file
        inp = open(fname, 'r')
        data = inp.read().split('\n')[:-1]
        inp.close()

        headers = data[0].split('\t')

        # mapping from index (0, 1, ...) to column name
        index_to_header = dict()
        for i, header in enumerate(headers):
            index_to_header[i] = header.replace('"', '')

        # read row-by-row and make TaskRecord out of row
        for row in data[1:]:
            info = dict()
            for num, val in enumerate(row.split('\t')):
                info[index_to_header[num]] = val

            if mask_users:
                real_userid = info['UserID']

                # if this user appears for the first time, assign next
                # number to him (0, 1, ...)
                if real_userid not in user_to_dummy:
                    user_to_dummy[real_userid] = len(user_to_dummy)

                converted_userid = user_to_dummy[real_userid]

                info['UserID'] = converted_userid

            result.append(TaskRecord(info))

        return result

    def calc_number_of_tasks_in_interval(self, start_time, end_time):
        """
            Returns total number of tasks that were running 
            at any moment between start_time and end_time.
        """
        # has_intersection(task) returns True iff 
        # [task.time_start, task.time_end] intersects with
        # [start_time, end_time].
        has_intersection = lambda task: \
                max(task.time_start, start_time) <= \
                min(task.time_end, end_time)

        return len(filter(has_intersection, self._tasks))

    def calc_total_time_in_queue_in_interval(self, start_time, end_time):
        """
            Returns total time spent in queue between start_time and
            end_time by any task.
            
            Task is queued on interval [task.time_submit, task.time_end].
            If this interval insersects with [start_time, end_time],
            we add length of this intersection to result.
        """
        len_intersection = lambda task:\
            min(task.time_end, end_time) -\
            max(task.time_submit, start_time)

        return sum(filter(lambda x: x > 0, \
                map(len_intersection, self._tasks)))

    def _get_matching_tasks(self, start_time, end_time):
        """
            Returns array of TaskRecords, that has intersection
            with [start_time, end_time].

            If [task.time_submit, task.time_end] intersects with
            [start_time, end_time], that task goes to result.
        """
        has_intersection = lambda task:\
            max(task.time_submit, start_time) <= \
            min(task.time_end, end_time)

        return list(filter(has_intersection, self._tasks))

    def calc_statistics(self, start_time, end_time):
        output_fname = '%s_%d_%d' % (self._prefix, start_time, end_time)

        num_of_tasks = self.calc_number_of_tasks_in_interval(\
                start_time, end_time)

        average_time_in_queue = \
                float(\
                self.calc_total_time_in_queue_in_interval(start_time, end_time))/\
                (num_of_tasks if num_of_tasks > 0 else 1)

        tasks = self._get_matching_tasks(start_time, end_time)
        
        # file, where we output tasks that match [start_time, end_time]
        csv = output_fname + '_raw.csv'
        f_csv = open(csv, 'w')

        # file, where we output calculated statistics 
        stat = output_fname + '_stat.txt'
        f_stat = open(stat, 'w')


        # output header for .csv
        f_csv.write('\t'.join(map(lambda t: '"%s"' % t, \
            ['JobID', 'UserID', 'Queue Date',\
            'Start Time', 'Completion Date', 'Size Requested', 'Status',\
            'Partition Allocated', 'Class', 'Time limit (seconds)'])) + '\n')

        # output tasks
        f_csv.write('\n'.join(map(lambda t: t.to_string(), tasks)))
        
        # output statistics
        f_stat.write('Total number of tasks: %d\n' % num_of_tasks)
        f_stat.write('Average time in queue: %f seconds\n' % average_time_in_queue)
        f_stat.write('Average congestion: NONE\n') 
        

        f_csv.close()
        f_stat.close()


def main(argv=None):
    """
        Главная функция программы
    """
    if argv == None:
        argv=sys.argv
    
    parser = argparse.ArgumentParser(\
            description="""
            Данная программа делает выборку за некоторый период времени 
            из статистики запуска задач на вычислительном кластере 
            управляемым системой ведения очередей Slurm. 
            Результат помещается в несколько текстовых файлов.
            Программа подсчитывает: количество задач, среднее время ожидания в очереди,
            а также среднюю загруженность за данный период времени.

            По умолчанию, указав лишь начальное S и конечное T значение времени, вы получите:
            файл slurm_stat_S_T_raw.csv с описанием задач, которые выполнялись между S и T,
            файл slurm_stat_S_T_stat.txt с посчитанной статистикой.
            """,\
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,\
            epilog="Например можно запустить так: \n%s --from %d --to %d" % \
            (argv[0], 1388369696, 1388505799)
    )
    
    parser.add_argument('--from',dest='time_from',required=True, help="Дата и время с которых выбирать статистику")
    parser.add_argument('--to',dest='time_to', required=True, help="Дата и время до кoторых выбирать статистику")
   # parser.add_argument('--cluster',dest='cluster',required=True, help="Имя кластера в базе данных slurm")
    parser.add_argument('--prefix',dest='prefix',required=False, default="./slurm_stat", help="префикс, по которому сохранять выборку")
    parser.add_argument('--masquerade-users',dest='masquerade_users',required=False,default="yes", help="Если включено, все пользователи будут маскироваться под именами типа 'user123'")

    args=parser.parse_args()

    stat = StatisticsCounter(args.masquerade_users, args.prefix)
    stat.calc_statistics(int(args.time_from), int(args.time_to))

    return 0
            

if __name__ == "__main__":
    sys.exit(main())
