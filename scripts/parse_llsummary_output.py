#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import sys
import pwd
import grp
import time
import datetime
import argparse
import string
import gettext

from pseudo_cluster.task import Task_record
from pseudo_cluster.tasks_list import Tasks_list

'''
    Converts statistics file into .csv format.
    
    Example usage:
        python parse.py --file ./salnikov_stat
'''

data_format = None

def convert_time(time_str):
    """ 
        Converts time string (e.g. "Thu 07 Mar 2013 13:45:29 MSK") into timestamp.
    """
    time_str = time_str.strip()
    while time_str[-1] not in string.digits:
        time_str = time_str[:-1]
    t = time.strptime(time_str, data_format)
    t = time.mktime(t)
    return t

def main(argv=None):
    """
        Main function
    """
    if argv == None:
        argv = sys.argv

    gettext.install('pseudo-cluster')
    
    parser = argparse.ArgumentParser(
            description=_("""
                Данная программа переводит файл со статистикой запуска задач
                на вычислительном кластере, управляемом системой ведения очередей
                Slurm, в формат .csv
                """),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
            '--file',
            dest='input',
            required=True,
            help=_('Название файла со статистикой')
    )

    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=False,
            default='./',
            help=_('Префикс, по которому сохранять выборку')
    )

    parser.add_argument(
            '--masquerade-users',
            dest='masquerade_users',
            required=False,
            default="Yes",
            help=_("""
                    Если включено, все пользователи будут маскироваться 
                    под именами типа 'user123'
                 """)
    )

    parser.add_argument(
            '--extract-logins',
            dest='extract_real_names',
            required=False,
            default="Yes",
            help=_("""
                    Если включено, для всех пользователей и групп по 
                    идентификаторам будут искаться их имена.
                 """)
    )
            

    args = parser.parse_args()

    tasks_list = Tasks_list()
    
    global data_format
    print _('''
           Пожалуйста, укажите формат, в котором записаны даты во входном файле 
           (например, в поле "Queue Date").
           Возможные символы:
           %a - сокращённое название дня недели (Mon, Tue, ...)
           %d - номер дня в месяце (01, 4, 31, ...)
           %b - сокращённое название месяца (Mar, Jan, Dec, ...)
           %Y - номер года полностью (2014, 2012, ...)
           %X - запись времени час:минута:секунда (13:45:29, 01:11:59, ...)

           Например, если время записано в таком виде: "Thu 07 Mar 2013 13:45:29 MSK",
           то нужно ввести "%a %d %b %Y %X" (без кавычек). MSK будет игнорироваться 
           вне зависимости от формата.

           Введите формат для даты:''')
    data_format = raw_input()
    print data_format

    pattern = '=' * 18 + ' .* ' + '=' * 18
    data = open(args.input, 'r').read().strip()

    for job in re.split(pattern, data):
        if job == '':
            continue
        task_record = Task_record()

        task_record.partition = 'pseudo_cluster_default'
        task_record.priority = 0
        task_record.other = dict()

        user_id = None
        group_id = None

        internal_user_id = None
        internal_group_id = None

        for line in job.split('\n'):
            # Each line has the form "Property: value"
            if ':' not in line:
                continue

            line = line.strip()
            # Usually, there are two parts, but if there is time (e.g. "23:51:49") in value,
            # then there are more.
            parts = line.split(':')

            if line.startswith('Job Id'):
                # Example: "Job Id: fen1.bg.cmc.msu.ru.186742"
                task_record.job_id = parts[1]
            elif line.startswith('Job Name'):
                # Example: "Job Name: fen1.bg.cmc.msu.ru.186742"
                task_record.job_name = parts[1]
            elif line.startswith('Queue Date'):
                # Example: "Queue Date: Thu 07 Mar 2013 13:44:22 MSK"
                tm = convert_time(line[line.find(':') + 1:])
                task_record.time_submit = datetime.datetime.fromtimestamp(tm)
            elif line.startswith('Start Time'):
                # Example: "Start Time: Thu 07 Mar 2013 13:44:23 MSK"
                tm = convert_time(line[line.find(':') + 1:])
                task_record.time_start = datetime.datetime.fromtimestamp(tm)
            elif line.startswith('Completion Date'):
                # Example: "Completion Date: Thu 07 Mar 2013 13:45:29 MSK"
                tm = convert_time(line[line.find(':') + 1:])
                task_record.time_end = datetime.datetime.fromtimestamp(tm)
            elif line.startswith('Submitting Userid'):
                # Example: "Submitting Userid: 3656"
                user_id = int(parts[1])
            elif line.startswith('Submitting Groupid'):
                # Example: "Submitting Groupid: 3336"
                group_id = int(parts[1])
            elif line.startswith('Wall Clk Soft Limit'):
                # Example: "Wall Clk Soft Limit: 00:15:00 (900 seconds)"
                bracket_idx = line.find('(')
                seconds = line[bracket_idx + 1:line.find(' ', bracket_idx)]

                task_record.time_limit = int(seconds) / 60
            elif line.startswith('Size Requested'):
                # Example: "Size Requested: 32"
                task_record.required_cpus = int(parts[1])
            elif line.startswith('Class'):
                # Example: "Class: n32_m15"
                task_record.task_class = parts[1]
            elif line.startswith('Status'):
                # Example: "Status: Completed"
                task_record.task_state = parts[1]

        if task_record.time_start == None:
            continue

        if args.extract_real_names == "Yes":
            try:
                user_touple=pwd.getpwuid(user_id)
                user_name=user_touple[0]
            except KeyError, e:
                internal_user_id=tasks_list.get_internal_user_id(user_id)
                user_name=str(user_id)

            try:
                group_touple=grp.getgrgid(group_id)
                group_name=group_touple[0]
            except KeyError, e:
                internal_group_id=tasks_list.get_internal_group_id(group_id)
                group_name=str(group_id)
        else:
            user_name=row[5]
            group_name=row[6]
                

        if args.masquerade_users == "Yes":
            internal_user_id  = tasks_list.get_internal_user_id(user_name)
            internal_group_id = tasks_list.get_internal_group_id(group_name)
            task_record.user_name  = tasks_list.get_user_name_by_id(internal_user_id)
            task_record.group_name = tasks_list.get_group_name_by_id(internal_group_id)
            tasks_list.register_user_in_group(internal_user_id, internal_group_id )           
        else:
            if internal_user_id:
                task_record.user_name  = tasks_list.get_user_name_by_id(internal_user_id)
                if internal_group_id:
                    tasks_list.register_user_in_group(internal_user_id, internal_group_id)
                else:
                    tasks_list.register_user_in_group(
                            internal_user_id,
                            group_name,
                            internal_group=False
                    )
            else:
                task_record.user_name = user_name

            if internal_group_id:
                task_record.group_name = tasks_list.get_group_name_by_id(internal_group_id)
                if internal_user_id:
                    tasks_list.register_user_in_group(internal_user_id, internal_group_id)
                else:
                    tasks_list.register_user_in_group(
                            user_name,
                            internal_group_id,
                            internal_user=False
                    )
            else:
                task_record.group_name = group_name
        
        tasks_list.add_task_record(task_record)

    tasks_list.print_to_files(args.prefix)

    return 0

if __name__ == "__main__":
    sys.exit(main())

