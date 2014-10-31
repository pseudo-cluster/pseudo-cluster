#!/opt/python-2.7.6/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
import time
import pwd

import MySQLdb

from pseudo_cluster.task import  Task_record
from pseudo_cluster.tasks_list import  Tasks_list


def main(argv=None):
    """
    Главная функция программы
    """
    if argv == None:
        argv=sys.argv
    
    parser= argparse.ArgumentParser(
            description="""
            Данная программа делает выборку за некоторый период времени 
            из статистики запуска задач на вычислительном кластере 
            управляемым системой ведения очередей Slurm. 
            Результат помещается в несколько текстовых файлов.
            """,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            epilog="Например можно запустить так:\n \""+sys.argv[0]
    )
    
    parser.add_argument(
           '--from',
           dest='time_from',
           required=True,
           help="Дата и время с которых выбирать статистику: формат YYYY-MM-DD HH:MM "
    )

    parser.add_argument(
            '--to',
            dest='time_to',
            required=True,
            help="Дата и время до кoторых выбирать статистику: формат YYYY-MM-DD HH:MM"
    )

    parser.add_argument(
            '--cluster',
            dest='cluster',
            required=True,
            help="Имя кластера в базе данных slurm"
    )

    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=False,
            default="./",
            help="префикс, по которому сохранять выборку"
    )

    parser.add_argument(
            '--db-passwd-file',
            dest='db_passwd_file',
            required=False,
            default="db_passwd",
            help="""
                    Путь до файла с логином и паролём пользователя, 
                    который имеет право просматривать базу данных slurm. 
                    Формат login:password
                 """
    )

    parser.add_argument(
            '--db-host',
            dest='db_host_and_port',
            required=False,
            default="localhost",
            help="""
                    Имя хоста с базой данных slurm и номер порта.
                    Если порт не указан, подставляется по умолчанию
                    Формат host:port
                 """
    )
   
    parser.add_argument(
            '--masquerade-users',
            dest='masquerade_users',
            required=False,
            default="Yes",
            help="""
                    Если включено, все пользователи будут маскироваться 
                    под именами типа 'user123'
                 """
    )
    
    args=parser.parse_args()
   
    
    time_from = time.mktime(time.strptime(args.time_from,"%Y-%m-%d %H:%M"))
    time_to   = time.mktime(time.strptime(args.time_to,"%Y-%m-%d %H:%M"))
    
    db_passwd_file=open(args.db_passwd_file,"r")
    pair=db_passwd_file.readline().split(':')
    db_login=pair[0].strip()
    db_password=pair[1].strip()
  
    pair=args.db_host_and_port.split(':')
    db_host=pair[0].strip()
    if len(pair) > 1:
        db_port=pair[1].strip()
    else:
         db_port="3306"
    
    if (db_host != "localhost") and (db_port == "") :
        db_port="3306"

    db=MySQLdb.connect(
            host=db_host,
            user=db_login,
            passwd=db_password,
            port=int(db_port),
            db="slurm_acct_db"
            )

    cursor=db.cursor()

    query=\
    """
        select
            id_job,
            job_name,
            time_submit,
            time_start,
            time_end,
            id_user,
            id_group,
            timelimit,
            cpus_req,
            partition,
            priority,
            account,
            state,
            mem_req
        from
            %s_job_table
        where
            ( time_submit >= %d ) and (time_submit <= %d )
      """ % (args.cluster, time_from, time_to)

    cursor.execute(query)
    #    print (cursor.fetchall())
    
    tasks_list=Tasks_list()
    
    #
    # Эти состояния именно так упорядоченны
    # в slurm.
    #
    slurm_task_states=\
    [ "pending",
      "running",
      "suspended",
      "completed",
      "canceled",
      "failed",
      "time_left",
      "node_fail" 
    ]
   
    for row in cursor.fetchall():
        task_record=Task_record()
        
        task_record.job_id   = row[0]
        
        task_record.job_name = row[1]
        
        datetime_obj=datetime.datetime.fromtimestamp(int(row[2]))
        task_record.time_submit = datetime_obj.strftime("%Y-%m-%d %H:%M")
        
        datetime_obj=datetime.datetime.fromtimestamp(int(row[3]))
        task_record.time_start = datetime_obj.strftime("%Y-%m-%d %H:%M")
        
        datetime_obj=datetime.datetime.fromtimestamp(int(row[4]))
        task_record.time_end = datetime_obj.strftime("%Y-%m-%d %H:%M")

        if args.masquerade_users == "Yes":
            user_id  = tasks_list.get_internal_user_id(row[5])
            group_id = tasks_list.get_internal_group_id(row[6])
            task_record.user_name = "pseudo_cluster_user_%d"   % user_id
            task_record.group_name = "pseudo_cluster_group_%d" % group_id
            tasks_list.register_user_in_group(user_id, group_id )
        else:
            return 1

        task_record.time_limit=int(row[7])
        
        task_record.required_cpus=int(row[8])

        task_record.partition=row[9]

        #TODO
        # В текущей ситуации я не понимаю, как его грамотно о 
        # туда выковорить, так, чтобы оставить только ту часть,
        # которую указывал пользователь.
        #
        # В идеале здесь должно быть что-то вроде
        # task_record.priority=function(row[10])
        #
        task_record.priority=0
        
        if row[11]:
            task_record.task_class=row[11]
        else:
            task_record.task_class="pseudo_cluster_default"

        task_record.task_state=slurm_task_states[int(row[12])]

        task_record.other["memory_limit"]=int(row[13])
        tasks_list.add_task_record(task_record)

    tasks_list.print_to_files(args.prefix)

    return 0
            

if __name__ == "__main__":
    sys.exit(main())
