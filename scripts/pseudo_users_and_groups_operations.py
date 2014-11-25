#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import os
import sys

def main(argv=None):
    """
        То, с чего начинается программа
    """
    if argv == None:
        argv=sys.argv

    parser = argparse.ArgumentParser(
            description=\
               """
               Данный скрипт создаёт пользователей и группы, 
               а так же добавляет пользователей в группы. При этом
               пользователи и группы берутся из специальных файлов,
               которые предоставляется утилитами разбора статистики.
               Например утилитой parse_slurm_db.py
               """,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            epilog="Например можно запустить так:\n "+argv[0]+" --prefix /tmp/cluster_name_"
            )

    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=True,
            default="./",
            help="префикс, по которому находятся файлы с отображениями пользователей"
    )

    parser.add_argument(
            '--homes-prefix',
            dest='homes_prefix',
            required=False,
            default="/home/pseudo_cluster_users",
            help="префикс, по которому находятся каталоги пользователей псевдокластера"
    )


    args=parser.parse_args()

    
    user_group_map=dict()

    file_descr=open(args.prefix+"user_in_groups_map","r")
    for  line in file_descr:
        tupl=line.split(':')
        user=tupl[0].strip()
        groups_line=tupl[1].strip()
        groups_tupl=groups_line.split(',')
        groups=list()
        for group in groups_tupl:
            groups.append(group.strip(' \t\n\r'))
        user_group_map[user]=groups
    file_descr.close()

    file_descr=open(args.prefix+"groups_map","r")
    for line in file_descr:
        tupl=line.split(':')
        group=tupl[0].strip()

        command_line  = "groupadd --force  '%s'" % group
        #os.system()
        print command_line
    file_descr.close()
        

    file_descr=open(args.prefix+"users_map","r")
    for line in file_descr:
        tupl=line.split(':')
        user=tupl[0].strip()

        command_line="useradd "
        command_line+="--create-home --home \'%s/%s\' " % (args.homes_prefix, user)
        command_line+="--gid '%s' " % user_group_map[user][0]
        groups_line=""
        for i in xrange (1, len(user_group_map[user])):
            groups_line+="%s," % user_group_map[user][i]
        groups_line=groups_line.strip("\t\r '")
        if groups_line != "":
            command_line+="--groups '%s'"
        command_line+=" %s" % user
        #os.system()
        print command_line
    file_descr.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())

