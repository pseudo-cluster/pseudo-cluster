#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import gettext

def create_users_and_groups(args):
    if os.path.exists(args.homes_prefix):
        if not os.path.isdir(args.homes_prefix):
            print _("path '%s' is not a directory") % args.homes_prefix
            return 3
    else:
        try:
            print _("make path to homes: '%s'") % args.homes_prefix
            os.makedirs(args.homes_prefix,0755)
        except OSError, e:
            print e
            return 3

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
        
        print _("create group: '%s'") % group
        if os.system(command_line):
            return 1
        #print command_line
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
        command_line+=" '%s'" % user

        print _("create user: '%s'") % user
        if os.system(command_line):
            return 1
        #print command_line
    file_descr.close()

def delete_users_and_groups(args):
    file_descr=open(args.prefix+"users_map","r")
    for line in file_descr:
        tupl=line.split(':')
        user=tupl[0].strip()

        command_line="userdel '%s'" % user

        print _("delete user: '%s'") % user
        if os.system(command_line):
            print _("            warning for user '%s'") % user
        #print command_line
    file_descr.close()

    file_descr=open(args.prefix+"groups_map","r")
    for line in file_descr:
        tupl=line.split(':')
        group=tupl[0].strip()

        command_line  = "groupdel '%s'" % group

        print _("delete group: '%s'") % group
        if os.system(command_line):
            print _("             warning for group '%s'") % group
        #print command_line
    file_descr.close()

    return 0


def main(argv=None):
    """
        То, с чего начинается программа
    """
    if argv == None:
        argv=sys.argv

    gettext.install('pseudo-cluster')

    parser = argparse.ArgumentParser(
            description=\
               _("""
               Данный скрипт создаёт пользователей и группы, 
               а так же добавляет пользователей в группы. При этом
               пользователи и группы берутся из специальных файлов,
               которые предоставляется утилитами разбора статистики.
               Например утилитой parse_slurm_db.py
               """),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            epilog=_("Например можно запустить так:\n ")+argv[0]+" --prefix /tmp/cluster_name_"
            )

    parser.add_argument(
            '--prefix',
            dest='prefix',
            required=True,
            default="./",
            help=_("префикс, по которому находятся файлы с отображениями пользователей")
    )

    parser.add_argument(
            '--mode',
            dest='mode',
            required=False,
            choices=["create","delete"],
            default="create",
            help=\
                    _("""
                    определяет режим в котором всё работает:

                        create -- создаются пользователи и группы,
                                  а так же домашние каталоги 
                                  пользователей.

                        delete -- удаляются пользователи и группы,
                                  каталоги пользователей остаются неизменными.
                    """)
    )

    parser.add_argument(
            '--homes-prefix',
            dest='homes_prefix',
            required=False,
            default="/home/pseudo_cluster_users",
            help=_("префикс, по которому находятся каталоги пользователей псевдокластера")
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
    
    if args.mode == "create":
        return create_users_and_groups(args)

    if args.mode == "delete":
        return delete_users_and_groups(args)

   
   
    return 100


if __name__ == "__main__":
    sys.exit(main())

