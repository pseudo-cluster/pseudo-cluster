# -*- coding: utf-8 -*-

import os
import pwd
import grp

import extended_task

class Scheduled_action(object):
    """
    Одиночное действие, которое нужно произвести с 
    системой ведения очередей.
    """
    def __init__(self,extended_task_record,action):
        self.extended_task_record = extended_task_record
        self.action               = action

    def __str__(self):
        s="class Scheduled_action(object):"
        s+="action='%s', " % self.action
        s+="extended_task_record='%s'" % str(self.extended_task_record)
        return s

    def submit_task(self,time_compression):
        """
        Ставит задачу в очередь c учётом коэффициента компрессии времени
        """
        time_limit=self.extended_task_record.time_limit
        if time_limit >0:
            time_limit=round(time_limit/float(time_compression))
            if time_limit == 0:
                time_limit=1
        
        duration=self.extended_task_record.time_end-self.extended_task_record.time_start
        
        s=self.extended_task_record.get_submit_string(time_limit,duration.total_seconds())
        #
        # Порождение процесса. 
        #
        pipe=os.pipe()
        pid=os.fork()
        if pid == 0:
            os.close(pipe[0])
            os.dup2(pipe[1],1)
            os.close(pipe[1])
            user_touple=pwd.getpwnam(self.extended_task_record.user_name)
            uid=user_touple[2]
            os.setuid(uid)
            os.seteuid(uid)
            group_touple=grp.getgrnam(self.extended_task_record.group_name)
            gid=group_touple[2]
            os.setgid(gid)
            os.setegid(gid)
            os.execve(s[0],s)
        #
        # father
        #
        os.close(pipe[1])
        f=os.fdopen(pipe[0],"r")
        line=f.readline()
        if not self.extended_task_record.parse_task_id(f,line):
            while True:
                print "\t\tTASK '%s': %s" % ( self.extended_task_record.job_name, line )
                try:
                    line=f.readline()
                except:
                    break
        f.close()

        status=os.wait(pid)
        if os.WIFEXITED(status) and (os.WEXITSTATUS(status) == 0):
            pass
        else:
            print "Submitting for task ID '%s' failed"\
                    % self.extended_task_record.job_name
        
        #print s

        #TODO
        #
        # Возможно оно должно возвращать True или False
        #

    def cancel_task(self):
        """
         Принудительно завершает задачу
        """
        s=self.extended_task_record.get_cancel_string()
        pipe=os.pipe()
        pid=os.fork()
        if pid == 0:
            os.close(pipe[0])
            os.dup2(pipe[1],1)
            os.close(pipe[1])
            user_touple=pwd.getpwnam(self.extended_task_record.user_name)
            uid=user_touple[2]
            os.setuid(uid)
            os.seteuid(uid)
            group_touple=grp.getgrnam(self.extended_task_record.group_name)
            gid=group_touple[2]
            os.setgid(gid)
            os.setegid(gid)
            os.execve(s[0],s)
        #
        # father
        #
        os.close(pipe[1])
        f=os.fdopen(pipe[0],"r")
        for line in f:
            print "\t\tTASK '%s': %s" % ( self.extended_task_record.job_name, line )
        f.close()

        status=os.wait(pid)
        if os.WIFEXITED(status) and (os.WEXITSTATUS(status) == 0):
            pass
        else:
            print "Canceling task with ID '%s'  and actual id '%s' failed"\
                    % ( self.extended_task_record.job_name,
                        self.extended_task_record.actual_task_id
                      )

        #print s
    

class Action_list(object):
    """
    Класс для хранения списка действий, которые нужно произвести 
    на один сеанс взаимодействия с системой ведения очередей.
    """
    def __init__(self):
        self.actions_list=list()

    def register_action(self,extended_task_record,action):
        """
         Регистрирует новое действие
        """ 
        action=Scheduled_action(extended_task_record,action)
        self.actions_list.append(action)

    def do_actions(self,time_compression):
        """
            Производит все действия, которые есть в списке
        """
        for action in self.actions_list:
            if   action.action == "submit":
                    action.submit_task(time_compression)
            elif action.action == "cancel":
                    action.cancel_task()
            #print action
        self.actions_list=list()

