# -*- coding: utf-8 -*-

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
        #TODO
        #
        # Здесь должен быть fork(), setuid(), setgit()
        # exec()
        #
        pipe=os.pipe()
        pid=os.fork()
        if pid == 0:
            os.close(pipe[0])
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

        os.wait(pid)

        print s

    def cancel_task(self):
        """
         Принудительно завершает задачу
        """
        s=self.extended_task_record.get_cancel_string()
        print s
    

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

