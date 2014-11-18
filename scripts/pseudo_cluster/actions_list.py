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

    def submit_task(self,time_compression):
        """
        Ставит задачу в очередь c учётом коэффициента компрессии времени
        """
        time_limit=self.extended_task_record.time_limit
        if time_limit >0:
            time_limit=round(time_limit/float(time_compression))
            if time_limit == 0:
                time_limit=1
        
        duration=extended_task_record.time_end-extended_task_record.time_start
        
        s=self.extended_task_record.get_submit_string(time_limit,duration.total_seconds())
        #TODO
        #
        # Здесь должен быть fork(), setuid(), setgit()
        # exec()
        #
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
        self.submit_string=None
        self.cancel_string=None
        self.actions_list=[]

    def register_action(self,extended_task_record,action):
        """
         Регистрирует новое действие
        """ 
        action=Scheduled_action(extended_task_record,action)
        actions_list.append(action)

    def do_actions(self,time_compression):
        """
            Производит все действия, которые есть в списке
        """
        for action in actions_list:
            if   action.action == "submit":
                    action.submit_task(time_compression)
            elif action.action == "cancel":
                    action.cancel_task()
