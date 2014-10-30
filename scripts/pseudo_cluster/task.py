# -*- coding: utf-8 -*-

class Task_record(object):
    """
    Класс для хранения атрибутов задач, полученных
    из файла со статистикой.
    """
    def __init__(self):
        self.job_id=None
        self.job_name=None
        self.time_submit=None
        self.time_start=None
        self.time_end=None
        self.user_name=None
        self.group_name=None
        self.time_limit=None
        self.required_cpus=None
        self.partition=None
        self.priority=None
        self.task_class=None
        self.task_state=None

        #
        # Cловарь с прочими атрибутами, ассоциированными с задачей на кластере.
        # ключ - имя атрибута, например имя поля в таблице [cluster_name]_job_table
        #
        self.other= {}

    def __str__(self):
        s="Class Task_record(object): "    
        s+="job_id='%s', "         % self.job_id
        s+="job_name='%s', "       % self.job_name
        s+="time_submit='%s', "    % self.time_submit
        s+="time_start='%s',  "    % self.time_start
        s+="time_end='%s', "       % self.time_end
        s+="user_name='%s', "      % self.user_name
        s+="group_name='%s', "     % self.group_name
        s+="time_limit=%d, "       % self.time_limit
        s+="required_cpus=%d, "    % self.required_cpus
        s+="partition='%s', "      % self.partition
        s+="priority=%d, "         % self.priority
        s+="task_class='%s', "     % self.task_class
        s+="task_state='%s', "     % self.task_state
        s+="other= "+str(self.other)
        return s

