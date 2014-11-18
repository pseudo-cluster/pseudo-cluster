# -*- coding: utf-8 -*-

import task

class Extended_task_record(task.Task_record):
    """
    Класс для хранения связи между задачей в статистике и 
    задачей, которая ставится в псевдокластер.
    """
    def init(self,task_record):
        """
        Делает новый объект по существующей задаче
        """
        self.actual_task_id= None
        #TODO 
        #
        # дописать копирование задачи
        #
        self.job_id=task_record.job_id
        self.job_name=task_record.job_name
        self.time_submit=task_record.time_submit
        self.time_start=task_record.time_start
        self.time_end=task_record.time_end
        self.user_name=task_record.user_name
        self.group_name=task_record.group_name
        self.time_limit=task_record.time_limit
        self.required_cpus=task_record.required_cpus
        self.partition=task_record.partition
        self.priority=task_record.priority
        self.task_class=task_record.task_class
        self.task_state=task_record.task_state
        self.other=task_record.other.copy()
    
