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


