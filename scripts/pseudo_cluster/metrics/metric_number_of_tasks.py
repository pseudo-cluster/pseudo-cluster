# -*- coding: utf-8 -*-

metric_short_description=\
        "вычисляет число задач"

metric_description=\
"""
Пока не знаю что сюда написать


требует параметров:
    count_mode - возможные значения: (user, day, total)
"""

class Metric_counter(object):
    """
        Класс задающий метрику
    """

    def __init__(self,tasks_list,parameters):
        """
        Собственно конструирование объекта
        """
        self.tasks_list=tasks_list
        self.parameters=dict()
        if parameters != "":
            for item in other_string.split(','):
                pair=item.strip().split('=')
                self.parameters[pair[0]]=pair[1].strip("'\"")

    def __str__(self):
        s="package %s: Metric_counter: " % __name__
        s+="tasks_list=%s, " % str(self.tasks_list)
        s+="parameters=%s " % str(self.parameters)

    def count_values(self,compression):
        """
        Подсчитать и выдать число,
        словарь значений, и т.п.
        """
        return None