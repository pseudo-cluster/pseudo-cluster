# -*- coding: utf-8 -*-

import importlib

import tasks_list



class Statistics_analyzer(object):
    """
    Класс для анализа статистики и её отображения
    как в виде текстовых файлов,
    так и в виде графика. 
    Поддерживает расчёт по некоторому набору метрик.
    """
    def __init__(self):
        """
        Конструктор
        """
        
        #
        #список задач
        #на котором будет 
        #вычисляться метрика
        #
        self.tasks_list = None
        
        #
        # Коэффициент сжатия времени
        #
        time_compression = 1.0
        
        #
        # Имя файла, куда сохраняется
        # результат вычисления метрик.
        #
        file_name= None

    def get_metric_description(self,metric_name):
        """
        Печатает описание метрики
        """
        module=importlib.import_module("metrics."+metric_name)
        return module.metric_description

    
   

    
