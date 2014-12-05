# -*- coding: utf-8 -*-

import imp
import sys

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
        try:
            print "file: %s" % "./metrics/%s.py" % metric_name
            module=imp.load_source("metrics."+metric_name,"./metrics/%s.py" % metric_name)
        except IOError, e:
            print e
            sys.exit(3)
        
        return module.metric_description

    
   

    
