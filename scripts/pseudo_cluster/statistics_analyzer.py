# -*- coding: utf-8 -*-

import imp
import sys


import tasks_list
import metrics



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

    def get_metrics_list(self):
        """
        Получает список доступных в 
        текущий момент метрик
        """
        metrics_list=list()
        for module_name in metrics.__all__:
            metrics_list.append(module_name.partition('_')[2])

    def get_metric_description(self,metric_name):
        """
        Получает описание метрики по её имени
        """
        if "metric_%s" % metric_name not in  metrics.__all__:
            print "Metric with name '%s' is not found" % metric_name
            sys.exit(3)

        try:
            print "file: %s" % "./metrics/%s.py" % metric_name
            #module=importlib("metrics.metric_"+metric_name)
            module=imp.load_source(
                    "metrics."+metric_name,
                    "%s/metric_%s.py" % (metrics.__path__[0], metric_name))
        except IOError, e:
            print e
            sys.exit(3)
        
        return module.metric_description

    
   

    
