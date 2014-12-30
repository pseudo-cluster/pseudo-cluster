# -*- coding: utf-8 -*-

import datetime

metric_short_description=\
        _("вычисляет среднее время ожидания в очереди.")

metric_description=\
_("""
Среднее время ожидания в очереди для задач пользователя. 
Выражает степень недовольства пользователей вычислительным 
кластером, поскольку никому не хочется находиться в очереди долго.

требует параметров:
    count_mode - возможные значения: (user, day, total)
""")

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
            for item in parameters.split(','):
                pair=item.strip().split('=')
                self.parameters[pair[0]]=pair[1].strip("'\"")

        if "count_mode" not in self.parameters.keys():
            self.parameters["count_mode"]="user"

    def __str__(self):
        s="package %s: Metric_counter: " % __name__
        s+="tasks_list=%s, " % str(self.tasks_list)
        s+="parameters=%s " % str(self.parameters)
        return s

    def get_metric_name(self):
        names=__name__.split('.')
        return names[1]

    def count_values(self,compression):
        """
        Подсчитать и выдать число,
        словарь значений, и т.п.
        """
        mes=_("\n\n\trun metric %s:") % self.get_metric_name() 
        mes+=_("\tmetric parameters is: %s\n\n") % self.parameters
        print mes

        mode=self.parameters['count_mode']
        tmp_result=dict()

        if mode == "user":
            for task in self.tasks_list:
                if task.user_name not in tmp_result.keys():
                    tmp_result[task.user_name]=(datetime.timedelta(minutes=0),0)
                if task.time_start > task.time_submit:
                    waitings, ones = tmp_result[task.user_name]
                    tmp_result[task.user_name]=( waitings + (task.time_start - task.time_submit) , ones + 1 )
        
        
        if mode == "day":
            for task in self.tasks_list:
                date=task.time_submit.date()
                if date not in tmp_result.keys():
                    tmp_result[date]=(datetime.timedelta(minutes=0),0)
                if task.time_start > task.time_submit:
                    waitings, ones = tmp_result[date]
                    tmp_result[date]=( waitings + (task.time_start - task.time_submit) , ones + 1 )

        if mode == "total":
            tmp_result["total"]=(datetime.timedelta(minutes=0),0)
            for task in self.tasks_list:
                date=task.time_submit.date()
                if task.time_start > task.time_submit:
                    waitings, ones = tmp_result["total"]
                    tmp_result["total"]=( waitings + (task.time_start - task.time_submit) , ones + 1 )

        result=dict()
        for key, record in tmp_result.items():
            if record[1]:
                ave_duration = ( record[0] * compression ) / record[1]
                result[key]=ave_duration.total_seconds() / 60
            else:
                result[key] = 0
        

        return result

    def get_header_string(self):
        """
        Выдаём строку заголовок для печати всего в 
        .csv файл
        """
        mode=self.parameters['count_mode']
        if mode == "user":
            return "\"%s\"\t\"%s\"" % (_("Users"), _("Duration (minutes)"))
        if mode == "day":
            return "\"%s\"\t\"%s\"" % (_("Date (YYYY-MM-DD)"), _("Duration (minutes)"))
        if mode == "total":
            return "\"%s\"\t\"%s\"" % (_("Totally"), _("Duration (minutes)"))

        return None

    def format_row(self,key,values_row):
        """
        Форматирует запись к виду пригодному для печати
        в .csv формате.
        """
        mode=self.parameters['count_mode']
        if mode == "user":
            return "\"%s\"\t%d" % (key, values_row)
        if mode == "day":
            return "\"%s\"\t%d" % (key.strftime("%Y-%m-%d"), values_row)
        if mode == "total":
            return "\"%s\"\t%d" % (key, values_row)

        return None

    
