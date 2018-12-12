# -*- coding: utf-8 -*-

import datetime

metric_short_description=\
        _("вычисляет среднее число запрашиваемых процессоров.")

metric_description=\
_("""
Среднее число запрашиваемых задачей процессоров. 
Ничего особо не выражает, нужно для задач машинного обучения.

требует параметров:
    count_mode - возможные значения: (user, group, class, day, total)
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
        return __name__

    def count_values(self,time_compression):
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
                    tmp_result[task.user_name]=(task.required_cpus,1)
                else:
                    cpus, ones = tmp_result[task.user_name]
                    tmp_result[task.user_name]=(cpus + task.required_cpus, ones + 1)

        if mode == "group":
            for task in self.tasks_list:
                if task.group_name not in tmp_result.keys():
                    tmp_result[task.group_name]=(task.required_cpus,1)
                else:
                    cpus, ones = tmp_result[task.group_name]
                    tmp_result[task.group_name]=(cpus + task.required_cpus, ones + 1)

        if mode == "class":
            for task in self.tasks_list:
                if task.task_class not in tmp_result.keys():
                    tmp_result[task.task_class]=(task.required_cpus,1)
                else:
                    cpus, ones = tmp_result[task.task_class]
                    tmp_result[task.task_class]=(cpus + task.required_cpus, ones + 1)
      
        if mode == "day":
            first_day=self.tasks_list[0].time_submit.date()
            for task in self.tasks_list:
                if self.parameters['plot_format'] == 'true':
                    date=(task.time_submit.date()-first_day).days
                else:
                    date=task.time_submit.date()

                if date not in tmp_result.keys():
                    tmp_result[date]=(task.required_cpus,1)
                else:
                    cpus, ones = tmp_result[date]
                    tmp_result[date]=(cpus + task.required_cpus , ones + 1)

        if mode == "total":
            tmp_result["total"]=(0, 0)
            cpus=0;
            for task in self.tasks_list:
                    cpus += task.required_cpus;
            tmp_result["total"]=(cpus , len(self.tasks_list))
        
        #TODO
        # Not optimal
        # better to return modified tmp_result
        #
        result=dict()
        for key, record in tmp_result.items():
                result[key] =  (record[0] / record[1], record[1])
        
        return result

    def get_header_string(self):
        """
        Выдаём строку заголовок для печати всего в 
        .csv файл
        """
        format_str = "\"%s\"\t\"%s\"\t\"%s\""
        mode=self.parameters['count_mode']
        if mode == "user":
            return format_str % (_("Users"), _("Average cpus"), _("Totally tasks"))
        if mode == "group":
            return format_str % (_("Groups"), _("Average cpus"),  _("Totally tasks"))
        if mode == "class":
            return format_str % (_("Classes"), _("Average cpus"), _("Totally tasks"))
        if mode == "day":
            if self.parameters['plot_format'] == 'true':
                return format_str % (_("Day number"), _("Average cpus"),  _("Totally tasks"))
            else:
                return format_str % (_("Date (YYYY-MM-DD)"), _("Average cpus"), _("Totally tasks"))
        if mode == "total":
            return format_str % (_("Totally"), _("Average cpus"), _("Totally tasks"))

        return None

    def get_draw_type(self):
        """
        Выдаёт:
            chart - если отображаемо как набор столбиков,
            plot - если кривая y=f(x)
        """
        mode=self.parameters['count_mode']
        if (mode == "user") or (mode == "group") or (mode == "class"):
            return "chart"
        if mode == "day":
            return "plot"

        return None


    def format_row(self,key,values_row):
        """
        Форматирует запись к виду пригодному для печати
        в .csv формате.
        """
        format_str="\"%s\"\t%f\t%d"
        mode=self.parameters['count_mode']
        if (mode == "user") or (mode == "group") or (mode == "class"):
            return format_str % (key, values_row[0], values_row[1])
        if mode == "day":
            if self.parameters['plot_format'] == 'true':
                return format_str % (key, values_row[0], values_row[1])
            else:
                return format_str % (key.strftime("%Y-%m-%d"), values_row[0], values_row[1])
        if mode == "total":
            return format_str % (key, values_row[0], values_row[1])

        return None

