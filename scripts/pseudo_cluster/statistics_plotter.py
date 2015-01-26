# -*- coding: utf-8 -*-

import pygal as pygal

class Plotter(object):
    """
    Класс для отрисовки графиков
    """

    def __init__(self,title,data,labels,draw_type):
        self.title=title
        self.data=data
        self.labels=labels
        self.draw_type=draw_type

    def draw(self,file_name):
        """
        Отрисовывает в файл, или 
        в окно, если имя файла не задано.
        """
        plt=None
        #plt.title(self.title)
        #plt.xlabel(self.labels[0])
        #plt.ylabel(self.labels[1])       


        if self.draw_type == 'chart':
            plt=pygal.Bar()
            #plt.x_labels(self.data.keys())
            plt.add(self.labels[1],self.data.values())            

        if self.draw_type == 'plot':
            plt=pygal.XY()
            data_array=list()
            for k,v  in self.data.items():
                data_array.append((float(k),float(v)))
            plt.add(self.labels[1], data_array )

        plt.title=self.title
        plt.render_to_file(file_name)

