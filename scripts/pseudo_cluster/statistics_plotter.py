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
            plt.x_labels=map(str,self.data.keys())
            plt.add("data",self.data.values())
            #
            # May be in graduses
            #
            plt.x_label_rotation=90 

        if self.draw_type == 'plot':
            plt=pygal.Line()
            plt.x_labels=map(str,self.data.keys())
            plt.add("data",self.data.values())

        plt.show_legend=False
        plt.label_font_size=11
        plt.width=1024
        plt.height=768
        plt.title=unicode(self.title)
        plt.x_title=unicode(self.labels[0])
        plt.y_title=unicode(self.labels[1])
        plt.render_to_file(file_name)

