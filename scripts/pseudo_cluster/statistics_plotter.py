# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt

class Plotter(object):
    """
    Класс для отрисовки графиков
    """

    def __init__(self,title,data,labels):
        self.title=title
        self.data=data
        self.labels=labels

    def draw(self,file_name):
        """
        Отрисовывает в файл, или 
        в окно, если имя файла не задано.
        """
        plt.title(self.title)
        plt.xlabel(self.labels[0])
        plt.ylabel(self.labels[1])

        plt.plot(self.data)
        plt.show()

