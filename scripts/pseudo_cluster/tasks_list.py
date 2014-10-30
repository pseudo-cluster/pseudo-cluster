# -*- coding: utf-8 -*-

import task

class Tasks_list(object):
    """
        Класс для хранения и вывода информации о 
        статистике прохождения задач через систему 
        ведения очередей кластера
    """
    def __init__(self):
         #
         #   Множество отображений: 
         #   пользователь на кластере --> порядковый номер, 
         #   как он встречается  в статистике.
         #
         self.users_map={}
         #
         #   Множество отображений: 
         #   группа на кластере --> порядковый номер,
         #   как он встречается  в статистике.
         #
         self.groups_map={}
         #
         #  список задач.
         #
         self.tasks_list=[]

    def get_internal_user_id(self,user):
        """
          Возвращает уникальный идентификатор пользователя, 
          или если его ещё нет в словаре пользователей
          добавить его туда.
        """
        internal_id=self.users_map.get(user)
        if internal_id == None:
            internal_id=len(self.users_map)+1
            self.users_map[user]=internal_id
        
        return internal_id

    def get_internal_group_id(self,group):
        """
          Возвращает уникальный идентификатор группы, 
          или если его ещё нет в словаре групп
          добавить его туда.
        """
        internal_id=self.groups_map.get(group)
        if internal_id == None:
            internal_id=len(self.groups_map)+1
            self.groups_map[group]=internal_id
        
        return internal_id

    def add_task_record(self,record):
        """
            Добавляет запись о задаче в список записей
        """
        #TODO Вставить сюда проверку соответствия типов
        #
        self.tasks_list.append(record)

    

