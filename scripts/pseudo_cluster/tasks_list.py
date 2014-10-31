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
         # Включённость пользователя в группу
         # словарь множеств.
         #
         self.user_groups_relations={}
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

    def register_user_in_group(self,user_id,group_id):
        """
            Добавляет ассоциацию пользователь группа,
            если такой ассоциации до этого небыло
        """
        groups_set=self.user_groups_relations.get(user_id)
        if groups_set == None:
            groups_set=set()
        if group_id not in groups_set:
            groups_set.add(group_id)
        #TODO
        #
        # Возможно в этом месте оно будет комировать
        # множества, вместо того, чтобы 
        # если множестсва одни и те же, оставить как есть. 
        #
        self.user_groups_relations[user_id]= groups_set

    def add_task_record(self,record):
        """
            Добавляет запись о задаче в список записей
        """
        #TODO Вставить сюда проверку соответствия типов
        #
        self.tasks_list.append(record)

    def print_to_files(self,file_system_prefix):
        """
         Печатает всё в файловую систему
        """
        f=open(file_system_prefix+"statistics.csv","w")
        f.write(task.get_header_string())
        for tsk in self.tasks_list:
            tsk.print_record_to_file(f)
        f.close()


    

