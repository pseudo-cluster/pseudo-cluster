# -*- coding: utf-8 -*-

import task

class Tasks_list(object):
    """
        Класс для хранения и вывода информации о 
        статистике прохождения задач через систему 
        ведения очередей кластера
    """
    def __init__(self):
         self.user_name_pattern="pseudo_cluster_user_"
         self.group_name_pattern="pseudo_cluster_group_"
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
        # Возможно в этом месте оно будет копировать
        # множества, вместо того, чтобы 
        # если множестсва одни и те же, оставить как есть. 
        #
        self.user_groups_relations[user_id]= groups_set

    def get_user_name_by_id(self,user_id):
        """
            получает имя пользователя в соответствии 
            с образцом по его идентификатору
        """
        return self.user_name_pattern+str(user_id)

    def get_group_name_by_id(self,group_id):
        """
            получает имя группы в соответствии с 
            образцом по её идентификатору
        """
        return self.group_name_pattern+str(group_id)


    def add_task_record(self,record):
        """
            Добавляет запись о задаче в список записей
        """
        #TODO Вставить сюда проверку соответствия типов
        #
        self.tasks_list.append(record)    

    def print_to_files(self, file_system_prefix):
        """
         Печатает всё в файловую систему
        """
        f=open(file_system_prefix+"statistics.csv","w")
        f.write(task.get_header_string())
        for tsk in self.tasks_list:
            tsk.print_record_to_file(f)
        f.close()
        
        f=open(file_system_prefix+"users_map","w")
        for k,v in self.users_map.items():
            f.write("%s:%s\n" % (self.get_user_name_by_id(v),k))
        f.close()

        f=open(file_system_prefix+"groups_map","w")
        for k,v in self.groups_map.items():
            f.write("%s:%s\n" % (self.get_group_name_by_id(v),k))
        f.close()

        f=open(file_system_prefix+"user_in_groups_map","w")
        for user_id,groups in self.user_groups_relations.items():
            s=self.get_user_name_by_id(user_id)+":"
            for group_id in groups:
                s+=self.get_group_name_by_id(group_id)+","
            f.write("%s\n" % s.strip(" ,"))
        f.close()


