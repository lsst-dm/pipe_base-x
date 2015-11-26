"""
basetask
"""
from __future__ import absolute_import, division, print_function
import networkx as nx
from .task import Task, TaskError
from lsst.pipe.base.struct import Struct


__all__ = ["SuperTask"]


class SuperTask(Task):
    """
    SuperTask Generic
    """

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None, input=None):
        super(SuperTask, self).__init__(config, name, parent_task, log, activator)

        self._parser = None
        self.input = Struct()
        self.output = None
        self.name = self.name.replace(" ","_")
        self._task_kind = 'SuperTask'


    @property
    def parser(self):
        return self._parser

    @parser.setter
    def parser(self, parser):
        self._parser = parser

    def execute(self, dataRef):
        return self.run()

    def gconf(self):
        self.list_config = []
        rootN = self.name+'.'
        for key,val in self.config.iteritems():
            self.list_config.append(rootN+'config.'+key+' = '+str(val))
        return self.list_config

    def print_config(self):
        """
        Print Config
        :return:
        """
        print()
        print('* Configuration * :')
        print()
        for branch in self.gconf():
            print(branch)
        print()


