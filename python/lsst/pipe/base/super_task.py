"""
basetask
"""
from __future__ import absolute_import, division, print_function
import inspect
from .task import Task, TaskError
from lsst.pipe.base.struct import Struct


__all__ = ["SuperTask"]


def wraprun(func):
    """
    Wrapper around run for pre and post run process
    :param pre:
    :param post:
    :return:
    """
    def inner(instance,*args, **kwargs):
        instance.pre_run()
        temp = func(instance, *args, **kwargs)
        instance.post_run(*args, **kwargs)
        return temp
    return inner


def wrapclass(decorator):
  def innerclass(cls):
    for name, method in inspect.getmembers(cls, inspect.ismethod):
      if name == 'run':
        setattr(cls, name, decorator(method))
    return cls
  return innerclass


@wrapclass(wraprun)
class SuperTask(Task):
    """
    SuperTask Generic
    """

    _default_name = None
    _parent_name = None

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        if name is None:
            name = getattr(self, "_default_name", None)
        super(SuperTask, self).__init__(config, name, parent_task)

        self._parser = None
        self.input = Struct()
        self.output = None
        self._activator = activator
        self._completed = False

        self.name = self.name.replace(" ","_")
        self._task_kind = 'SuperTask'

        print('%s was initiated' % self.name)



    @property
    def name(self):
        """
        Return name of task
        :return:
        """
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def activator(self):
        """
        :return:
        """
        return self._activator

    @activator.setter
    def activator(self, activator):
        self._activator = activator

    def get_task_name(self):
        """Get class name for object if nothing is available"""
        try:
            return self.__class__.__name__
        except:
            raise RuntimeError("name is required for a task unless it has attribute _default_name")

    @property
    def task_kind(self):
        """
        Return Class type
        """
        return self._task_kind

    @task_kind.setter
    def task_kind(self, task_kind):
        self._task_kind = task_kind

    @property
    def parent_name(self):
        """
        Return name of parent
        """
        return self._parent_name

    @property
    def completed(self):
        """
        Return the status of task
        """
        return self._completed

    @completed.setter
    def completed(self, completed):
        self._completed = completed


    def pre_run(self, *args, **kwargs):
        """
        Prerun method
        """
        #print('Pre run!, activator %s' % self.activator)
        pass

    def post_run(self, *args, **kwargs):
        """
        Postrun method
        """
        #print('Done!')
        self.completed = True

    def run(self, *args, **kwargs):
        """
        Run method
        """


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


