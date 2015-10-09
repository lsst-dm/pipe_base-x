"""
basetask
"""
from __future__ import absolute_import, division, print_function

import lsstDebug
import lsst.pex.logging as pexLog
import lsst.daf.base as dafBase

__all__ = ["Task", "TaskError"]


def wraprun(func):
    """
    Wrapper around run for pre and post run process
    :param pre:
    :param post:
    :return:
    """
    def inner(instance,*args, **kwargs):
        instance.pre_run(*args, **kwargs)
        temp = func(instance,*args, **kwargs)
        instance.post_run(*args, **kwargs)
        return temp
    return inner



class TaskError(Exception):
    """
    !Use to report errors for which a traceback is not useful.
    Examples of such errors:
    - processCcd is asked to run detection, but not calibration, and no calexp is found.
    - coadd finds no valid images in the specified patch.
    """
    pass


class Task(object):
    """
    Task Class
    """
    _default_name = None
    _parent_name = None

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        """
        !Create a Task
        @param[in] config       configuration for this task (an instance of self.ConfigClass,
            which is a task-specific subclass of lsst.pex.config.Config), or None. If None:
            - If parentTask specified then defaults to parentTask.config.name
            - If parentTask is None then defaults to self.ConfigClass()
        @param[in] name   brief name of task, or None; if None then defaults to self._DefaultName
        @param[in] parentTask   the parent task of this subtask, if any.
            - If None (a top-level task) then you must specify config and name is ignored.
            - If not None (a subtask) then you must specify name
        @param[in] log          pexLog log; if None then the default is used;
            in either case a copy is made using the full task name
        @throw RuntimeError if parentTask is None and config is None.
        @throw RuntimeError if parentTask is not None and name is None.
        @throw RuntimeError if name is None and _DefaultName does not exist.
        """
        self.metadata = dafBase.PropertyList()
        self._completed = False
        self._task_kind = 'Task'
        self._activator = activator

        if parent_task is not None:
            if name is None:
                raise RuntimeError("name is required for a subtask")
            self._name = name
        else:
            if name is None:
                name = getattr(self, "_default_name", None)
                if name is None:
                    pass
                    # raise RuntimeError("name is required for a task unless
                    # it has attribute _DefaultName")
                try:
                    name = self._default_name
                except AttributeError:
                    name = self.get_task_name()
            self._name = name
            self._fullname = self._name
            if config is None:
                config = self.ConfigClass()
            #self._taskdict = dict()

        self.config = config
        if log is None:
            log = pexLog.getDefaultLog()
        self.log = pexLog.Log(log, self._fullname)
        self._display = lsstDebug.Info(self.__module__).display
        # self._taskdict[self._fullname] = self
        print('%s was initiated' % self.name)


    @property
    def name(self):
        """
        Return name of task
        :return:
        """
        return self._name

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
            print(self.__module__, self.__class__.__bases__, self.__class__.__name__)
            print()
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

    @wraprun
    def run(self, *args, **kwargs):
        """
        Run method
        """


