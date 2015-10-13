"""
basetask
"""
from __future__ import absolute_import, division, print_function

import inspect
import contextlib
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


def wrapclass(decorator):
  def innerclass(cls):
    for name, method in inspect.getmembers(cls, inspect.ismethod):
      if name == 'run':
        setattr(cls, name, decorator(method))
    return cls
  return innerclass



class TaskError(Exception):
    """
    !Use to report errors for which a traceback is not useful.
    Examples of such errors:
    - processCcd is asked to run detection, but not calibration, and no calexp is found.
    - coadd finds no valid images in the specified patch.
    """
    pass


@wrapclass(wraprun)
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

    def run(self, *args, **kwargs):
        """
        Run method
        """

## Methods from task.py

    def emptyMetadata(self):
        """!Empty (clear) the metadata for this Task and all sub-Tasks."""
        for subtask in self._taskDict.itervalues():
            subtask.metadata = dafBase.PropertyList()

    def getSchemaCatalogs(self):
        """!Return the schemas generated by this task

        @warning Subclasses the use schemas must override this method. The default implemenation
        returns an empty dict.

        @return a dict of butler dataset type: empty catalog (an instance of the appropriate
            lsst.afw.table Catalog type) for this task

        This method may be called at any time after the Task is constructed, which means that
        all task schemas should be computed at construction time, __not__ when data is actually
        processed. This reflects the philosophy that the schema should not depend on the data.

        Returning catalogs rather than just schemas allows us to save e.g. slots for SourceCatalog as well.

        See also Task.getAllSchemaCatalogs
        """
        return {}

    def getAllSchemaCatalogs(self):
        """!Call getSchemaCatalogs() on all tasks in the hiearchy, combining the results into a single dict.

        @return a dict of butler dataset type: empty catalog (an instance of the appropriate
            lsst.afw.table Catalog type) for all tasks in the hierarchy, from the top-level task down
            through all subtasks

        This method may be called on any task in the hierarchy; it will return the same answer, regardless.

        The default implementation should always suffice. If your subtask uses schemas the override
        Task.getSchemaCatalogs, not this method.
        """
        schemaDict = self.getSchemaCatalogs()
        for subtask in self._taskDict.itervalues():
            schemaDict.update(subtask.getSchemaCatalogs())
        return schemaDict

    def getFullMetadata(self):
        """!Get metadata for all tasks

        The returned metadata includes timing information (if \@timer.timeMethod is used)
        and any metadata set by the task. The name of each item consists of the full task name
        with "." replaced by ":", followed by "." and the name of the item, e.g.:
            topLeveltTaskName:subtaskName:subsubtaskName.itemName
        using ":" in the full task name disambiguates the rare situation that a task has a subtask
        and a metadata item with the same name.

        @return metadata: an lsst.daf.base.PropertySet containing full task name: metadata
            for the top-level task and all subtasks, sub-subtasks, etc.
        """
        fullMetadata = dafBase.PropertySet()
        for fullName, task in self.getTaskDict().iteritems():
            fullMetadata.set(fullName.replace(".", ":"), task.metadata)
        return fullMetadata

    def getFullName(self):
        """!Return the task name as a hierarchical name including parent task names

        The full name consists of the name of the parent task and each subtask separated by periods.
        For example:
        - The full name of top-level task "top" is simply "top"
        - The full name of subtask "sub" of top-level task "top" is "top.sub"
        - The full name of subtask "sub2" of subtask "sub" of top-level task "top" is "top.sub.sub2".
        """
        return self._fullName

    def getName(self):
        """!Return the name of the task

        See getFullName to get a hierarchical name including parent task names
        """
        return self._name

    def getTaskDict(self):
        """!Return a dictionary of all tasks as a shallow copy.

        @return taskDict: a dict containing full task name: task object
            for the top-level task and all subtasks, sub-subtasks, etc.
        """
        return self._taskDict.copy()

    def makeSubtask(self, name, **keyArgs):
        """!Create a subtask as a new instance self.\<name>

        The subtask must be defined by self.config.\<name>, an instance of pex_config ConfigurableField.

        @param name         brief name of subtask
        @param **keyArgs    extra keyword arguments used to construct the task.
            The following arguments are automatically provided and cannot be overridden:
            "config" and "parentTask".
        """
        configurableField = getattr(self.config, name, None)
        if configurableField is None:
            raise KeyError("%s's config does not have field %r" % (self.getFullName, name))
        subtask = configurableField.apply(name=name, parentTask=self, **keyArgs)
        setattr(self, name, subtask)

    @contextlib.contextmanager
    def timer(self, name, logLevel = pexLog.Log.DEBUG):
        """!Context manager to log performance data for an arbitrary block of code

        @param[in] name         name of code being timed;
            data will be logged using item name: \<name>Start\<item> and \<name>End\<item>
        @param[in] logLevel     one of the lsst.pex.logging.Log level constants

        Example of use:
        \code
        with self.timer("someCodeToTime"):
            ...code to time...
        \endcode

        See timer.logInfo for the information logged
        """
        logInfo(obj = self, prefix = name + "Start", logLevel = logLevel)
        try:
            yield
        finally:
            logInfo(obj = self, prefix = name + "End",   logLevel = logLevel)



    @classmethod
    def makeField(cls, doc):
        """!Make an lsst.pex.config.ConfigurableField for this task

        Provides a convenient way to specify this task is a subtask of another task.
        Here is an example of use:
        \code
        class OtherTaskConfig(lsst.pex.config.Config)
            aSubtask = ATaskClass.makeField("a brief description of what this task does")
        \endcode

        @param[in] cls      this class
        @param[in] doc      help text for the field
        @return a lsst.pex.config.ConfigurableField for this task
        """
        return ConfigurableField(doc=doc, target=cls)

    def _computeFullName(self, name):
        """!Compute the full name of a subtask or metadata item, given its brief name

        For example: if the full name of this task is "top.sub.sub2"
        then _computeFullName("subname") returns "top.sub.sub2.subname".

        @param[in] name     brief name of subtask or metadata item
        @return the full name: the "name" argument prefixed by the full task name and a period.
        """
        return "%s.%s" % (self._fullName, name)
