from __future__ import absolute_import, division
#
# LSST Data Management System
# Copyright 2008-2013 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

import sys
import traceback
import functools
import contextlib
from lsst.pex.logging import getDefaultLog



__all__ = ["SuperTaskRunner"]

def _poolFunctionWrapper(function, arg):
    """Wrapper around function to catch exceptions that don't inherit from Exception

    Such exceptions aren't caught by multiprocessing, which causes the slave
    process to crash and you end up hitting the timeout.
    """
    try:
        return function(arg)
    except Exception:
        raise # No worries
    except:
        # Need to wrap the exception with something multiprocessing will recognise
        cls, exc, tb = sys.exc_info()
        log = getDefaultLog()
        log.warn("Unhandled exception %s (%s):\n%s" % (cls.__name__, exc, traceback.format_exc()))
        raise Exception("Unhandled exception: %s (%s)" % (cls.__name__, exc))

def _runPool(pool, timeout, function, iterable):
    """Wrapper around pool.map_async, to handle timeout

    This is required so as to trigger an immediate interrupt on the KeyboardInterrupt (Ctrl-C); see
    http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool

    Further wraps the function in _poolFunctionWrapper to catch exceptions
    that don't inherit from Exception.
    """
    return pool.map_async(functools.partial(_poolFunctionWrapper, function), iterable).get(timeout)

@contextlib.contextmanager
def profile(filename, log=None):
    """!Context manager for profiling with cProfile

    @param filename     filename to which to write profile (profiling disabled if None or empty)
    @param log          log object for logging the profile operations

    If profiling is enabled, the context manager returns the cProfile.Profile object (otherwise
    it returns None), which allows additional control over profiling.  You can obtain this using
    the "as" clause, e.g.:

        with profile(filename) as prof:
            runYourCodeHere()

    The output cumulative profile can be printed with a command-line like:

        python -c 'import pstats; pstats.Stats("<filename>").sort_stats("cumtime").print_stats(30)'
    """
    if not filename:
        # Nothing to do
        yield
        return
    from cProfile import Profile
    profile = Profile()
    if log is not None:
        log.info("Enabling cProfile profiling")
    profile.enable()
    yield profile
    profile.disable()
    profile.dump_stats(filename)
    if log is not None:
        log.info("cProfile stats written to %s" % filename)


class SuperTaskRunner(object):

    TIMEOUT = 9999 # Default timeout (sec) for multiprocessing
    def __init__(self, TaskClass, parsedCmd, doReturnResults=False):
        """!Construct a TaskRunner

        @warning Do not store parsedCmd, as this instance is pickled (if multiprocessing) and parsedCmd may
        contain non-picklable elements. It certainly contains more data than we need to send to each
        instance of the task.

        @param TaskClass    The class of the task to run
        @param parsedCmd    The parsed command-line arguments, as returned by the task's argument parser's
                            parse_args method.
        @param doReturnResults    Should run return the collected result from each invocation of the task?
            This is only intended for unit tests and similar use.
            It can easily exhaust memory (if the task returns enough data and you call it enough times)
            and it will fail when using multiprocessing if the returned data cannot be pickled.

        @throws ImportError if multiprocessing requested (and the task supports it)
        but the multiprocessing library cannot be imported.
        """
        self.TaskClass = TaskClass
        self.doReturnResults = bool(doReturnResults)
        self.config = parsedCmd.config
        self.log = parsedCmd.log
        self.doRaise = bool(parsedCmd.doraise)
        self.clobberConfig = bool(parsedCmd.clobberConfig)
        self.numProcesses = int(getattr(parsedCmd, 'processes', 1))

        self.timeout = getattr(parsedCmd, 'timeout', None)
        if self.timeout is None or self.timeout <= 0:
            self.timeout = self.TIMEOUT

        if self.numProcesses > 1:
            if not TaskClass.canMultiprocess:
                self.log.warn("This task does not support multiprocessing; using one process")
                self.numProcesses = 1

    def prepareForMultiProcessing(self):
        """!Prepare this instance for multiprocessing by removing optional non-picklable elements.

        This is only called if the task is run under multiprocessing.
        """
        self.log = None

    def run(self, parsedCmd):
        """!Run the task on all targets.

        The task is run under multiprocessing if numProcesses > 1; otherwise processing is serial.

        @return a list of results returned by TaskRunner.\_\_call\_\_, or an empty list if
        TaskRunner.\_\_call\_\_ is not called (e.g. if TaskRunner.precall returns `False`).
        See TaskRunner.\_\_call\_\_ for details.
        """
        resultList = []
        if self.numProcesses > 1:
            import multiprocessing
            self.prepareForMultiProcessing()
            pool = multiprocessing.Pool(processes=self.numProcesses, maxtasksperchild=1)
            mapFunc = functools.partial(_runPool, pool, self.timeout)
        else:
            pool = None
            mapFunc = map

        if self.precall(parsedCmd):
            profileName = parsedCmd.profile if hasattr(parsedCmd, "profile") else None
            log = parsedCmd.log
            targetList = self.getTargetList(parsedCmd)
            if len(targetList) > 0:
                with profile(profileName, log):
                    # Run the task using self.__call__
                    resultList = mapFunc(self, targetList)
            else:
                log.warn("Not running the task because there is no data to process; "
                    "you may preview data using \"--show data\"")

        if pool is not None:
            pool.close()
            pool.join()

        return resultList

    @staticmethod
    def getTargetList(parsedCmd, **kwargs):
        """!Return a list of (dataRef, kwargs) to be used as arguments for TaskRunner.\_\_call\_\_.

        @param parsedCmd    the parsed command object (an argparse.Namespace) returned by
            \ref argumentParser.ArgumentParser.parse_args "ArgumentParser.parse_args".
        @param **kwargs     any additional keyword arguments. In the default TaskRunner
            this is an empty dict, but having it simplifies overriding TaskRunner for tasks
            whose run method takes additional arguments (see case (1) below).

        The default implementation of TaskRunner.getTargetList and TaskRunner.\_\_call\_\_ works for any
        command-line task whose run method takes exactly one argument: a data reference.
        Otherwise you must provide a variant of TaskRunner that overrides TaskRunner.getTargetList
        and possibly TaskRunner.\_\_call\_\_. There are two cases:

        (1) If your command-line task has a `run` method that takes one data reference followed by additional
        arguments, then you need only override TaskRunner.getTargetList to return the additional arguments as
        an argument dict. To make this easier, your overridden version of getTargetList may call
        TaskRunner.getTargetList with the extra arguments as keyword arguments. For example,
        the following adds an argument dict containing a single key: "calExpList", whose value is the list
        of data IDs for the calexp ID argument:

        \code
        \@staticmethod
        def getTargetList(parsedCmd):
            return TaskRunner.getTargetList(parsedCmd, calExpList=parsedCmd.calexp.idList)
        \endcode

        It is equivalent to this slightly longer version:

        \code
        \@staticmethod
        def getTargetList(parsedCmd):
            argDict = dict(calExpList=parsedCmd.calexp.idList)
            return [(dataId, argDict) for dataId in parsedCmd.id.idList]
        \endcode

        (2) If your task does not meet condition (1) then you must override both TaskRunner.getTargetList
        and TaskRunner.\_\_call\_\_. You may do this however you see fit, so long as TaskRunner.getTargetList
        returns a list, each of whose elements is sent to TaskRunner.\_\_call\_\_, which runs your task.
        """
        return [(ref, kwargs) for ref in parsedCmd.id.refList]

    def makeTask(self, parsedCmd=None, args=None):
        """!Create a Task instance

        @param[in] parsedCmd    parsed command-line options (used for extra task args by some task runners)
        @param[in] args         args tuple passed to TaskRunner.\_\_call\_\_ (used for extra task arguments
            by some task runners)

        makeTask() can be called with either the 'parsedCmd' argument or 'args' argument set to None,
        but it must construct identical Task instances in either case.

        Subclasses may ignore this method entirely if they reimplement both TaskRunner.precall and
        TaskRunner.\_\_call\_\_
        """
        return self.TaskClass(config=self.config, log=self.log)

    def precall(self, parsedCmd):
        """!Hook for code that should run exactly once, before multiprocessing is invoked.

        Must return True if TaskRunner.\_\_call\_\_ should subsequently be called.

        @warning Implementations must take care to ensure that no unpicklable attributes are added to
        the TaskRunner itself, for compatibility with multiprocessing.

        The default implementation writes schemas and configs, or compares them to existing
        files on disk if present.
        """
        task = self.makeTask(parsedCmd=parsedCmd)
        if self.doRaise:
            task.writeConfig(parsedCmd.butler, clobber=self.clobberConfig)
            task.writeSchemas(parsedCmd.butler, clobber=self.clobberConfig)
        else:
            try:
                task.writeConfig(parsedCmd.butler, clobber=self.clobberConfig)
                task.writeSchemas(parsedCmd.butler, clobber=self.clobberConfig)
            except Exception, e:
                task.log.fatal("Failed in task initialization: %s" % e)
                if not isinstance(e, TaskError):
                    traceback.print_exc(file=sys.stderr)
                return False
        return True

    def __call__(self, args):
        """!Run the Task on a single target.

        This default implementation assumes that the 'args' is a tuple
        containing a data reference and a dict of keyword arguments.

        @warning if you override this method and wish to return something when
        doReturnResults is false, then it must be picklable to support
        multiprocessing and it should be small enough that pickling and
        unpickling do not add excessive overhead.

        @param args     Arguments for Task.run()

        @return:
        - None if doReturnResults false
        - A pipe_base Struct containing these fields if doReturnResults true:
            - dataRef: the provided data reference
            - metadata: task metadata after execution of run
            - result: result returned by task run, or None if the task fails
        """
        dataRef, kwargs = args
        task = self.makeTask(args=args)
        result = None # in case the task fails
        if self.doRaise:
            result = task.run(dataRef, **kwargs)
        else:
            try:
                result = task.run(dataRef, **kwargs)
            except Exception, e:
                task.log.fatal("Failed on dataId=%s: %s" % (dataRef.dataId, e))
                if not isinstance(e, TaskError):
                    traceback.print_exc(file=sys.stderr)
        task.writeMetadata(dataRef)

        if self.doReturnResults:
            return Struct(
                dataRef = dataRef,
                metadata = task.metadata,
                result = result,
            )