from __future__ import absolute_import, division, print_function
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
#

import sys
import imp
import os
import inspect
import pkgutil
import pyclbr
import traceback
import functools
import contextlib
from lsst.pex.logging import getDefaultLog
from .argumentParser import ArgumentParser
import argparse as argp
import importlib


__all__ = ["CmdLineActivator"]

task_packages = {'lsst.pipe.base.examples':None}

for pkg in task_packages.keys():
   task_packages[pkg]=importlib.import_module(pkg)

class ActivatorParser(argp.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)

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

class ClassName(Exception):
    def __init__(self, msg, errs):
        super(ClassName, self).__init__(msg)
        self.errs = errs


class CmdLineActivator(object):
    def __init__(self, SuperTask, parsed_cmd, return_results=False):

        self.SuperTask = SuperTask
        self.return_results = bool(return_results)
        self.config = parsed_cmd.config
        self.log = parsed_cmd.log
        self.doRaise = bool(parsed_cmd.doraise)
        self.clobber_config = bool(parsed_cmd.clobberConfig)
        #self.do_backup = not bool(parsed_cmd.noBackupConfig)
        self.parsed_cmd = parsed_cmd
        self.num_processes = 1 #int(getattr(parsed_cmd, 'processes', 1))



    def make_task(self, parsedCmd=None, args=None):
        return self.SuperTask.__class__(config=self.config, log=self.log, activator='cmdLine')

    def precall(self):
        return True

    def execute(self):
        result_list=[]

        if self.precall():
            profile_name = self.parsed_cmd.profile if hasattr(self.parsed_cmd, "profile") else None
            log = self.parsed_cmd.log
            target_list = self.get_target_list(self.parsed_cmd)

            if len(target_list) > 0:
                with profile(profile_name, log):
                    # Run the task using self.__call__
                    #result_list = map(self, target_list)
                    print(len(target_list))
                    for target in target_list:
                        data_ref , kwargs = target
                        result = None
                        super_task = self.make_task(args=target)
                        result = super_task.run(data_ref, **kwargs)

            else:
                log.warn("Not running the task because there is no data to process; "
                    "you may preview data using \"--show data\"")


    def display_tree(self):
        if hasattr(self.SuperTask, 'print_tree'):
            self.SuperTask.print_tree()

    def generate_dot(self):
        if hasattr(self.SuperTask, 'write_tree'):
            self.SuperTask.write_tree()



    @staticmethod
    def get_target_list(parsed_cmd, **kwargs):
        return [(ref, kwargs) for ref in parsed_cmd.id.refList]



    @staticmethod
    def loadSuperTask(super_taskname):
        classTaskInstance = None
        classConfigInstance = None
        super_module = None


        for package_name, package in task_packages.iteritems():
            mod_names = []
            for _, modname, _ in pkgutil.iter_modules(package.__path__): mod_names.append(modname)

            for module in mod_names:
                classes_map = pyclbr.readmodule(module, path=package.__path__)
                mod_classes = [mk.upper() for mk in classes_map.keys() if classes_map[mk].module.upper() == module.upper()]
                if super_taskname.upper() in mod_classes:
                    super_module = module
                    break    # First instance

        if super_module:
            py_mod_task=__import__(package.__name__+'.'+super_module, fromlist=" ")
        else:
            print("\nSuper Task %s not found!\n" % super_taskname)
            return classTaskInstance, classConfigInstance

        print('\nClasses inside module %s : \n ' % (package.__name__+'.'+super_module))
        for name, obj in inspect.getmembers(py_mod_task):
            if inspect.isclass(obj):
                if obj.__module__ == py_mod_task.__name__:
                    print(super_module + '.' + obj.__name__)
                if obj.__name__.upper() == super_taskname.upper():
                    classTaskInstance = obj
                if obj.__name__.upper() == (super_taskname[:-4]+'Config').upper():
                    classConfigInstance = obj
        print()

        if classTaskInstance == None:
            raise ClassName(' no superTaskClass %s found: Task or similiar' % (super_taskname), None)

        if classConfigInstance == None:
            raise ClassName(' no superConfig Class %s found: Task or similiar' % (super_taskname.replace('Task','Config')), None)

        return classTaskInstance, classConfigInstance


    @staticmethod
    def get_tasks(modules_only=False):

        tasks_list =[]
        module_list = []
        for package_name, package in task_packages.iteritems():
            mod_names = []
            for _, modname, _ in pkgutil.iter_modules(package.__path__):
                mod_names.append(modname)


            for module in mod_names:
                task_module=package.__name__+'.'+module
                module_list.append(task_module)
                if not modules_only:
                    classes_map = pyclbr.readmodule(module, path=package.__path__)
                    mod_classes = [mk for mk in classes_map.keys() if classes_map[mk].module == module]
                    for m in mod_classes:
                        if m.upper().find('TASK') > -1 and m not in ['SuperParTask', 'SuperSeqTask', 'Task', 'SuperTask']:
                            tasks_list.append(task_module+'.'+m)
        if modules_only:
            return module_list
        else:
            return tasks_list



    @classmethod
    def parse_and_run(cls):
        parser_activator = ActivatorParser(description='CmdLine Activator')
        parser_activator.add_argument('taskname', nargs='?', type=str, help='name of the task')
        parser_activator.add_argument('-lt','--list_tasks', action="store_true", default=False, help='list tasks available')
        parser_activator.add_argument('-lm','--list_modules', action="store_true", default=False, help='list modules available')
        parser_activator.add_argument('--extras', action="store_true", default=False, help='Add extra parameters after it')


        try:
            idx=sys.argv.index('--extras')
            args1 = sys.argv[1:idx]
            args = parser_activator.parse_args(args1)
            args2 = sys.argv[idx+1:]
        except ValueError:
            args = parser_activator.parse_args()

        if args.list_modules :
            for i in cls.get_tasks(modules_only=True):
                print(i)
            sys.exit()

        if args.list_tasks :
            for i in cls.get_tasks():
                print(i)
            sys.exit()




        super_taskname = args.taskname
        SuperTaskClass, SuperTaskConfig = cls.loadSuperTask(super_taskname)
        SuperTask = SuperTaskClass(activator='cmdLine')
        argparse = ArgumentParser(name=SuperTask.name)
        argparse.add_id_argument(name="--id", datasetType="raw", help="data ID, e.g. --id visit=12345 ccd=1,2")
        parser = argparse.parse_args(config=SuperTask.ConfigClass(), args=args2)

        CmdLineClass = cls(SuperTask, parser)
        CmdLineClass.display_tree()
        CmdLineClass.generate_dot()
        CmdLineClass.execute()




