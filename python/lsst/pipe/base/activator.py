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
from .argumentParser import ArgumentParser


__all__ = ["CmdLineActivator"]

class ClassName(Exception):
    def __init__(self, msg, errs):
        super(ClassName, self).__init__(msg)
        self.errs = errs


class CmdLineActivator(object):
    def __init__(self, SuperTaskClass):
        self.SuperTaskClass = SuperTaskClass

    def execute(self):
        self.SuperTaskClass.run()

    def display_tree(self):
        if hasattr(self.SuperTaskClass, 'print_tree'):
            self.SuperTaskClass.print_tree()

    def generate_dot(self):
        if hasattr(self.SuperTaskClass, 'write_tree'):
            self.SuperTaskClass.write_tree()

    @staticmethod
    def loadSuperTask(superfile):
        classTaskInstance = None
        classConfigInstance = None

        module, file_ext = os.path.splitext(os.path.split(superfile)[-1])

        root = module[:module.upper().find('TASK')]

        print(root)
        print()

        if file_ext.lower() == '.py':
            py_mod_task = imp.load_source(module, superfile)

        elif file_ext.lower() == '.pyc':
            py_mod_task = imp.load_compiled(module, superfile)

        print('Classes inside %s : \n' % superfile)
        for name, obj in inspect.getmembers(py_mod_task):
            if inspect.isclass(obj):
                print(module + '.' + obj.__name__)
                if obj.__name__.upper() == (root + 'task').upper():
                    classTaskInstance = obj
                if obj.__name__.upper() == (root + 'config').upper():
                    classConfigInstance = obj

        if classTaskInstance == None:
            raise ClassName(' no superTaskClass found: ' + root + 'Task or simliar', None)

        return classTaskInstance, classConfigInstance


    @classmethod
    def parse_and_run(cls):
        superfile = sys.argv[1]
        SuperTaskClass, SuperTaskConfig = cls.loadSuperTask(superfile)
        SuperTask = SuperTaskClass(activator='cmdLine')
        argparse = ArgumentParser(name=SuperTask.name)
        argparse.add_id_argument(name="--id", datasetType="raw", help="data ID, e.g. --id visit=12345 ccd=1,2")
        SuperTask.parser = argparse.parse_args(config=SuperTask.ConfigClass(), args=sys.argv[2:])

        CmdLineClass = cls(SuperTask)
        CmdLineClass.display_tree()
        CmdLineClass.generate_dot()
        CmdLineClass.execute()

        #SuperTask = SuperTaskClass(activator='cmdLine')

        #SuperTask.run()
        #print()
        #SuperTask.print_tree()
        #SuperTask.write_tree()
        #print(result)





