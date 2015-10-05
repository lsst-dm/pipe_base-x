from __future__ import absolute_import, division, print_function

from basetask import Task, TaskError
from basestruct import Struct
from superTaskRunner import SuperTaskRunner
from argumentParser import ArgumentParser
import lsst.pex.config as pexConfig
import six


class test1Config(pexConfig.Config):
    doPrint = pexConfig.Field(
        dtype=bool,
        doc="Display info",
        default=False,
    )


class test1Task(Task):
    ConfigClass = test1Config  # ConfigClass = pexConfig.Config

    def __init__(self, *args, **kwargs):
        super(test1Task, self).__init__(*args, **kwargs)  # # P3 would be super().__init__()
        print('test1Task was initiated')
        self.activator = None

    def run(self):
        print('I am running test1Task')
        if self.config.doPrint:
            print("Displaying Info")

        return Struct(
            val1=10.,
            str1=10)

    def print_activator(self):
        if self.activator == 'cmdLine':
            return self.activator


