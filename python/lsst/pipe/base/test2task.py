"""
Test2 Task
"""
from __future__ import absolute_import, division, print_function
from basetask import Task
from basestruct import Struct
import lsst.pex.config as pexConfig


class Test2Config(pexConfig.Config):
    """
    Config
    """
    maxval = pexConfig.Field(
        dtype=int,
        doc="Max value",
        default=22,
    )


class Test2Task(Task):
    """
    Task
    """
    ConfigClass = Test2Config  # ConfigClass = pexConfig.Config
    _default_name = 'test2'

    def __init__(self, *args, **kwargs):
        super(Test2Task, self).__init__(*args, **kwargs)  # # P3 would be super().__init__()
        print('test2Task was initiated')
        self._activator = None

    def run(self):
        """
        Run method
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))


        return Struct(
            val1=20.,
            str1='value 2')

    def __str__(self):
        return str(self.__class__.__name__)+' named : '+self.name