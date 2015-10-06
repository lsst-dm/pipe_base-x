#!/usr/bin/env python
from __future__ import absolute_import, division, print_function

from basesupertask import SuperTask
from test1task import Test1Task
from test2task import Test2Task
import lsst.pex.config as pexConfig


class SuperTestConfig(pexConfig.Config):
    """
    Config
    """
    minval = pexConfig.Field(
        dtype=int,
        doc="Min value",
        default=2,
    )



class SuperTestTask(SuperTask):
    """
    SuperTest
    """
    ConfigClass = SuperTestConfig
    _default_name = 'Super Test 1'

    def __init__(self, config=None, name=None, parent_task=None, log=None):
        super(SuperTestTask, self).__init__(config, name, parent_task, log)

        T1 = Test1Task(name = 'T1')
        T2 = Test2Task(name = 'T2')

        self.link(T1,T2)


if __name__ == '__main__':

    T1 = Test1Task(name = 'T1')
    T2 = Test2Task(name = 'T2')
    T3 = Test2Task(name = 'T3')
    T4 = Test2Task(name = 'T4')

    S=SuperTask('super')

    S.link(T1,T2, T3)

    S.link(T4)

    S.run()