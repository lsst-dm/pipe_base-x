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
    _default_name = 'Super_Task_1'

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(SuperTestTask, self).__init__(config, name, parent_task, log, activator)
        print('%s was initiated' % self.name)

        T1 = Test1Task()
        T1.config.do_print = True
        T2 = Test2Task()
        T3 = Test2Task(name='T3')

        self.link(T1, T2, T3)


class Super2Config(pexConfig.Config):
    """
    Config
    """
    minval = pexConfig.Field(
        dtype=int,
        doc="Min value",
        default=2,
    )

class Super2Task(SuperTask):
    """
    SuperTest
    """
    ConfigClass = Super2Config
    _default_name = 'Big_Task'

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(Super2Task, self).__init__(config, name, parent_task, log, activator)
        print('%s was initiated' % self.name)

        T4 = Test1Task(name='T4')
        T5 = Test2Task(name='T5')
        T6 = Test2Task(name='T6')
        S1 = SuperTestTask(name = 'S1')
        S2 = SuperTask(name = 'S2Task', config = pexConfig.Config)

        S2.link(Test1Task(name='TR7'), Test1Task(name='TR8'))


        self.link(T4, S1, T5, S2, T6)


if __name__ == '__main__':
    MyTest = Super2Task()
    MyTest.write_tree()
    MyTest.run()
    print()
    MyTest.print_tree()
