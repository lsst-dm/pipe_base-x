"""
Test1 Task
"""
from __future__ import absolute_import, division, print_function
import basetask
from basestruct import Struct
import lsst.pex.config as pexConfig


class Test1Config(pexConfig.Config):
    """
    Config
    """
    do_print = pexConfig.Field(
        dtype=bool,
        doc="Display info",
        default=False,
    )


class Test1Task(basetask.Task):
    """
    Task
    """
    ConfigClass = Test1Config  # ConfigClass = pexConfig.Config
    _default_name = 'test1'

    def __init__(self, *args, **kwargs):
        super(Test1Task, self).__init__(*args, **kwargs)  # # P3 would be super().__init__()
        print('%s was initiated' % self.name)

    def pre_run(self):
        pass
        #print("Custom pre run commands")

    @basetask.wraprun
    def run(self):
        """
        Run method
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))
        if self.config.do_print:
            print("Displaying Info...")

        return Struct(
            val1=10.,
            str1=10)

    def __str__(self):
        return str(self.__class__.__name__)+' named : '+self.name

if __name__ == '__main__':

    MyTest = Test1Task()
    MyTest.run()