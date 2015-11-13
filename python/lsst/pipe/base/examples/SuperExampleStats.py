from __future__ import absolute_import, division, print_function

from lsst.pipe.base.basesupertask import SuperTask, SuperSeqTask, SuperParTask
from lsst.pipe.base.examples.ExampleStats import ExampleMeanTask
from lsst.pipe.base.examples.ExampleStats import ExampleStdTask
import lsst.pex.config as pexConfig


class AllStatConfig(pexConfig.Config):
    """
    Config
    """
    minval = pexConfig.Field(
        dtype=int,
        doc="Min value",
        default=2,
    )

class AllStatTask(SuperSeqTask):
    """
    SuperTest
    """
    ConfigClass = AllStatConfig
    _default_name = 'All_Stats'

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(AllStatTask, self).__init__(config, name, parent_task, log, activator)
        print('%s was initiated' % self.name)

        Mean = ExampleMeanTask()
        Mean.config.numSigmaClip = 3

        Mean2 = ExampleMeanTask(name='mean_2')
        Mean2.config.numSigmaClip = 5

        Std = ExampleStdTask()

        self.link(Mean, Mean2, Std)