#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2014 LSST Corporation.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
from lsst.afw.image import MaskU
import lsst.afw.math as afwMath
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.pipe.base.task as basetask



class ExampleSigmaClippedStatsConfig(pexConfig.Config):
    """!Configuration for ExampleSigmaClippedStatsTask
    """
    badMaskPlanes = pexConfig.ListField(
        dtype = str,
        doc = "Mask planes that, if set, the associated pixel should not be included in the coaddTempExp.",
        default = ("EDGE",),
    )
    numSigmaClip = pexConfig.Field(
        doc = "number of sigmas at which to clip data",
        dtype = float,
        default = 3.0,
    )
    numIter = pexConfig.Field(
        doc = "number of iterations of sigma clipping",
        dtype = int,
        default = 2,
    )


class ExampleSigmaClippedStatsTask(basetask.Task):
    """!Example task to compute sigma-clipped mean and standard deviation of an image

    \section pipeTasks_ExampleSigmaClippedStatsTask_Contents Contents

     - \ref pipeTasks_ExampleSigmaClippedStatsTask_Purpose 
     - \ref pipeTasks_ExampleSigmaClippedStatsTask_Config
     - \ref pipeTasks_ExampleSigmaClippedStatsTask_Debug
     - \ref pipeTasks_ExampleSigmaClippedStatsTask_Example

    \section pipeTasks_ExampleSigmaClippedStatsTask_Purpose Description

    \copybrief ExampleSigmaClippedStatsTask

    This is a simple example task designed to be run as a subtask by ExampleCmdLineTask.
    See also ExampleSimpleStatsTask as a variant that is even simpler.
    
    The main method is \ref ExampleSigmaClippedStatsTask.run "run".

    \section pipeTasks_ExampleSigmaClippedStatsTask_Config  Configuration parameters

    See \ref ExampleSigmaClippedStatsConfig

    \section pipeTasks_ExampleSigmaClippedStatsTask_Debug   Debug variables

    This task has no debug variables.

    \section pipeTasks_ExampleSigmaClippedStatsTask_Example A complete example of using ExampleSigmaClippedStatsTask

    This code is in examples/exampleStatsTask.py (this one example runs both
    ExampleSigmaClippedStatsTask and ExampleSimpleStatsTask), and can be run as:
    \code
    examples/exampleStatsTask.py [fitsFile]
    \endcode
    """
    ConfigClass = ExampleSigmaClippedStatsConfig
    _DefaultName = "exampleSigmaClippedStats"

    def __init__(self, *args, **kwargs):
        """!Construct an ExampleSigmaClippedStatsTask

        The init method may compute anything that that does not require data.
        In this case we create a statistics control object using the config
        (which cannot change once the task is created).
        """
        super(ExampleSigmaClippedStatsTask, self).__init__(*args, **kwargs)
        #basetask.Task.__init__(self, *args, **kwargs)

        self._badPixelMask = MaskU.getPlaneBitMask(self.config.badMaskPlanes)

        self._statsControl = afwMath.StatisticsControl()
        self._statsControl.setNumSigmaClip(self.config.numSigmaClip)
        self._statsControl.setNumIter(self.config.numIter)
        self._statsControl.setAndMask(self._badPixelMask)

    @pipeBase.timeMethod
    def run(self, maskedImage):
        """!Compute and return statistics for a masked image

        @param[in] maskedImage: masked image (an lsst::afw::MaskedImage)
        @return a pipeBase Struct containing:
        - mean: mean of image plane
        - meanErr: uncertainty in mean
        - stdDev: standard deviation of image plane
        - stdDevErr: uncertainty in standard deviation
        """
        statObj = afwMath.makeStatistics(maskedImage, afwMath.MEANCLIP | afwMath.STDEVCLIP | afwMath.ERRORS,
            self._statsControl)
        mean, meanErr = statObj.getResult(afwMath.MEANCLIP)
        stdDev, stdDevErr = statObj.getResult(afwMath.STDEVCLIP)
        self.log.info("clipped mean=%0.2f; meanErr=%0.2f; stdDev=%0.2f; stdDevErr=%0.2f" % \
            (mean, meanErr, stdDev, stdDevErr))
        return pipeBase.Struct(
            mean = mean,
            meanErr = meanErr,
            stdDev = stdDev,
            stdDevErr = stdDevErr,
        )
