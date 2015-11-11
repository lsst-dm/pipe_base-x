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
from lsst.afw.display.ds9 import mtv
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from .exampleStatsTasks import ExampleSigmaClippedStatsTask

# The following block adds links to this task from the Task Documentation page.
# This works even for task(s) that are not in lsst.pipe.tasks.
## \addtogroup LSST_task_documentation
## \{
## \page pipeTasks_exampleTask
## \ref ExampleCmdLineTask "ExampleCmdLineTask"
##      An example intended to show how to write a command-line task.
## \}

class ExampleCmdLineConfig(pexConfig.Config):
    """!Configuration for ExampleCmdLineTask
    """
    stats = pexConfig.ConfigurableField(
        doc = "Subtask to compute statistics of an image",
        target = ExampleSigmaClippedStatsTask,
    )
    doFail = pexConfig.Field(
        doc = "Raise an lsst.base.TaskError exception when processing each image? " \
            + "This allows one to see the effects of the --doraise command line flag",
        dtype = bool,
        default = False,
    )

class ExampleCmdLineTask(pipeBase.CmdLineTask):
    """!Example command-line task that computes simple statistics on an image

    \section pipeTasks_ExampleCmdLineTask_Contents Contents

     - \ref pipeTasks_ExampleCmdLineTask_Purpose
     - \ref pipeTasks_ExampleCmdLineTask_Config
     - \ref pipeTasks_ExampleCmdLineTask_Debug
     - \ref pipeTasks_ExampleCmdLineTask_Example

    \section pipeTasks_ExampleCmdLineTask_Purpose Description

    \copybrief ExampleCmdLineTask

    This task was written as an example for the documents \ref pipeTasks_writeTask
    and \ref pipeTasks_writeCmdLineTask.
    The task reads in a "calexp" (a calibrated science \ref lsst::afw::image::Exposure "exposure"),
    computes statistics on the image plane, and logs and returns the statistics.
    In addition, if debugging is enabled, it displays the image in ds9.

    The image statistics are computed using a subtask, in order to show how to call subtasks and how to
    \ref pipeBase_argumentParser_retargetSubtasks "retarget" (replace) them with variant subtasks.

    The main method is \ref ExampleCmdLineTask.run "run".

    \section pipeTasks_ExampleCmdLineTask_Config    Configuration parameters

    See \ref ExampleCmdLineConfig

    \section pipeTasks_ExampleCmdLineTask_Debug     Debug variables

    This task supports the following debug variables:
    <dl>
        <dt>`display`
        <dd>If True then display the exposure in ds9
    </dl>

    To enable debugging, see \ref baseDebug.

    \section pipeTasks_ExampleCmdLineTask_Example A complete example of using ExampleCmdLineTask

    This code is in examples/exampleCmdLineTask.py, and can be run as _e.g._
    \code
    examples/exampleCmdLineTask.py <path_to_data_repo> --id <data_id>
    # The following will work on an NCSA lsst* computer:
    examples/exampleCmdLineTask.py /lsst8/krughoff/diffim_data/sparse_diffim_output_v7_2 --id visit=6866601
    # also try these flags:
    --config doFail=True --doraise
    --show config data
    \endcode
    """
    ConfigClass = ExampleCmdLineConfig
    _DefaultName = "exampleTask"

    def __init__(self, *args, **kwargs):
        """Construct an ExampleCmdLineTask

        Call the parent class constructor and make the "stats" subtask from the config field of the same name.
        """
        pipeBase.CmdLineTask.__init__(self, *args, **kwargs)
        self.makeSubtask("stats")
    
    @pipeBase.timeMethod
    def run(self, dataRef):
        """!Compute a few statistics on the image plane of an exposure
        
        @param dataRef: data reference for a calibrated science exposure ("calexp")
        @return a pipeBase Struct containing:
        - mean: mean of image plane
        - meanErr: uncertainty in mean
        - stdDev: standard deviation of image plane
        - stdDevErr: uncertainty in standard deviation
        """
        if self.config.doFail:
            raise pipeBase.TaskError("Raising TaskError by request (config.doFail=True)")

        # Unpersist the data. In this case the data reference will retrieve a "calexp" by default,
        # so the the string "calexp" is optiona, but the same data reference can be used
        # to retrieve other dataset types that use the same data ID, so it is nice to be explicit
        calExp = dataRef.get("calexp")
        maskedImage = calExp.getMaskedImage()

        # Support extra debug output.
        # - 
        import lsstDebug
        display = lsstDebug.Info(__name__).display
        if display:
            frame = 1
            mtv(calExp, frame=frame, title="photocal")

        # return the pipe_base Struct that is returned by self.stats.run
        return self.stats.run(maskedImage)

    def _getConfigName(self):
        """!Get the name prefix for the task config's dataset type, or None to prevent persisting the config

        This override returns None to avoid persisting metadata for this trivial task.

        However, if the method returns a name, then the full name of the dataset type will be <name>_config.
        The default CmdLineTask._getConfigName returns _DefaultName,
        which for this task would result in a dataset name of "exampleTask_config".

        Normally you can use the default CmdLineTask._getConfigName, but here are two reasons
        why you might want to override it:
        - If you do not want your task to write its config, then have the override return None.
          That is done for this example task, because I didn't want to clutter up the
          repository with config information for a trivial task.
        - If the default name would not be unique. An example is
          \ref lsst.pipe.tasks.makeSkyMap.MakeSkyMapTask "MakeSkyMapTask": it makes a
          \ref lsst.skymap.SkyMap "sky map" (sky pixelization for a coadd)
          for any of several different types of coadd, such as deep or goodSeeing.
          As such, the name of the persisted config must include the coadd type in order to be unique.

        Normally if you override _getConfigName then you override _getMetadataName to match.
        """
        return None
    
    def _getMetadataName(self):
        """!Get the name prefix for the task metadata's dataset type, or None to prevent persisting metadata

        This override returns None to avoid persisting metadata for this trivial task.

        However, if the method returns a name, then the full name of the dataset type will be <name>_metadata.
        The default CmdLineTask._getConfigName returns _DefaultName,
        which for this task would result in a dataset name of "exampleTask_metadata".

        See the description of _getConfigName for reasons to override this method.
        """
        return None
