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

from .task import Task, TaskError
from .struct import Struct
from .superTaskRunner import SuperTaskRunner
from .argumentParser import ArgumentParser


__all__ = ["SuperTask"]


class SuperTask(Task, SuperTaskRunner):

    RunnerClass = TaskRunner
    canMultiprocess = True

    @classmethod
    def applyOverrides(cls, config):
        """!A hook to allow a task to change the values of its config *after* the camera-specific
        overrides are loaded but before any command-line overrides are applied.

        This is necessary in some cases because the camera-specific overrides may retarget subtasks,
        wiping out changes made in ConfigClass.setDefaults. See LSST Trac ticket #2282 for more discussion.

        @warning This is called by CmdLineTask.parseAndRun; other ways of constructing a config
        will not apply these overrides.

        @param[in] cls      the class object
        @param[in] config   task configuration (an instance of cls.ConfigClass)
        """
        pass

    @classmethod
    def parseAndRun(cls, args=None, config=None, log=None, doReturnResults=False):
        """!Parse an argument list and run the command

        Calling this method with no arguments specified is the standard way to run a command-line task
        from the command line. For an example see pipe_tasks `bin/makeSkyMap.py` or almost any other
        file in that directory.

        @param cls      the class object
        @param args     list of command-line arguments; if `None` use sys.argv
        @param config   config for task (instance of pex_config Config); if `None` use cls.ConfigClass()
        @param log      log (instance of lsst.pex.logging.Log); if `None` use the default log
        @param doReturnResults  Return the collected results from each invocation of the task?
            This is only intended for unit tests and similar use.
            It can easily exhaust memory (if the task returns enough data and you call it enough times)
            and it will fail when using multiprocessing if the returned data cannot be pickled.

        @return a Struct containing:
        - argumentParser: the argument parser
        - parsedCmd: the parsed command returned by the argument parser's parse_args method
        - taskRunner: the task runner used to run the task (an instance of cls.RunnerClass)
        - resultList: results returned by the task runner's run method, one entry per invocation.
            This will typically be a list of `None` unless doReturnResults is `True`;
            see cls.RunnerClass (TaskRunner by default) for more information.
        """
        argumentParser = cls._makeArgumentParser()
        if config is None:
            config = cls.ConfigClass()
        parsedCmd = argumentParser.parse_args(config=config, args=args, log=log, override=cls.applyOverrides)
        taskRunner = cls.SuperTaskRunner(TaskClass=cls, parsedCmd=parsedCmd, doReturnResults=doReturnResults)
        resultList = taskRunner.run(parsedCmd)
        return Struct(
            argumentParser = argumentParser,
            parsedCmd = parsedCmd,
            taskRunner = taskRunner,
            resultList = resultList,
        )

    @classmethod
    def _makeArgumentParser(cls):
        """!Create and return an argument parser

        @param[in] cls      the class object
        @return the argument parser for this task.

        By default this returns an ArgumentParser with one ID argument named `--id`  of dataset type "raw".

        Your task subclass may need to override this method to change the dataset type or data ref level,
        or to add additional data ID arguments. If you add additional data ID arguments or your task's
        run method takes more than a single data reference then you will also have to provide a task-specific
        task runner (see TaskRunner for more information).
        """
        parser = ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument(name="--id", datasetType="raw", help="data ID, e.g. --id visit=12345 ccd=1,2")
        return parser


### TODO: These should be on each activator,

    def writeConfig(self, butler, clobber=False):
        """!Write the configuration used for processing the data, or check that an existing
        one is equal to the new one if present.

        @param[in] butler   data butler used to write the config.
            The config is written to dataset type self._getConfigName()
        @param[in] clobber  a boolean flag that controls what happens if a config already has been saved:
            - True: overwrite the existing config
            - False: raise TaskError if this config does not match the existing config
        """
        configName = self._getConfigName()
        if configName is None:
            return
        if clobber:
            butler.put(self.config, configName, doBackup=True)
        elif butler.datasetExists(configName):
            # this may be subject to a race condition; see #2789
            oldConfig = butler.get(configName, immediate=True)
            output = lambda msg: self.log.fatal("Comparing configuration: " + msg)
            if not self.config.compare(oldConfig, shortcut=False, output=output):
                raise TaskError(
                    ("Config does not match existing task config %r on disk; tasks configurations " + \
                    "must be consistent within the same output repo (override with --clobber-config)") % \
                    (configName,))
        else:
            butler.put(self.config, configName)

    def writeSchemas(self, butler, clobber=False):
        """!Write the schemas returned by \ref task.Task.getAllSchemaCatalogs "getAllSchemaCatalogs"

        @param[in] butler   data butler used to write the schema.
            Each schema is written to the dataset type specified as the key in the dict returned by
            \ref task.Task.getAllSchemaCatalogs "getAllSchemaCatalogs".
        @param[in] clobber  a boolean flag that controls what happens if a schema already has been saved:
            - True: overwrite the existing schema
            - False: raise TaskError if this schema does not match the existing schema

        @warning if clobber is False and an existing schema does not match a current schema,
        then some schemas may have been saved successfully and others may not, and there is no easy way to
        tell which is which.
        """
        for dataset, catalog in self.getAllSchemaCatalogs().iteritems():
            schemaDataset = dataset + "_schema"
            if clobber:
                butler.put(catalog, schemaDataset, doBackup=True)
            elif butler.datasetExists(schemaDataset):
                oldSchema = butler.get(schemaDataset, immediate=True).getSchema()
                if not oldSchema.compare(catalog.getSchema(), afwTable.Schema.IDENTICAL):
                    raise TaskError(
                        ("New schema does not match schema %r on disk; schemas must be " + \
                        " consistent within the same output repo (override with --clobber-config)") % \
                        (dataset,))
            else:
                butler.put(catalog, schemaDataset)

    def writeMetadata(self, dataRef):
        """!Write the metadata produced from processing the data

        @param[in] dataRef  butler data reference used to write the metadata.
            The metadata is written to dataset type self._getMetadataName()
        """
        try:
            metadataName = self._getMetadataName()
            if metadataName is not None:
                dataRef.put(self.getFullMetadata(), metadataName)
        except Exception, e:
            self.log.warn("Could not persist metadata for dataId=%s: %s" % (dataRef.dataId, e,))

    def _getConfigName(self):
        """!Return the name of the config dataset type, or None if config is not to be persisted

        @note The name may depend on the config; that is why this is not a class method.
        """
        return self._DefaultName + "_config"

    def _getMetadataName(self):
        """!Return the name of the metadata dataset type, or None if metadata is not to be persisted

        @note The name may depend on the config; that is why this is not a class method.
        """
        return self._DefaultName + "_metadata"

