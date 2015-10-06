"""
basetask
"""
from __future__ import absolute_import, division, print_function
import pydot
import basetask

__all__ = ["SuperTask", "NodeTask", "ClusterTask"]


class NodeTask(pydot.Node):
    """
    NodeTask
    """

    def __init__(self, taskclass, obj_dict=None, **attrs):
        """
        :rtype : object
        """
        super(NodeTask, self).__init__(taskclass.name, obj_dict, **attrs)
        setattr(self, taskclass.name, taskclass)


class ClusterTask(pydot.Cluster):
    """
    ClusterTask
    """

    def __init__(self, graph_name='subG', obj_dict=None, suppress_disconnected=False,
                 simplify=False, **attrs):
        """
        :rtype : object
        """
        super(ClusterTask, self).__init__(graph_name, obj_dict, suppress_disconnected,
                                          simplify, **attrs)

    def get_nodes_names(self):
        """
        get list of names of nodes
        :return:
        """
        names = []
        for node in self.get_nodes():
            names.append(node.get_name())
        return names


class SuperTask(basetask.Task):
    """
    SuperTask
    """

    def __init__(self, config=None, name=None, parent_task=None, log=None):
        super(SuperTask, self).__init__(config,name,parent_task,log)

        self._subgraph = ClusterTask(label=self._name)
        self._last = None
        self._first = None
        self._task_list = dict()


    def link(self, *subtasks):
        """
        Add task to the supertask
        :param subtasks:
        :return:
        """
        for task in subtasks:
            if not task.name in self._subgraph.get_nodes_names():
                self._subgraph.add_node(NodeTask(task))
                task._parent_name = self.name
                self._task_list[task.name] = task
                if self._last is None:
                    self._last = task.name
                if self._first is None:
                    self._first = task.name
                if self._last != task.name:
                    self._subgraph.add_edge(pydot.Edge(self._last, task.name))  # Sequential
                self._last = task.name
        return self

    def run(self):
        """
        Run method for supertask
        :return:
        """
        if self._first is not None:
            self._task_list[self._first].run()
            self._task_list[self._first]._completed = True
        for edge in self._subgraph.get_edges():
            source_task = self._task_list[edge.get_source()]
            if not source_task.completed:
                source_task.run()
                source_task._completed = True
            target_task = self._task_list[edge.get_destination()]
            if not target_task.completed:
                target_task.run()
                target_task._completed = True
        self._completed=True


    def get_task_list(self):
        """
        :return:
        """
        return self._task_list

    def write_plan(self):
        """
        Write dot file
        :return:
        """
        temp_g = pydot.Dot(graph_name='SuperTask', composite=True)
        temp_g.add_subgraph(self._subgraph)
        temp_g.write('graph.dotfile', format='raw', prog='dot')
