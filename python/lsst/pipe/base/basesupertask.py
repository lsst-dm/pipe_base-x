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

    def get_cluster_names(self):
        """
        get list of names of cluster
        :return:
        """
        names = []
        for node in self.get_subgraphs():
            names.append(node.get_label())
        return names


class SuperTask(basetask.Task):
    """
    SuperTask
    """

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(SuperTask, self).__init__(config, name, parent_task, log, activator)

        self._subgraph = ClusterTask(graph_name=self._name  , label=self._name)
        # Add dummy node for connections between clusters
        self._subgraph.add_node(pydot.Node(name='dummy_'+self._name, style='invis', shape='point'))
        self._last = None
        self._last_kind = None
        self._first = None
        self._first_kind = None
        self._task_list = dict()
        self._task_kind = 'SuperTask'


    def link(self, *subtasks):
        """
        Add task or subtasks to the supertask
        :param subtasks:
        :return:
        """
        for task in subtasks:
            task._parent_name = self.name
            task._activator = self.activator
            if task.task_kind == 'Task':
                if not task.name in self._subgraph.get_nodes_names():
                    self._subgraph.add_node(NodeTask(task))
                    self._task_list[task.name] = task  # Might use subclasses instead
                    if self._last is None:
                        self._last = task.name
                        self._last_kind = 'Task'
                    if self._first is None:
                        self._first = task.name
                        self._first_kind = 'Task'

            elif task.task_kind == 'SuperTask':
                if not task.name in self._subgraph.get_cluster_names():
                    self._subgraph.add_subgraph(task._subgraph)
                    self._task_list[task.name] = task  # Might use subclasses instead
                    if self._last is None:
                        self._last = task.name
                        self._last_kind = 'SuperTask'
                    if self._first is None:
                        self._first = task.name
                        self._first_kind = 'SuperTask'
            if self._last != task.name:
                if self._last_kind == 'SuperTask' and task.task_kind == 'Task':
                    self._subgraph.add_edge(pydot.Edge('dummy_'+self._last, task.name, ltail='cluster_'+self._last))  # Sequential
                    self._last = task.name
                    self._last_kind = 'Task'

                elif self._last_kind == 'SuperTask' and task.task_kind == 'SuperTask':
                    self._subgraph.add_edge(pydot.Edge('dummy_'+self._last, 'dummy_'+task.name, ltail='cluster_'+self._last, lhead='cluster_'+task.name))  # Sequential
                    self._last = task.name
                    self._last_kind = 'SuperTask'

                elif self._last_kind == 'Task' and task.task_kind == 'Task':
                    self._subgraph.add_edge(pydot.Edge(self._last, task.name))  # Sequential
                    self._last = task.name
                    self._last_kind = 'Task'

                elif self._last_kind == 'Task' and task.task_kind == 'SuperTask':
                    self._subgraph.add_edge(pydot.Edge(self._last, 'dummy_'+task.name,  lhead='cluster_'+task.name))  # Sequential
                    #self._subgraph.add_edge(pydot.Edge('dummy_'+self._last, task.name, ltail='cluster_'+self._last))  # Sequential
                    self._last = task.name
                    self._last_kind = 'SuperTask'

        return self

    @basetask.wraprun
    def run(self):
        """
        Run method for supertask, need to check for order
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))
        if self._first is not None:
            self._task_list[self._first].run()
            #self._task_list[self._first].completed = True
        for edge in self._subgraph.get_edges():
            source_task = self._task_list[edge.get_source().replace('dummy_','')]
            if not source_task.completed:
                source_task.run()
                #source_task.completed = True
            target_task = self._task_list[edge.get_destination().replace('dummy_','')]
            if not target_task.completed:
                target_task.run()
                #target_task.completed = True
        #self.completed = True


    def get_task_list(self):
        """
        :return:
        """
        return self._task_list

    def write_tree(self):
        """
        Write dot file
        :return:
        """
        temp_g = pydot.Dot(graph_name='SuperTask', compound='true', rankdir='LR')
        temp_g.add_subgraph(self._subgraph)
        temp_g.write('graph.dotfile', format='raw', prog='dot')


    def get_tree(self):
        """
        get tree
        :return:
        """
        tab0 = '+-- '
        tab1 = '|   '
        tabn = '    '
        self.tree = [tab0+self.name]
        if self._first is not None:
            self.tree.append(tab1 + tab0 + self._first)
            for edge in self._subgraph.get_edges():
                ptask = self._task_list[edge.get_destination().replace('dummy_','')]
                if ptask.task_kind == 'Task':
                    self.tree.append(tab1 + tab0 + ptask.name)
                if ptask.task_kind == 'SuperTask':
                    temp_tree=ptask.get_tree()
                    for branch in temp_tree:
                        self.tree.append(tab1  + branch)

        return self.tree


    def print_tree(self):
        """
        Print Tree Ascii
        :return:
        """

        for branch in self.get_tree():
            print(branch)


