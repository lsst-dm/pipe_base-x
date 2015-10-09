"""
basetask
"""
from __future__ import absolute_import, division, print_function
import pydot
import networkx as nx
import basetask

__all__ = ["SuperTask", "NodeTask", "ClusterTask"]



class SuperTask(basetask.Task):
    """
    SuperTask Generic
    """

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(SuperTask, self).__init__(config, name, parent_task, log, activator)


    def get_tasks_labels(self):
        labels = nx.get_node_attributes(self._subgraph, 'label')
        return labels


    @property
    def get_dot(self):
        """
        get subgrapgh
        :return:
        """
        self.lines=[]

        if self._first is not None:
            if (self._task_kind == 'SuperSeqTask'):
                self._current = self._first
            else:
                self.nodes=self._subgraph.nodes_iter()
                self._current = self.nodes.next()

        if (self._current._task_kind == 'SuperSeqTask') :
            self.lines.append('subgraph cluster_%s {' % self._current.name)
            self.lines.append('label = %s ;' % self._current.name)
            self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
            temp_dot = self._current.get_dot
            for branch in temp_dot:
                self.lines.append(branch)
            for branch in self._current.add_edges():
                self.lines.append(branch)
            self.lines.append('}')
        elif (self._current._task_kind == 'SuperParTask') :
            self.lines.append('subgraph cluster_%s {' % self._current.name)
            self.lines.append('node [shape = doublecircle];')
            self.lines.append('label = %s ;' % self._current.name)
            self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
            temp_dot = self._current.get_dot
            for branch in temp_dot:
                self.lines.append(branch)
        else:
            self.lines.append(self._current.name+';')

        if self._task_kind == 'SuperSeqTask':
            while True:
                if not self._subgraph.successors(self._current):
                    break
                else:
                    self._current = self._subgraph.successors(self._current)[0]
                    if (self._current._task_kind == 'SuperSeqTask') :
                        self.lines.append('subgraph cluster_%s {' % self._current.name)
                        self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
                        self.lines.append('label = %s ;' % self._current.name)
                        temp_dot = self._current.get_dot
                        for branch in temp_dot:
                            self.lines.append(branch)
                        for branch in self._current.add_edges():
                            self.lines.append(branch)
                        self.lines.append('}')
                    elif (self._current._task_kind == 'SuperParTask'):
                        self.lines.append('subgraph cluster_%s {' % self._current.name)
                        self.lines.append('node [shape = doublecircle];')

                        self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
                        self.lines.append('label = %s ;' % self._current.name)
                        temp_dot = self._current.get_dot
                        for branch in temp_dot:
                            self.lines.append(branch)
                        self.lines.append('}')

                    else:
                        self.lines.append(self._current.name+';')
        else:
            while True:
                try:
                    self._current = self.nodes.next()
                except StopIteration:
                    break

                if (self._current._task_kind == 'SuperParTask'):
                    self.lines.append('subgraph cluster_%s {' % self._current.name)
                    self.lines.append('node [shape = doublecircle];')

                    self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
                    self.lines.append('label = %s ;' % self._current.name)
                    temp_dot = self._current.get_dot
                    for branch in temp_dot:
                        self.lines.append(branch)
                    self.lines.append('}')
                elif (self._current._task_kind == 'SuperSeqTask'):
                    self.lines.append('subgraph cluster_%s {' % self._current.name)
                    self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
                    self.lines.append('label = %s ;' % self._current.name)
                    temp_dot = self._current.get_dot
                    for branch in temp_dot:
                        self.lines.append(branch)
                    for branch in self._current.add_edges():
                        self.lines.append(branch)
                    self.lines.append('}')
                else:
                    self.lines.append(self._current.name+';')

        return self.lines



    def write_tree(self):
        """
        Write dot file
        :return:
        """
        lines = ['digraph %s {' % self.name]
        lines.append('rankdir=LR;')
        lines.append('compound=true;')
        lines.append('subgraph cluster_%s {' % self.name)
        lines.append('label = %s ;' % self.name)
        for branch in self.get_dot:
            lines.append(branch)
        if self._task_kind == 'SuperSeqTask':
            for branch in self.add_edges():
                lines.append(branch)
        lines.append('}')
        lines.append('}')
        F=open('graph.dot','w')
        for l in lines:
            F.write(l+' \n')
        F.close()

    def get_tree(self,tab='+--'):
        """
        get tree
        :return:
        """

        self.tree = [tab+self.name]
        if self._first is not None:
            self._current = self._first
        tab = '|    '+tab

        if self._current._task_kind == 'SuperSeqTask':
            temp_tree = self._current.get_tree(tab=tab+'> ')
            for branch in temp_tree:
                self.tree.append(branch)
        elif self._current._task_kind == 'SuperParTask':
            temp_tree = self._current.get_tree(tab=tab+'o ')
            for branch in temp_tree:
                self.tree.append(branch)
        else:
            self.tree.append(tab+self._current.name)
        while True:
            if not self._subgraph.successors(self._current):
                break
            else:
                self._current = self._subgraph.successors(self._current)[0]
                if self._current._task_kind == 'SuperSeqTask':
                    temp_tree = self._current.get_tree(tab=tab+'> ')
                    for branch in temp_tree:
                        self.tree.append(branch)
                elif self._current._task_kind == 'SuperParTask':
                    temp_tree = self._current.get_tree(tab=tab+'o ')
                    for branch in temp_tree:
                        self.tree.append(branch)
                else:
                    self.tree.append(tab+self._current.name)
        return self.tree


    def print_tree(self):
        """
        Print Tree Ascii
        :return:
        """

        for branch in self.get_tree():
            print(branch)




class SuperSeqTask(SuperTask):
    """
    SuperTask Sequential
    """

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(SuperSeqTask, self).__init__(config, name, parent_task, log, activator)

        self._subgraph = nx.DiGraph(label = self.name)

        self._last = None
        self._last_kind = None
        self._first = None
        self._first_kind = None
        self._task_list = dict()
        self._task_kind = 'SuperSeqTask'
        self._tasks_kind = None
        self._current = None
        self._next = None


    def link(self, *subtasks):
        """
        Add task or subtasks to the supertask
        :param subtasks:
        :return:
        """
        for task in subtasks:
            task._parent_name = self.name
            task._activator = self.activator

            if task.name not in self.get_tasks_labels():
                self._subgraph.add_node(task, label = task.name, kind=self._task_kind)
                if self._first is None:
                    self._first = task
                if self._last is None:
                    self._last = task

                if self._last.name != task.name:
                    self._subgraph.add_edge(self._last, task)
                    self._last = task
        return self


    @basetask.wraprun
    def run(self):
        """
        Run method for supertask, need to check for order
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))
        if self._first is not None:
            self._first.run()
        self._current = self._first
        while True:
            if not self._subgraph.successors(self._current):
                break
            else:
                self._current = self._subgraph.successors(self._current)[0]
                self._current.run()



    def add_edges(self):
        lines=[]
        for e in self._subgraph.edges():
            source = e[0].name
            target = e[1].name
            opt = []
            if (e[0]._task_kind == 'SuperSeqTask') or (e[0]._task_kind == 'SuperParTask'):
                source = 'dummy_'+e[0].name
                opt.append(' ltail = cluster_'+e[0].name)
            if (e[1]._task_kind == 'SuperSeqTask') or (e[1]._task_kind == 'SuperParTask'):
                target = 'dummy_'+e[1].name
                opt.append(' lhead = cluster_'+e[1].name)
            temp_line = source + '->' + target
            if opt:
                temp_line += ' [ '+ ','.join(opt) + ']'
            lines.append(temp_line+';')
        return lines



class SuperParTask(SuperTask):
    """
    SuperTask Parallel (undirected)
    """

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(SuperParTask, self).__init__(config, name, parent_task, log, activator)

        self._subgraph = nx.Graph(label = self.name)
        self._task_kind = 'SuperParTask'

        self._last = None
        self._first = None
        self._current = None


    def link(self, *subtasks):
        """
        Add task or subtasks to the supertask
        :param subtasks:
        :return:
        """
        for task in subtasks:
            task._parent_name = self.name
            task._activator = self.activator
            if task.name not in self.get_tasks_labels():
                if self._first is None:
                    self._first = task
                self._subgraph.add_node(task, label = task.name, kind=self._task_kind)
        return self


    @basetask.wraprun
    def run(self):
        """
        Run method for supertask, need to check for order
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))
        for node in self._subgraph.nodes():
            node.run()



