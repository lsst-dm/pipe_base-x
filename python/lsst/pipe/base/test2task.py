#test2

#SuperTask
class test2Task(object):
    def __init__(self):
        print 'test2Task was initialized'
        self.activator=None
    def run(self):
        print 'I am running test2Task'

    def print_activator(self):
        if self.activator == 'cmdLine':
            return  self.activator


class test2Config(object):
    def __init__(self):
        pass