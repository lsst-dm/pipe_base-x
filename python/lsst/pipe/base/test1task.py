#test1

#SuperTask
class test1Task(object):
    def __init__(self):
        print 'test1Task was initialized'
        self.activator=None
    def run(self):
        print 'I am running test1Task'

    def print_activator(self):
        if self.activator == 'cmdLine':
            return self.activator


class test1Config(object):
    def __init__(self):
        pass

