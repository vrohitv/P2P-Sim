class BaseMessage(object):
    #basic meaasge to be carried out in peers
    base_size = 20
    def __init__(self, sender, data=None):
        self.sender = sender
        self.data = data
    @property
    def size(self):
        return self.base_size + len(repr(self.data))
    def __repr__(self):
        return '<%s>' % self.__class__.__name__
