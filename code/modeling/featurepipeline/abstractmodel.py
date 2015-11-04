__author__ = 'College Persistence Team'

from abstractpipeline import *

class AbstractModel(AbstractPipelineConfig):
    '''
    Abstract representation of a model configuration. Should be extended by classes representing
    individual models, not instantiated.
    '''

    def __init__(self):
        pass

    def summary(self):
        raise NotImplementedError

    def predict(self):
        raise NotImplementedError

    def fit(self):
        raise NotImplementedError

    def evaluate(self):
        raise NotImplementedError

    def coefs(self):
        raise NotImplementedError        
