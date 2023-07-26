from easynmt import EasyNMT



class EasyNMT_Model_provider():

    num_instances = 0
    model=None

    def __init__(self):
        self.__class__.num_instances += 1

    @classmethod
    def create_model(cls):
        cls.model='model_instance'

    @classmethod
    def get_model_instance(cls):
        if cls.model==None:
            cls.create_model()
        return cls.model

    @classmethod
    def get_num_instances(self):
        return self.num_instances
