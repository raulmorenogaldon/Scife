# Class that act as interface
# Any Node.js (or from python) process will use only these methods
class Minion:

    # Additional clases must be implemented
    # Class: Image
    # Class: Flavor
    # Class: Instance

    def login(self, config):
        """
        Login to remote service
        Return nothing
        """
        raise NotImplementedError()

    def createInstance(self, instance_cfg):
        """
        Create instance
        Must return an ID string
        """
        raise NotImplementedError()

    def destroyInstance(self, id_inst):
        """
        Destroy instance
        Return nothing
        """
        raise NotImplementedError()

    def createSize(self, size_cfg):
        """
        Create size
        Must return an ID string
        """

    def getImages(self, filter=""):
        """
        Get images list
        filter: String with ID or part of the name
        Return a list of Images
        """
        raise NotImplementedError()

    def getSizes(self, filter=""):
        """
        Get sizes list
        filter: String with ID or with part of the name
        Return a list of Size
        """
        raise NotImplementedError()

    def getInstances(self, filter=""):
        """
        Get instance list
        filter: String with ID or with part of the name
        Return a list of Instances
        """
        raise NotImplementedError()

    def deployExperiment(self, app, experiment, system):
        """
        Deploy experiment
        app: Application data
        experiment: Experiment data
        system: Cluster of instances
        """
        raise NotImplementedError()

    def executeExperiment(self, app, experiment, system):
        """
        Execute a deployed experiment
        app: Application data
        experiment: Experiment data
        system: Cluster of instances
        """
        raise NotImplementedError()

    def pollExperiment(self, experiment, system):
        """
        Return experiment status
        experiment: Experiment data
        system: Cluster of instances
        Return a status string
        """
        raise NotImplementedError()

    def cleanExperiment(self, experiment, system):
        """
        Remove experiment data from instances
        experiment: Experiment data
        system: Cluster of instances
        """
        raise NotImplementedError()

    # More methods will be needed
    # getKey, getIP
    # exec(script)
    # ...

