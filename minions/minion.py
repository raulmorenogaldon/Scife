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

    def createInstance(self, name, id_image, id_size):
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

    def createSize(self, size):
        """
        Create size
        Must return an ID string
        """

    def getImages(self, filter=""):
        """
        Get images list
        filter: String with part of the name
        Return a list of Images
        """
        raise NotImplementedError()

    def getSizes(self, filter=""):
        """
        Get images list
        filter: String with part of the name
        Return a list of Size
        """
        raise NotImplementedError()

    def getInstances(self, filter=""):
        """
        Get instance list
        filter: String with part of the name
        Return a list of Instances
        """
        raise NotImplementedError()

    def findImage(self, id):
        """
        Find image from ID
        id: Image string id
        Return a Image or None
        """
        raise NotImplementedError()

    def findFlavor(self, id):
        """
        Find flavor from ID
        id: Flavor string id
        Return a Flavor or None
        """
        raise NotImplementedError()

    def findInstance(self, id):
        """
        Find instance from ID
        id: Instance string id
        Return a Instance or None
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

    # More methods will be needed
    # getKey, getIP
    # exec(script)
    # ...
