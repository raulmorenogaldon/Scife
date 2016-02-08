# Class that act as interface
# Any Node.js (or from python) process will use only these methods
class Minion:

    # Additional clases must be implemented
    # Class: Image
    # Class: Flavor
    # Class: Instance

    # Login to remote service
    # Return nothing
    def login(self, config):
        raise NotImplementedError()

    # Create instance
    # Must return an ID string
    def createInstance(self, name, id_image, id_size):
        raise NotImplementedError()

    # Destroy instance
    # Return nothing
    def destroyInstance(self, id_inst):
        raise NotImplementedError()

    # Get images list
    # filter: String with part of the name
    # Return a list of Images
    def getImages(self, filter):
        raise NotImplementedError()

    # Get images list
    # filter: String with part of the name
    # Return a list of Flavors
    def getFlavors(self, filter):
        raise NotImplementedError()

    # Get instance list
    # filter: String with part of the name
    # Return a list of Instances
    def getInstances(self, filter):
        raise NotImplementedError()

    # Find image from ID
    # id: Image string id
    # Return a Image or None
    def findImage(self, id):
        raise NotImplementedError()

    # Find flavor from ID
    # id: Flavor string id
    # Return a Flavor or None
    def findFlavor(self, id):
        raise NotImplementedError()

    # Find instance from ID
    # id: Instance string id
    # Return a Instance or None
    def findInstance(self, id):
        raise NotImplementedError()

    # More methods will be needed
    # getKey, getIP
    # exec(script)
    # ...

