import zerorpc
import uuid
import json

from pexpect import pxssh
from .. import minion

from urlparse import urlparse

class ClusterMinion(minion.Minion):

    def __init__(self):
        self.connected = False

        # Initialize list of images
        self.images = []

        # Initialize list of flavors
        self.flavors = []

    # Login to cluster (SSH)
    def login(self, config):
        if self.connected:
            print "Already connected..."
            return 0

        # Connect to provider
        self.config = config

        # Params
        username = config['username']
        password = config['password']

        # Get valid URL
        url = urlparse(config['url'])
        url = username + "@" + url.path
        try:
            print "Connecting..."
            print "User: " + username
            print "Endpoint: " + url

            # Create command line and connect to the cluster
            ssh = pxssh.pxssh(echo=False)
            ssh.login(url, username, password)
            ssh.prompt()
            print "Connected!"

            # Save connection var
            self.ssh = ssh

        except Exception as e:
            print 'FAILED to connect to ' + url + ". Reason: ", e
            raise e

        # Get configuration from cloud.json
        ssh.sendline("cat cloud.json")
        ssh.prompt()
        try:
            config = json.loads(ssh.before)
            self.__loadConfig(config)
        except Exception as e:
            print "Malformed config.json!"
            raise e

        return

    def getImages(self, filter=""):
        return self.images

    def getFlavors(self, filter=""):
        return self.flavors

    def createFlavor(self, flavor):
        flavor['id'] = str(uuid.uuid1())
        self.flavors.append(flavor)
        print "Created flavor:"
        print flavor

    def __loadConfig(self, config):
        # Parse images
        for image in config['images']:
            image['id'] = str(uuid.uuid1())
            self.images.append(image)

        # Parse flavors
        for flavor in config['flavors']:
            flavor['id'] = str(uuid.uuid1())
            self.flavors.append(flavor)


# Start RPC minion
# Execute this only if called directly from python command
# From now RPC is waiting for requests
if __name__ == "__main__":
    rpc = zerorpc.Server(ClusterMinion())
    rpc.bind("tcp://0.0.0.0:4242")
    rpc.run()
