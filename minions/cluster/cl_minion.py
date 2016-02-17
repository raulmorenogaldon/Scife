"""Minion for clusters."""
import json
import uuid
import zerorpc

from minions import minion
from pexpect import pxssh

from urlparse import urlparse


class ClusterMinion(minion.Minion):
    """Class definition."""

    def __init__(self):
        """Init function."""
        self.connected = False

        # Initialize list of images
        self.images = []

        # Initialize list of flavors
        self.flavors = []

        # Initialize list of instances
        self.instances = []

    def login(self, config):
        """Login to cluster using SSH."""
        if self.connected:
            print("Already connected...")
            return 0

        # Connect to provider
        self.config = config

        # Params
        username = config['username']
        password = config['password']
        url = config['url']

        # Get command line
        ssh = self._retrieveSSH(url, username, password)

        # Get configuration from cloud.json
        ssh.sendline("")
        ssh.prompt()
        ssh.sendline("cat cloud.json")
        ssh.sendline("")
        ssh.prompt()
        try:
            config = json.loads(ssh.before)
            self.__loadConfig(config)
        except Exception as e:
            print("Malformed config.json!")
            raise e

        return

    def _retrieveSSH(self, url, username, password=None):
        # Get valid URL
        url = urlparse(url)
        url = username + "@" + url.path
        try:
            print("Connecting...")
            print("User: {0}".format(username))
            print("Endpoint: {0}".format(url))

            # Create command line and connect to the cluster
            ssh = pxssh.pxssh(echo=False)
            ssh.login(url, username, password)
            print("Connected!")

        except Exception as e:
            print("FAILED to connect to", url, "- Reason:", e)
            raise e

        # Save connection var
        return ssh

    def getImages(self, filter=""):
        """Get image list using an optional name filter."""
        return self.images

    def getFlavors(self, filter=""):
        """Get flavor list using an optional name filter."""
        return self.flavors

    def createFlavor(self, flavor):
        """Create a new flavor and assign an UUID."""
        flavor['id'] = str(uuid.uuid1())
        self.flavors.append(flavor)
        print("Created flavor:")
        print(flavor)

    def createInstance(self, name, id_image, id_size):
        """Reserve resources in cluster"""
        # Get image
        image = self.findImage(id_image)

        # Get size
        size = self.findFlavor(id_size)

        print("=============================")
        print("Creating instance:")
        print("--> Name : {0}".format(name))
        print("--> Image: {0}".format(image))
        print("--> Size : {0}".format(size))

        # Get instance as a SSH connection
        url = self.config['url']
        username = self.config['username']
        password = self.config['password']
        ssh = self._retrieveSSH(url, username, password)

        # Launch job in c)luster
        cmd = "qsub -I -l select={0}:ncpus={1}:mem={2}MB".format(
            1, size['cpus'], size['ram']
        )
        print("Line: {0}".format(cmd))

        # Save instance
        instance = {
            'name': name,
            'id': uuid.uuid1(),
            'ssh': ssh
        }
        self.instances.append(instance)

        return instance['id']

    def deployExperiment(self, storage, app, experiment, instance_id):
        """Deploy an experiment in the cluster FS."""
        print('Deploying app "{0}": {1} - {2}'.format(
            app['name'], experiment['name'], experiment['id']
        ))

        # Get instance
        instance = self.findInstance(instance_id)

        # Get instance command line
        ssh = instance['ssh']
        if ssh is None:
            raise Exception("Instance without SSH")

        # Copy experiment in FS
        experiment_url = storage.getExperimentPublicURL(experiment)
        cmd = "git clone -b {0} {1} {2}/{3}".format(
            experiment['id'], experiment_url, self.workspace, experiment['id']
        )
        print("Cloning: {0}".format(cmd))
        ssh.sendline(cmd)

        # Change working dir
        cd_cmd = "cd {0}/{1}".format(self.workspace, experiment['id'])
        print cd_cmd
        ssh.sendline(cd_cmd)
        ssh.sendline("")
        ssh.prompt()
        ssh.sendline("ls -l")
        ssh.prompt()
        print "LS: ", ssh.before

    def findImage(self, image_id):
        for image in self.images:
            if image['id'] == image_id:
                return image
        return None

    def findFlavor(self, flavor_id):
        for flavor in self.flavors:
            if flavor['id'] == flavor_id:
                return flavor
        return None

    def findInstance(self, inst_id):
        for inst in self.instances:
            if inst['id'] == inst_id:
                return inst
        return None

    """ Private functions """
    def __loadConfig(self, config):
        print("Loading config...")
        print("Images:")
        # Parse images
        for image in config['images']:
            image['id'] = str(uuid.uuid1())
            print(image)
            self.images.append(image)

        # Parse flavors
        print("Flavors:")
        for flavor in config['flavors']:
            flavor['id'] = str(uuid.uuid1())
            print(flavor)
            self.flavors.append(flavor)

        # Parse working directory path
        self.workspace = config['workspace']
        print("Workspace: {0}".format(self.workspace))

        print("Config loaded!")


# Start RPC minion
# Execute this only if called directly from python command
# From now RPC is waiting for requests
if __name__ == "__main__":
    rpc = zerorpc.Server(ClusterMinion())
    rpc.bind("tcp://0.0.0.0:4242")
    rpc.run()
