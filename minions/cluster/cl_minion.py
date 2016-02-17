"""Minion for clusters."""
import json
import uuid
import zerorpc

from minions import minion
##import pexpect
import paramiko

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

        # Command to load bash environment in SSH
        #self.cmd_env = ". ~/.bash_profile"
        self.cmd_env = ". /etc/profile; . ~/.bash_profile"

    def login(self, config):
        """Login to cluster using SSH."""
        if self.connected:
            print("Already connected...")
            return 0

        # Connect to provider
        self.config = config

        # Params
        url = config['url']
        username = config['username']
        password = config['password']

        # Get command line
        ssh = self._retrieveSSH(url, username, password)

        # Get configuration from cloud.json
        stdin, stdout, stderr = ssh.exec_command("cat cloud.json")
        json_config = stdout.read()
        try:
            config = json.loads(json_config)
            self.__loadConfig(config)
        except Exception as e:
            print("Malformed config.json!")
            raise e

        # Close connection
        ssh.close()

        return

    def _retrieveSSH(self, url, username, password=None):
        # Get valid URL
        url = urlparse(url).path
        try:
            print("Connecting...")
            print("User: {0}".format(username))
            print("Endpoint: {0}".format(url))

            # Create command line and connect to the cluster
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(url, username=username)

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

    def createInstance(self, name, image_id, size_id):
        """Reserve resources in cluster"""
        # Get image
        image = self.findImage(image_id)

        # Get size
        size = self.findFlavor(size_id)

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

        # Save instance
        instance = {
            'name': name,
            'id': uuid.uuid1(),
            'image_id': image_id,
            'size_id': size_id,
            'ssh': ssh
        }
        self.instances.append(instance)
        print('Instance "{0}" (DUMMY) created'.format(instance['id']))

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
        stdin, stdout, stderr = ssh.exec_command(cmd)
        stdout.channel.recv_exit_status()

        # Get size
        size = self.findFlavor(instance['size_id'])

        # Create PBS experiment file
        work_dir = "{0}/{1}".format(self.workspace, experiment['id'])
        cd_cmd = "cd {0}".format(work_dir)
        exe_cmd = "./{0}".format(app['execution_script'])
        qsub_cmd = "qsub -l select={0}:ncpus={1}:mem={2}MB -o {3} -e {3}".format(
            1, size['cpus'], size['ram'], work_dir
        )
        cmd = '{0}; echo "{1}; {2};" | {3} '.format(
            self.cmd_env, cd_cmd, exe_cmd, qsub_cmd
        )

        # Execute experiment
        print("==========")
        print("Launching execution: {0}".format(cmd))
        print("Output:")
        print("-------")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        stdout.channel.recv_exit_status()

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
