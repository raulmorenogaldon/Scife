"""Minion for clusters."""
import gevent
import json
import uuid
import zerorpc

from minions import minion
import paramiko

from urlparse import urlparse


class ClusterMinion(minion.Minion):
    """Class definition."""

    def __init__(self):
        """Init function."""
        print("Initializing Cluster minion...")
        self.connected = False

        # Initialize list of images
        self.images = []

        # Initialize list of sizes
        self.sizes = []

        # Initialize list of instances
        self.instances = []

        # Command to load bash environment in SSH
        self.cmd_env = ". /etc/profile; . ~/.bash_profile"
        print("Initialization completed!")

    def login(self, config):
        """Login to cluster using SSH."""
        if self.connected:
            print("Already connected...")
            return 0

        # Connect to provider
        self.config = config

        # Params
        url = self.config['url']
        username = self.config['username']
        if 'password' not in self.config:
            self.config['password'] = None
        password = self.config['password']

        # Get command line
        ssh = self._retrieveSSH(url, username, password)

        # Get configuration from cloud.json
        stdin, stdout, stderr = ssh.exec_command("cat cloud.json")
        json_config = stdout.read()
        try:
            config = json.loads(json_config)
            self._loadConfig(config)
        except Exception as e:
            print("Malformed config.json!")
            raise e

        # Close connection
        ssh.close()

        # Set connected
        self.connected = 1

        return

    def createSize(self, size):
        """Create a new size and assign an UUID."""
        size['id'] = str(uuid.uuid1())
        self.sizes.append(size)
        print("Created size:")
        print(size)
        return size['id']

    def createInstance(self, name, image_id, size_id):
        """Reserve resources in cluster"""
        # Get image
        image = self.findImage(image_id)

        # Get size
        size = self.findSize(size_id)

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
            'id': str(uuid.uuid1()),
            'image_id': image_id,
            'size_id': size_id,
            'ssh': ssh
        }
        self.instances.append(instance)
        print('Instance "{0}" (DUMMY) created'.format(instance['id']))

        return instance['id']

    def getImages(self, filter=""):
        """Get image list using an optional name filter."""
        return self.images

    def getSizes(self, filter=""):
        """Get size list using an optional name filter."""
        return self.sizes

    def getInstances(self, filter=""):
        """Get instance list using an optional name filter."""
        return self.instances

    def deployExperiment(self, app, experiment, system):
        """Deploy an experiment in the cluster FS."""
        print('Deploying app "{0}": {1} - {2}'.format(
            app['name'], experiment['name'], experiment['id']
        ))

        # Get master instance
        instance_id = system['master']
        instance = self.findInstance(instance_id)

        # Get size
        size = self.findSize(instance['size_id'])

        # Get instance command line
        ssh = instance['ssh']
        if ssh is None:
            raise Exception("Instance without SSH")

        # Copy experiment in FS
        experiment_url = experiment['public_url']
        cmd = "git clone -b {0} {1} {2}/{3}".format(
            experiment['id'], experiment_url, self.workspace, experiment['id']
        )
        print("Cloning: {0}".format(cmd))
        stdin, stdout, stderr = ssh.exec_command(cmd)
        while not stdout.channel.exit_status_ready():
            gevent.sleep(0)

        # Init EXPERIMENT_STATUS
        cmd = 'echo -n "initialized" > {0}/{1}/EXPERIMENT_STATUS'.format(
            self.workspace, experiment['id']
        )
        stdin, stdout, stderr = ssh.exec_command(cmd)
        while not stdout.channel.exit_status_ready():
            gevent.sleep(0)

        # PBS command for compile creation
        work_dir = "{0}/{1}".format(self.workspace, experiment['id'])
        exe_script = """
            #!/bin/sh
            cd {0}
            echo -n "compiling" > EXPERIMENT_STATUS
            ./{1}
            RETVAL=\$?
            if [ \$RETVAL -eq 0 ]; then
                echo -n "compiled" > EXPERIMENT_STATUS
            else
                echo -n "failed_compilation" > EXPERIMENT_STATUS
            fi
            echo -n \$RETVAL > COMPILATION_EXIT_CODE
        """.format(work_dir, app['creation_script'])
        qsub_cmd = "qsub -N COMPILE -l select={0}:ncpus={1}:mem={2}MB -o {3} -e {3}".format(
            1, size['cpus'], size['ram'], work_dir
        )
        cmd = '{0}; echo "{1}" | {2} '.format(
            self.cmd_env, exe_script, qsub_cmd
        )

        # Execute creation
        print("==========")
        print("Launching creation script: {0}".format(cmd))
        print("Output:")
        print("-------")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        while not stdout.channel.exit_status_ready():
            gevent.sleep(0)

    def executeExperiment(self, app, experiment, system):
        """Execute an experiment in the cluster FS."""
        print('EXECUTING app "{0}": {1} - {2}'.format(
            app['name'], experiment['name'], experiment['id']
        ))

        # Get instance
        instance_id = system['master']
        instance = self.findInstance(instance_id)

        # Get instance command line
        ssh = instance['ssh']
        if ssh is None:
            raise Exception("Instance without SSH")

        # Get size
        size = self.findSize(instance['size_id'])

        # Create PBS script for experiment
        work_dir = "{0}/{1}".format(self.workspace, experiment['id'])
        exe_script = """
            #!/bin/bash
            cd {0}
            echo -n "executing" > EXPERIMENT_STATUS
            ./{1}
            RETVAL=\$?
            if [ \$RETVAL -eq 0 ]; then
                echo -n "done" > EXPERIMENT_STATUS
            else
                echo -n "failed_execution" > EXPERIMENT_STATUS
            fi
            echo -n \$RETVAL > EXECUTION_EXIT_CODE
        """.format(work_dir, app['execution_script'])
        qsub_cmd = "qsub -N EXEC -l select={0}:ncpus={1}:mem={2}MB -o {3} -e {3}".format(
            len(system['instances']), size['cpus'], size['ram'], work_dir
        )
        cmd = '{0}; echo "{1}" | {2} '.format(
            self.cmd_env, exe_script, qsub_cmd
        )

        # Execute experiment
        print("==========")
        print("Launching execution: {0}".format(cmd))
        print("Output:")
        print("-------")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        while not stdout.channel.exit_status_ready():
            gevent.sleep(0)

    def pollExperiment(self, experiment, system):
        """Update experiment status."""

        # Get instance
        instance_id = system['master']
        instance = self.findInstance(instance_id)

        # Get instance command line
        ssh = instance['ssh']
        if ssh is None:
            raise Exception("Instance without SSH")

        # Check status
        work_dir = "{0}/{1}".format(self.workspace, experiment['id'])
        cmd = 'cat {0}/EXPERIMENT_STATUS'.format(work_dir)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        while not stdout.channel.exit_status_ready():
            gevent.sleep(0)
        status = stdout.read()
        return status

    def findImage(self, image_id):
        """Return image data"""
        for image in self.images:
            if image['id'] == image_id:
                return image
        return None

    def findSize(self, size_id):
        """Return size data"""
        for size in self.sizes:
            if size['id'] == size_id:
                return size
        return None

    def findInstance(self, inst_id):
        """Return instance data"""
        for inst in self.instances:
            if inst['id'] == inst_id:
                return inst
        return None

    """ Private functions """
    def _loadConfig(self, config):
        print("Loading config...")
        print("Images:")
        # Parse images
        for image in config['images']:
            image['id'] = str(uuid.uuid1())
            print(image)
            self.images.append(image)

        # Parse sizes
        print("Sizes:")
        for size in config['sizes']:
            size['id'] = str(uuid.uuid1())
            print(size)
            self.sizes.append(size)

        # Parse working directory path
        self.workspace = config['workspace']
        print("Workspace: {0}".format(self.workspace))
        print("Config loaded!")

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


# Start RPC minion
# Execute this only if called directly from python command
# From now RPC is waiting for requests
if __name__ == "__main__":
    rpc = zerorpc.Server(ClusterMinion())
    rpc.bind("tcp://0.0.0.0:8238")
    rpc.run()
