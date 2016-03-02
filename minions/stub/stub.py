"""Stub minion. Does nothing."""
import uuid
import zerorpc

from minions import minion


class StubMinion(minion.Minion):
    """Class definition."""

    def __init__(self):
        """Init function."""
        print("Initializing Stub minion...")
        self.connected = False

        # Initialize list of images
        self.images = []

        # Initialize list of sizes
        self.sizes = []

        # Initialize list of instances
        self.instances = []

        # Initialize list of experiments
        self.experiments = []

        self.workspace = "/home/candyland"

    def login(self, config):
        """Login to nothing."""
        if self.connected:
            print("Already connected...")
            return 0

        # Connect to provider
        self.config = config

        # Load stub config
        print("Loading config...")
        self.images.append({
            'id': str(uuid.uuid1()),
            'name': "Stub image",
            'desc': "Description",
            'inputpath': "/home/image/input",
            'libpath': "/lib",
            'tmppath': "/tmp"
        })
        print("Images:")
        for image in self.images:
            print("- {0} - {1}".format(image['name'], image['id']))
        size = ({
            'id': str(uuid.uuid1()),
            'name': "Stub size",
            'desc': "Description",
            'cpus': 4,
            'ram': 4096
        })
        self.sizes.append(size)
        size = ({
            'id': str(uuid.uuid1()),
            'name': "Stub size",
            'desc': "Description",
            'cpus': 2,
            'ram': 2048
        })
        self.sizes.append(size)
        print("Sizes:")
        for size in self.sizes:
            print("- {0} - {1}, CPUS: {2}, RAM: {3}".format(
                size['name'], size['id'], size['cpus'], size['ram']
            ))

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

    def createInstance(self, instance_cfg):
        """Reserve resources in cluster"""
        name = instance_cfg['name']
        # Get image
        image_id = instance_cfg['image_id']
        image = self.findImage(image_id)
        if image is None:
            raise Exception("Image ID does not exists.")

        # Get size
        size_id = instance_cfg['size_id']
        size = self.findSize(size_id)
        if size is None:
            raise Exception("Size ID does not exists.")

        print("=============================")
        print("Creating instance:")
        print("--> Name : {0}".format(name))
        print("--> Image: {0}".format(image))
        print("--> Size : {0}".format(size))

        # Save instance
        instance = {
            'name': name,
            'id': str(uuid.uuid1()),
            'desc': "Description",
            'image_id': image_id,
            'size_id': size_id
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
        if instance is None:
            raise Exception("Instance ID does not exists")

        # Get size
        size = self.findSize(instance['size_id'])
        if size is None:
            raise Exception("Size ID does not exists")

        # Copy experiment in FS
        experiment_url = experiment['public_url']
        cmd = "git clone -b {0} {1} {2}/{3}".format(
            experiment['id'], experiment_url, self.workspace, experiment['id']
        )
        print("Cloning: {0}".format(cmd))

        # Script for compile creation
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
        cmd = '{0}'.format(exe_script)

        # Execute creation
        print("==========")
        print("Launching creation script: {0}".format(cmd))
        print("Output:")
        print("-------")
        experiment['status'] = "compiling"
        self.experiments.append(experiment)

    def executeExperiment(self, app, experiment, system):
        """Execute an experiment in the cluster FS."""
        print('EXECUTING app "{0}": {1} - {2}'.format(
            app['name'], experiment['name'], experiment['id']
        ))

        # Get instance
        instance_id = system['master']
        instance = self.findInstance(instance_id)
        if instance is None:
            raise Exception("Instance ID does not exists")

        # Get size
        size = self.findSize(instance['size_id'])
        if size is None:
            raise Exception("Size ID does not exists")

        # Create script for experiment
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
        cmd = '{0}'.format(exe_script)

        # Execute experiment
        print("==========")
        print("Launching execution: {0}".format(cmd))
        print("Output:")
        print("-------")
        for exp in self.experiments:
            if exp['id'] == experiment['id']:
                exp['status'] = "executing"

    def pollExperiment(self, experiment, system):
        """Update experiment status."""

        # Get instance
        instance_id = system['master']
        instance = self.findInstance(instance_id)
        if instance is None:
            raise Exception("Instance ID does not exists")

        for exp in self.experiments:
            if exp['id'] == experiment['id']:
                # Check status
                if exp['status'] == "compiling":
                    return "compiled"

                if exp['status'] == "executing":
                    return "done"

        return "failed_compilation"

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


# Start RPC minion
# Execute this only if called directly from python command
# From now RPC is waiting for requests
if __name__ == "__main__":
    rpc = zerorpc.Server(StubMinion(), heartbeat=30)
    rpc.bind("tcp://0.0.0.0:8238")
    rpc.run()
