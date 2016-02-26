"""Minion for clusters."""
import gevent
import json
import uuid
import zerorpc

from minions import minion
import paramiko

from urlparse import urlparse

import pymongo
from pymongo import MongoClient

class ClusterMinion(minion.Minion):
    """Class definition."""

    def __init__(self):
        """Init function."""
        print("Initializing Cluster minion...")
        self._connected = False

        # Connect to DB (default "localhost")
        # db vars must be private to avoid zerorpc errors
        print("Connecting to DB...")
        try:
            self._db_client = MongoClient()
            self._db = self._db_client.test_db
        except Exception as e:
            print("Failed to get Storage database, reason: ", e)
            raise e

        try:
            self._db.images.create_index([
                ('id', pymongo.ASCENDING),
                ('name', pymongo.ASCENDING),
                ('minion', pymongo.ASCENDING)
            ], unique=True)
            self._db.sizes.create_index([
                ('id', pymongo.ASCENDING),
                ('name', pymongo.ASCENDING),
                ('minion', pymongo.ASCENDING)
            ], unique=True)
            self._db.instances.create_index([
                ('id', pymongo.ASCENDING),
                ('name', pymongo.ASCENDING),
                ('minion', pymongo.ASCENDING)
            ], unique=True)
        except Exception as e:
            print("Failed to create DB index, reason: ", e)
            raise e

        # Locks
        self._login_lock = False
        self._instance_lock = {}

        # SSH connections
        self._instance_ssh = {}

        # Command to load bash environment in SSH
        self._cmd_env = ". /etc/profile; . ~/.bash_profile"
        print("Initialization completed!")

    def login(self, config):
        """Login to cluster using SSH."""

        ####################
        # Set the lock for login
        while self._login_lock:
            gevent.sleep(0)
        self._login_lock = True

        # Check if already connected
        if self._connected:
            print("Already connected...")
            self._login_lock = False
            return 0

        # Connect to provider
        self._config = config

        # Params
        url = self._config['url']
        username = self._config['username']
        if 'password' not in self._config:
            self._config['password'] = None
        password = self._config['password']

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
        self._connected = 1

        self._login_lock = False
        ####################

        return

    def createSize(self, size_cfg):
        """Create a new size and assign an UUID."""
        id = str(uuid.uuid1())
        size = {
            "_id": id,
            "id": id,
            "name": size_cfg['name'],
            "desc": "Description",
            "cpus": size_cfg['cpus'],
            "ram": size_cfg['ram'],
            "minion": self.__class__.__name__
        }
        self._db.sizes.insert_one(size)
        print("Created size:")
        print(size)
        return size['id']

    def createInstance(self, instance_cfg):
        """Reserve resources in cluster"""
        name = instance_cfg['name']

        # Get image
        image_id = instance_cfg['image_id']
        image = self.findImage(image_id)

        # Get size
        size_id = instance_cfg['size_id']
        size = self.findSize(size_id)

        print("=============================")
        print("Creating instance:")
        print("--> Name : {0}".format(name))
        print("--> Image: {0}".format(image))
        print("--> Size : {0}".format(size))

        # Get instance as a SSH connection
        url = self._config['url']
        username = self._config['username']
        password = self._config['password']
        ssh = self._retrieveSSH(url, username, password)

        # Save instance
        id = str(uuid.uuid1())
        instance = {
            '_id': id,
            'id': id,
            'name': name,
            'image_id': image_id,
            'size_id': size_id,
            'minion': self.__class__.__name__,
            'deployed': False,
            'executed': False
        }
        self._instance_ssh[id] = ssh
        self._instance_lock[id] = False
        self._db.instances.insert_one(instance)
        print('Instance "{0}" (DUMMY) created'.format(instance['id']))

        return instance['id']

    def getImages(self, filter=""):
        """Get image list using an optional name filter."""
        return list(
            self._db.images.find({'minion': self.__class__.__name__})
        )

    def getSizes(self, filter=""):
        """Get size list using an optional name filter."""
        return list(
            self._db.sizes.find({'minion': self.__class__.__name__})
        )

    def getInstances(self, filter=""):
        """Get instance list using an optional name filter."""
        return list(
            self._db.instances.find({'minion': self.__class__.__name__})
        )

    def deployExperiment(self, app, experiment, system):
        """Deploy an experiment in the cluster FS."""
        print('Deploying app "{0}": {1} - {2}'.format(
            app['name'], experiment['name'], experiment['id']
        ))

        # Get master instance
        instance_id = system['master']

        ####################
        # Set the lock for the instance
        while self._instance_lock[instance_id]:
            gevent.sleep(0)
        self._instance_lock[instance_id] = True

        # Get instance
        instance = self.findInstance(instance_id)

        # Check if instance is already deployed
        if instance['deployed']:
            self._instance_lock[instance_id] = False
            raise Exception("Experiment {0} is already deployed in instance {1}".format(
                experiment['id'], instance['id']
            ))

        # Get instance command line
        ssh = self._instance_ssh[instance_id]
        if ssh is None:
            self._instance_lock[instance_id] = False
            raise Exception("Instance without SSH")

        # Get size
        size = self.findSize(instance['size_id'])

        # Copy experiment in FS
        experiment_url = experiment['public_url']
        cmd = "git clone -b {0} {1} {2}/{3}".format(
            experiment['id'], experiment_url, self._workspace, experiment['id']
        )
        print("Cloning: {0}".format(cmd))
        task = gevent.spawn(self._executeSSH, ssh, cmd)
        gevent.joinall([task])

        # Init EXPERIMENT_STATUS
        cmd = 'echo -n "initialized" > {0}/{1}/EXPERIMENT_STATUS'.format(
            self._workspace, experiment['id']
        )
        task = gevent.spawn(self._executeSSH, ssh, cmd)
        gevent.joinall([task])

        # PBS command for compile creation
        work_dir = "{0}/{1}".format(self._workspace, experiment['id'])
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
            self._cmd_env, exe_script, qsub_cmd
        )

        # Execute creation
        print("==========")
        print("Launching creation script: {0}".format(cmd))
        print("Output:")
        print("-------")
        task = gevent.spawn(self._executeSSH, ssh, cmd)
        gevent.joinall([task])

        # Update in DB
        self._db.instances.update_one(
            {'id': instance_id},
            {"$set": {"deployed": True}}
        )
        self._instance_lock[instance_id] = False
        ####################

    def executeExperiment(self, app, experiment, system):
        """Execute an experiment in the cluster FS."""
        print('EXECUTING app "{0}": {1} - {2}'.format(
            app['name'], experiment['name'], experiment['id']
        ))

        # Get instance
        instance_id = system['master']

        ####################
        # Set the lock for the instance
        while self._instance_lock[instance_id]:
            gevent.sleep(0)
        self._instance_lock[instance_id] = True

        # Get instance
        instance = self.findInstance(instance_id)

        # Check if instance is already executed
        if instance['executed']:
            self._instance_lock[instance_id] = False
            raise Exception("Experiment {0} is already executed in instance {1}".format(
                experiment['id'], instance['id']
            ))

        # Get instance command line
        ssh = self._instance_ssh[instance_id]
        if ssh is None:
            self._instance_lock[instance_id] = False
            raise Exception("Instance without SSH")

        # Get size
        size = self.findSize(instance['size_id'])

        # Create PBS script for experiment
        work_dir = "{0}/{1}".format(self._workspace, experiment['id'])
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
            self._cmd_env, exe_script, qsub_cmd
        )

        # Execute experiment
        print("==========")
        print("Launching execution: {0}".format(cmd))
        print("Output:")
        print("-------")
        task = gevent.spawn(self._executeSSH, ssh, cmd)
        gevent.joinall([task])

        # Update in DB
        self._db.instances.update_one(
            {'id': instance_id},
            {"$set": {"executed": True}}
        )
        self._instance_lock[instance_id] = False
        ####################

    def pollExperiment(self, experiment, system):
        """Update experiment status."""

        # Get instance
        instance_id = system['master']

        ####################
        # Set the lock for the instance
        while self._instance_lock[instance_id]:
            gevent.sleep(0)
        self._instance_lock[instance_id] = True

        # Get instance command line
        ssh = self._instance_ssh[instance_id]
        if ssh is None:
            self._instance_lock[instance_id] = False
            raise Exception("Instance without SSH")

        # Check status
        work_dir = "{0}/{1}".format(self._workspace, experiment['id'])
        cmd = 'cat {0}/EXPERIMENT_STATUS'.format(work_dir)
        task = gevent.spawn(self._executeSSH, ssh, cmd)
        gevent.joinall([task])
        status = task.value[0].read()
        if status == "":
            status = None

        self._instance_lock[instance_id] = False
        ####################

        return status

    def findImage(self, image_id):
        """Return image data"""
        for img in self._db.images.find({
            'id': image_id,
            'minion': self.__class__.__name__
        }):
            return img
        return None

    def findSize(self, size_id):
        """Return size data"""
        for size in self._db.sizes.find({
            'id': size_id,
            'minion': self.__class__.__name__
        }):
            return size
        return None

    def findInstance(self, inst_id):
        """Return instance data"""
        for instance in self._db.instances.find({
            'id': inst_id,
            'minion': self.__class__.__name__
        }):
            return instance
        return None

    """ Private functions """
    def _loadConfig(self, config):
        print("Loading config...")

        # Get existing data from DB
        prev_images = self._db.images.find({'minion': self.__class__.__name__})
        prev_sizes = self._db.sizes.find({'minion': self.__class__.__name__})

        # Parse images
        print("Parsing images...")
        for image in config['images']:
            # Check if already exists
            found = False
            for prev_img in prev_images:
                if image['name'] == prev_img['name']:
                    # Found!
                    found = True
                    print('-- Image "{0}" already exists'.format(image['name']))
                    break
            if not found:
                id = str(uuid.uuid1())
                image['_id'] = id
                image['id'] = id
                image['minion'] = self.__class__.__name__
                print("-- Adding image: {0}".format(image))
                self._db.images.insert_one(image)

        # Parse sizes
        print("Parsing sizes...")
        for size in config['sizes']:
            # Check if already exists
            found = False
            for prev_size in prev_sizes:
                if size['name'] == prev_size['name'] \
                    and size['cpus'] == prev_size['cpus'] \
                        and size['ram'] == prev_size['ram']:
                    # Found!
                    found = True
                    print('-- Size "{0}" already exists'.format(size['name']))
                    break
            if not found:
                id = str(uuid.uuid1())
                size['_id'] = id
                size['id'] = id
                size['minion'] = self.__class__.__name__
                print("-- Adding size: {0}".format(size))
                self._db.sizes.insert_one(size)

        # Parse working directory path
        self._workspace = config['workspace']
        print("Workspace: {0}".format(self._workspace))
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

    def _executeSSH(self, ssh, cmd):
        stdin, stdout, stderr = ssh.exec_command(cmd)
        while not stdout.channel.exit_status_ready():
            gevent.sleep(0)
        return [stdout, stderr]

# Start RPC minion
# Execute this only if called directly from python command
# From now RPC is waiting for requests
if __name__ == "__main__":
    rpc = zerorpc.Server(ClusterMinion(), heartbeat=30)
    rpc.bind("tcp://0.0.0.0:8238")
    rpc.run()
