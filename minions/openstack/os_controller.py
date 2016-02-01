import zerorpc
import os
import json
from os import environ as env

# Libcloud imports
import keystoneclient.v2_0.client as ksclient
import novaclient.client as nvclient
import glanceclient as glclient

class OpenStackRPC(object):

    def __init__(self):
        self.connected = False

    def createCluster(self, nodes):
        return "Creating %d nodes" % nodes

    # Login to OpenStack
    def login(self):

        if self.connected:
            print "Already connected..."
            return 0

        # Connect to Keystone
        print "Connecting to keystone: ", env['OS_AUTH_URL']
        keystone = ksclient.Client(
            auth_url=env['OS_AUTH_URL'],
            username=env['OS_USERNAME'],
            password=env['OS_PASSWORD'],
            tenant_name=env['OS_TENANT_NAME'],
            region_name=env['OS_REGION_NAME']
        )

        # Get Glance endpoint and connect
        glance_endpoint = keystone.service_catalog.url_for(
            service_type='image'
        )
        print "Connecting to Glance: ", glance_endpoint
        glance = glclient.Client("2",
            endpoint=glance_endpoint,
            token=keystone.auth_token
        )

        # Connect to Nova
        print "Connecting to Nova..."
        nova = nvclient.Client("2.0",
            auth_url=env['OS_AUTH_URL'],
            username=env['OS_USERNAME'],
            api_key=env['OS_PASSWORD'],
            project_id=env['OS_TENANT_NAME'],
            region_name=env['OS_REGION_NAME']
        )
        # Important to do the authentication
        nova.authenticate()

        # Save connections
        self.keystone = keystone
        self.glance = glance
        self.nova = nova
        self.connected = True

        return 0

    # Instance creation
    def createInstance(self, name, image_id):
        print "Creating instance:" + name

        # Get image
        image = self.conn.compute.find_image(image_id)
        print "--- Image :" + image.name + ' --- ' + image.id

        # Get flavor
        flavor = self.conn.compute.find_flavor('m1.tiny')
        print "--- Flavor:" + flavor.name + ' --- ' + flavor.id

        # Create instance
        instance = self.conn.compute.create_server(
            name = name,
            image = image,
            flavor = flavor
        )

        return [0,instance.id]

    def deleteInstance(self, inst_id):
        # Get instance
        self.conn.compute.delete_server(inst_id)
        return [0]

    def getInstanceStatus(self, inst_id):
        # Get instance
        inst = self.conn.compute.find_server(inst_id)
        return [0,inst.status,inst.progress]

    def getImages(self):
        ret = []
        images = self.glance.images.list()
        #for image in images:
        #    ret.append(json.dumps(image.__dict__))
        return list(images)

    def getImageSize(self, id):
        image = self.conn.compute.find_image(id)
        return [0, image.size]

    # Upload an image
    def uploadImage(self, name, filepath):
        # Check file existence
        if os.path.isfile(filepath):
            # Create image
            image = self.glance.images.create(
                name=name,
                disk_format='qcow2',
                container_format='bare',
                is_public='True'
            )
            # Upload data
            print "Uploading ", filepath
            with open(filepath) as fimage:
                self.glance.images.upload(image.id, fimage)
        else:
            return 1

        return 0


# Start OpenStack minion
openstack = zerorpc.Server(OpenStackRPC())
openstack.bind("tcp://0.0.0.0:4242")
openstack.run()
