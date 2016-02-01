import zerorpc

from os import environ as env
from urlparse import urlparse

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

class OpenStackLCRPC(object):

    def __init__(self):
        self.connected = False

    # Login with libcloud
    def login(self, strProvider):

        if self.connected:
            print "Already connected..."
            return 0

        # Connect to provider
        self.strProvider = strProvider
        if strProvider == "OpenStack":
            # Get valid URL
            url = urlparse(env['OS_AUTH_URL'])
            url = url.scheme + "://" + url.netloc + "/v2.0/tokens"
            try:
                print "Connecting..."
                print "User: " + env['OS_USERNAME']
                print "Endpoint: " + url
                print "Tenant: " + env['OS_TENANT_NAME']
                print "Region: " + env['OS_REGION_NAME']

                # Connect to provider
                provider = get_driver(Provider.OPENSTACK)
                self.conn = provider(
                    env['OS_USERNAME'],
                    env['OS_PASSWORD'],
                    ex_force_auth_url=url,
                    ex_force_auth_version='2.0_password',
                    ex_force_service_region=env['OS_REGION_NAME'],
                    ex_tenant_name=env['OS_TENANT_NAME']
                )
            except Exception as e:
                print 'FAILED to connect to ' + url + ". Reason: ", e
                return -2

        if strProvider == "OpenNebula":
            # Get valid URL
            url = urlparse(env['ON_AUTH_URL'])
            url = url.netloc
            try:
                print "Connecting..."
                print "User: " + env['ON_USERNAME']
                print "Endpoint: " + url + ":2633"

                # Connect to provider
                provider = get_driver(Provider.OPENNEBULA)
                self.conn = provider(
                    key=env['ON_USERNAME'],
                    secret=env['ON_PASSWORD'],
                    secure=False,
                    host=url,
                    port=2633,
                    api_version='2.0'
                )
            except Exception as e:
                print 'FAILED to connect to ' + url + ". Reason: ", e
                return -2
        else:
            return -1

        return 0

    def getImages(self):
        try:
            images = self.conn.list_images()
            ret = []
            for image in images:
                ret.append({
                    'id': image.id,
                    'name': image.name
                })
        except Exception as e:
            print "Error retrieving the list of images: ", e
        finally:
            return ret

    def getFlavors(self):
        try:
            ret = []
            if self.strProvider == "OpenNebula":
                ret = self.__getFlavorsNebula()
            else:
                ret = self.__getFlavorsOpenStack()
        except Exception as e:
            print "Error retrieving the list of flavors: ", e
        finally:
            return ret

    def __getFlavorsNebula(self):
        sizes = self.conn.list_sizes()
        ret = []
        for size in sizes:
            ret.append({
                'id': size.id,
                'name': size.name,
                'cpus': size.vcpu,
                'ram': size.ram
            })
        return ret

    def __getFlavorsOpenStack(self):
        sizes = self.conn.list_sizes()
        ret = []
        for size in sizes:
            ret.append({
                'id': size.id,
                'name': size.name,
                'cpus': size.vcpus,
                'ram': size.ram
            })
        return ret

# Start OpenStack minion
# Execute this only if called directly from python command
if __name__ == "__main__":
    openstack = zerorpc.Server(OpenStackLCRPC())
    openstack.bind("tcp://0.0.0.0:4242")
    openstack.run()
