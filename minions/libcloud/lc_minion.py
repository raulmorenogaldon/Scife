import zerorpc
from .. import minion

from urlparse import urlparse

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

class LibCloudMinion(minion.Minion):

    def __init__(self):
        self.connected = False

    # Login with libcloud
    def login(self, config):
        if self.connected:
            print "Already connected..."
            return 0

        # Connect to provider
        self.config = config
        if config['provider'] == "OpenStack":
            # Params
            username = config['username']
            password = config['password']
            project  = config['project']
            region   = config['region']

            # Get valid URL
            url = urlparse(config['url'])
            url = url.scheme + "://" + url.netloc + "/v2.0/tokens"
            try:
                print "Connecting..."
                print "User: " + username
                print "Endpoint: " + url
                print "Tenant: " + project
                print "Region: " + region

                # Connect to provider
                provider = get_driver(Provider.OPENSTACK)
                self.conn = provider(
                    username,
                    password,
                    ex_force_auth_url=url,
                    ex_force_auth_version='2.0_password',
                    ex_force_service_region=region,
                    ex_tenant_name=project
                )
            except Exception as e:
                print 'FAILED to connect to ' + url + ". Reason: ", e
                return -2

        if config['provider'] == "OpenNebula":
            # Params
            username = config['username']
            password = config['password']
            port     = config['port']

            print port
            # Get valid URL
            url = urlparse(config['url']).netloc

            try:
                print "Connecting..."
                print "User: " + username
                print "Endpoint: " + url + ":" + str(port)

                # Connect to provider
                provider = get_driver(Provider.OPENNEBULA)
                self.conn = provider(
                    key=username,
                    secret=password,
                    secure=False,
                    host=url,
                    port=port,
                    api_version='3.8'
                )
            except Exception as e:
                print 'FAILED to connect to ' + url + ". Reason: ", e
                return -2
        else:
            return -1

        return 0

    def getImages(self):
        """Return the id and the name of each image"""
        ret = []
        try:
            images = self.conn.list_images()
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
        """Return a list with the templates"""
        try:
            ret = []
            if self.config['provider'] == "OpenNebula":
                ret = self.__getFlavorsNebula()
            else:
                ret = self.__getFlavorsOpenStack()
        except Exception as e:
            print "Error retrieving the list of flavors: ", e
        finally:
            return ret

    def __getFlavorsNebula(self):
        """Return a list with the templates of Opennebula"""
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
        """Return a list with the templates of Openstack"""
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

if __name__ == "__main__":
    """Start OpenStack minion
    Execute this only if called directly from python command
    From now RPC is waiting for requests
    """
    openstack = zerorpc.Server(LibCloudMinion())
    openstack.bind("tcp://0.0.0.0:4242")
    openstack.run()
