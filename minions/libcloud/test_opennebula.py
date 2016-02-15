from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver


# Testing
print "======== OpenNebula ========="
provider = get_driver(Provider.OPENNEBULA)
conn = provider(key='oneadmin',secret='vincloud',secure=False,host='161.67.100.87',port=2474,api_version='3.2')
#conn.name
#conn.list_images()
conn.list_sizes()
