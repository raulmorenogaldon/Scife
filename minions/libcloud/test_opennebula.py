from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver


# Testing
print "======== OpenNebula ========="
provider = get_driver(Provider.OPENNEBULA)
conn = provider(key='oneadmin',secret='35a16a51404255ba4a4b0b66ea9ec5d4',secure=False,host='vesuvius.i3a.uclm.es',port=2474,api_version='3.8')
conn.name
#conn.list_images()
conn.list_sizes()
