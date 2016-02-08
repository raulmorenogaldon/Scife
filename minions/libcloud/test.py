from lc_minion import *
from os import environ as env

# Testing
print "======== OpenStack ========="
openstack = LibCloudMinion()
config = {
    'url': env['OS_AUTH_URL'],
    'username': env['OS_USERNAME'],
    'password': env['OS_PASSWORD'],
    'project': env['OS_TENANT_NAME'],
    'region': env['OS_REGION_NAME'],
    'provider': "OpenStack"
}
openstack.login(config)

images = openstack.getImages()
for image in images:
    print image

flavors = openstack.getFlavors()
for flavor in flavors:
    print flavor

# Testing
print "======== OpenNebula ========="
opennebula = LibCloudMinion()
config = {
    'url': "http://vesuvius.i3a.uclm.es",
    'port': 2474,
    'username': "oneadmin",
    'password': "35a16a51404255ba4a4b0b66ea9ec5d4",
    'provider': "OpenNebula"
}
opennebula.login(config)

flavors = opennebula.getFlavors()
for flavor in flavors:
    print flavor

images = opennebula.getImages()
for image in images:
    print image

