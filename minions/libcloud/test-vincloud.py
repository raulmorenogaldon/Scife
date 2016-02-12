from lc_minion import *
from os import environ as env

# Testing
print "======== OpenNebula ========="
opennebula = LibCloudMinion()
config = {
    'url': "http://161.67.100.87",
    'port':2474,
    'username': "oneadmin",
    'password': "vincloud",
    'provider': "OpenNebula"
}
opennebula.login(config)

'''
flavors = opennebula.getFlavors()
for flavor in flavors:
    print flavor

images = opennebula.conn.list_images()

'''
one = get_driver(Provider.OPENNEBULA)

driver =  one('oneadmin', 'vincloud')

images = driver.list_images()

print images

