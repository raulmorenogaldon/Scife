from minions.cluster.cl_minion import ClusterMinion
from os import environ as env

import getpass

# Testing
print "======== Galgo ========="
cluster = ClusterMinion()
config = {
    'url': "galgo.i3a.info",
    'username': "rmoreno",
    'password': getpass.getpass('password: ')
}
cluster.login(config)

# Add some flavors to the cluster
flavor = {
    'name': 'little',
    'cpus': 1,
    'ram': 1024,
    'disk': 0
}
cluster.createFlavor(flavor)

images = cluster.getImages()
print "Images:"
for image in images:
    print image

flavors = cluster.getFlavors()
print "Flavors:"
for flavor in flavors:
    print flavor
