from lc_controller import *

# Testing
#print "======== OpenStack ========="
#openstack = OpenStackLCRPC()
#openstack.login("OpenStack")
#
#images = openstack.getImages()
#for image in images:
#    print image
#
#flavors = openstack.getFlavors()
#for flavor in flavors:
#    print flavor

# Testing
print "======== OpenNebula ========="
opennebula = OpenStackLCRPC()
opennebula.login("OpenNebula")

flavors = opennebula.getFlavors()
for flavor in flavors:
    print flavor

images = opennebula.getImages()
for image in images:
    print image

