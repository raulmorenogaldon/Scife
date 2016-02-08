#!/usr/bin/env python

import sys
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from login import *
from deploy import *

auth_username = 'admin'
auth_password = 'devstack'
auth_url = 'http://localhost:5000'
project_name = 'admin'
region_name = 'RegionOne'

# Get OpenStack connection
conn = cloud_login(auth_username, auth_password, auth_url, project_name, region_name)

# Select location
print "======================================"
print "Available locations:"
locations = conn.list_locations()
if len(locations) == 0:
	print "No locations available!"
	exit(0)

i = 0
for location in locations:
	print("["+str(i)+"] "+location.name)
	i+=1

try:
	selected_loc = int(input("Select location number: "))
	print "Selected: ", locations[selected_loc].name
except (IndexError, SyntaxError, NameError):
	print "Invalid selection!"
	exit(1)

# Get available images
print "======================================"
print "Available images:"
images = conn.list_images()
if len(images) == 0:
	print "No images available!"
	exit(0)

i = 0
for image in images:
	print("["+str(i)+"] "+image.name)
	i+=1

try:
	selected_img = int(input("Select image number: "))
	print "Selected: ", images[selected_img].name
except (IndexError, SyntaxError, NameError):
	print "Invalid selection!"
	exit(1)

# Get available flavors
print "======================================"
print "Available sizes:"
flavors = conn.list_sizes()
if len(flavors) == 0:
	print "No sizes available!"
	exit(0)

i = 0
for flavor in flavors:
	print("["+str(i)+"] "+flavor.name)
	i+=1

try:
	selected_flv = int(input("Select flavor number: "))
	print "Selected: ", flavors[selected_flv].name
except (IndexError, SyntaxError, NameError):
	print "Invalid selection!"
	exit(1)

# List volumes
print "======================================"
print "Volumes:"
volumes = conn.list_volumes()

# No volumes available
if len(volumes) == 0:
	print "No volumes!"
	try:
		volume_size = int(input("Select volume size in GB: "))
	except (SyntaxError, NameError):
		print "Invalid volume size!"
		exit(1)
	print "Creating volume with "+str(volume_size)+" GBs"
	volumes = [conn.create_volume(size=volume_size, name="Inputdata", location="nova", ex_volume_type="lvmdriver-1")]
	selected_vol = 0

# Select available volume
else :
	i = 0
	for volume in volumes:
		print("["+str(i)+"] "+volume.name)
		i+=1

	# Select volume
	try:
		selected_vol = int(input("Select volume: "))
	except (SyntaxError, NameError):
		print "Invalid selection!"
		exit(1)
	print "Selected "+volumes[selected_vol].name

# Call deployment function
print "======================================"
print "CESM Deployment"
cloud_deploy_cesm(conn, images[selected_img], flavors[selected_flv], volumes[selected_vol])
