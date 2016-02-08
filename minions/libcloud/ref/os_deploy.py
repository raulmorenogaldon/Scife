#!/usr/bin/env python

import os
import traceback
from libcloud.compute.deployment import MultiStepDeployment
from libcloud.compute.deployment import ScriptDeployment, SSHKeyDeployment
from libcloud.compute.base import NodeAuthSSHKey, NodeAuthPassword
from libcloud.compute.types import DeploymentException

# Add one public key for SSH
def template_key_add(template, key):
	str = template
	idx = str.find("[[[SSH_KEYS]]]")	
	if idx < len(template):
		# Found
		str = str.replace("[[[SSH_KEYS]]]", "- "+key)

	return str

# Deploy CESM in cloud
def cloud_deploy_cesm(conn, image, flavor, input_volume):
	print "Deploying CESM with image ID: ", image.id, " - ", image.name
	print "Size: ", flavor.name

	# List key pair
	print "Key pairs available:"
	keys = conn.list_key_pairs()
	if len(keys) == 0:
		print "No keys available!"
		exit(0)

	i = 0
	for key in keys:
		print("["+str(i)+"] "+key.name)
		i+=1

	try:
		selected_key = int(input("Select image number: "))
		# Get key pair
		key_pair = keys[selected_key]
		print "Selected: ", key_pair.name
	except (IndexError, SyntaxError, NameError):
		print "Invalid selection!"
		exit(1)

	# Setup initialization steps	
	template = open('/home/devstack/cloud-cesm/cloud-config-manager.template', "r").read()
	SCRIPT = template_key_add(template, key_pair.public_key)
	print SCRIPT

	# Create node
	#print conn.features['create_node']
	try:
		node = conn.create_node(name="CESM Manager", image=image, size=flavor, ssh_username='centos', ex_keyname=key_pair.name, ex_userdata=SCRIPT, ex_config_drive=True)
		print "Created node: ", node.id
	except DeploymentException as e:
		e.node.destroy()
		print "Failed to create node!"
		print traceback.print_exc()

	# Wait till node is running
	conn.wait_until_running([node])

	# Get volume
	if (conn.attach_volume(node, input_volume, "/dev/vdb") == False) :
		print "Failed to attach input data volume"
		exit(1)

	# Configure node

	#node.destroy()
	

	return
