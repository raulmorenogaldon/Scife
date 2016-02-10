#!/usr/bin/env python
# -*- coding: utf-8 -*-

#require oca package, you can install from pip with the command "sudo pip install oca"

import oca

client = oca.Client("oneadmin:35a16a51404255ba4a4b0b66ea9ec5d4", 'http://vesuvius.i3a.uclm.es:2633/RPC2')

if not client:
	print "Error al efectuar la conexión"
	exit(-1)

host_pool = oca.HostPool(client)
print "***** Hosts *****"
print host_pool.info()
for host in host_pool:
    print host.name
    
img_pool = oca.ImagePool(client)
print "\n***** Images *****"
print img_pool.info()
for img in img_pool:
	print img.name

vn_pool = oca.VirtualNetworkPool(client)
print "\n***** Virtual Networks *****"
#print vn_pool.info() #Da error
for vn in vn_pool:
	print vn.name
	
user_pool = oca.UserPool(client)
print "\n***** Users *****"
#print user_pool.info() #Da error
for user in user_pool:
	print user.name
	
group_pool = oca.GroupPool(client)
print "\n***** Groups *****"
#print group_pool.info() #Da error
for group in group_pool:
	print group.name

vmTemplate_pool = oca.VmTemplatePool(client)
print "\n***** Virtual Machine Templates *****"
print vmTemplate_pool.info()
for vmTemplate in vmTemplate_pool:
	print vmTemplate.name
		
vm_pool = oca.VirtualMachinePool(client)
print "\n***** Virtual Machines *****"
print vm_pool.info()
for vm in vm_pool:
    print "%s (memory: %s MB)" % ( vm.name, vm.template.memory)
