#!/usr/bin/env python

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

def cloud_login(username, password, url, project, region):
	# Get OpenStack provider
	provider = get_driver(Provider.OPENSTACK)
	conn = provider(username,
			password,
			ex_force_auth_url=url,
			ex_force_auth_version='2.0_password',
			ex_tenant_name=project,
			ex_force_service_region=region)
	return conn
