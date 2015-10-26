#!/usr/bin/sh

# Error check
set -e

echo "Hostname: ${NODE_HOSTNAME} - ${NODE_IP}"

# Update system
echo "-------------------------------------------------------"
echo "Installing missing libs..."
yum install -y rpcbind nfs-utils

# Disable firewall
echo "-------------------------------------------------------"
echo "Disabling firewall..."
systemctl disable firewalld
systemctl stop firewalld

# Set hostname
echo "-------------------------------------------------------"
echo "Configuring hostname and hosts file..."
echo "${NODE_HOSTNAME}" > /etc/hostname
echo "127.0.0.1 localhost ${NODE_HOSTNAME}" > /etc/hosts

# Set fstab
echo "-------------------------------------------------------"
echo "Configuring FSTAB..."
echo "/dev/vdb1	/home	xfs	defaults	0 0" >> /etc/fstab

# Mount home
echo "-------------------------------------------------------"
echo "Mounting home..."
mount /home

# Setup exports for nfs
echo "-------------------------------------------------------"
echo "Exporting home..."
echo "/home	*(rw,sync,no_subtree_check,no_root_squash)" > /etc/exports
exportfs -a

# Start nfs
echo "-------------------------------------------------------"
echo "Starting nfs..."
systemctl enable rpcbind
systemctl start rpcbind
systemctl enable nfs-server
systemctl start nfs

# Create hostfile
echo "-------------------------------------------------------"
echo "Creating hostfile for MPI..."
rm -f /home/cesm/hostfile
touch /home/cesm/hostfile
chown cesm:cesm /home/cesm/hostfile

# Create hostfile
echo "-------------------------------------------------------"
echo "Disabling SSH host key verification..."
echo """
Host *
	StrictHostKeyChecking no
	UserKnownHostsFile=/dev/null
""" > /home/cesm/.ssh/config
chmod 644 /home/cesm/.ssh/config

echo "-------------------------------------------------------"
echo "DONE!"
