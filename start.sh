#!/bin/bash

echo "#############################################"

# Parameters
NODE=v5.5.0
BRANCH=develop

# IDs
UUID_OVERLORD="overlord"
OVERLORD="overlord/app.js ./overlord.cfg"

UUID_STORAGE="storage"
STORAGE="storage/storage.js ./storage.cfg"

# Update repository
echo "Updating repository..."
git pull origin $BRANCH >/dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Repository update failed!"
fi

# Load NodeJS
echo "Loading NodeJS..."
hash node
if [ $? -ne 0 ]; then
	nvm use $NODE
fi

# Check if loaded correctly
hash node
if [ $? -ne 0 ]; then
	echo "NodeJS not found!"
	exit 1
fi

# Update packages
echo "Installing NodeJS packages..."
npm install >/dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Package updating failed!"
fi

# Launch processes
echo "Launching SciFE..."
forever restart "$UUID_STORAGE"  || forever start --uid "$UUID_STORAGE"  $STORAGE
forever restart "$UUID_OVERLORD" || forever start --uid "$UUID_OVERLORD" $OVERLORD

echo "DONE!"
echo "#############################################"
