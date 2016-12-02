#!/bin/bash

# Parameters
NODE=v5.5.0
BRANCH=develop

OVERLORD=overlord/app.js ./overlord.cfg
STORAGE=storage/storage.js ./storage.cfg

# Update repository
git pull origin $BRANCH

# Load NodeJS
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
npm install
npm install forever

# Launch processes
forever restart $STORAGE || forever start $STORAGE
forever restart $OVERLORD || forever start $OVERLORD

echo "Success!"
