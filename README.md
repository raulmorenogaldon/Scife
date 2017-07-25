# Scife #

Science For Everyone (Scife) is a framework design easily use scientific software through a web interface.
It is based on NodeJS with a Service Oriented Architecture.
Scife can use different resource providers from Beowulf clusters to Cloud virtual machines.
This is possible through the use of *minions*,
which are independent services that act as interface between Scife and the resource provider.

For example,
we could use a system with 2 possible cloud providers,
using a different minion for both of them.
The overlord can now allocate VMs in any of the two providers using its minions.
Full information about the instances are also retrieved.

Scife aims to be scalable.
If a new provider is added, we only need to add their minion address to the overlord configuration without changing its code.
The main service is the *overlord* or scheduler,
which handles all the workflows of the executions.
The other service is the storage service,
which makes all the I/O-related operations and saves the generated data.

## Configuration files ##

* **start.sh** helper script to launch Scife services.
* **overlord.cfg** contains the main service configuration.
* **storage.cfg** contains the storage service configuration (can be deployed in other machine), needed by the overlord.
* **cluster.cfg** contains the configuration for a HPC cluster-based minion (can be deployed in other machine).
* **openstack.json** contains the configuration for a OpenStack-based minion (can be deployed in other machine).

## Usage ##

In order to deploy Scife it is needed to clone the repository and modify Scife's configuration files.

Git daemon should be installed in the target system because it is needed for the instances to download the application data.
A MongoDB server should be accesible too as it is needed for Scife to work.

Here is an example of the ***overlord.cfg*** configuration file:
```
{
    "OVERLORD_LISTEN_PORT":"4000",         # Specify the port for the Scife external API
    "OVERLORD_USERNAME":"scife",           # Insert the username from which scife is being executed
    "OVERLORD_IP":"192.168.1.10",          # Insert the public IP on which Scife is going to listen for requests.
    "STORAGE_PASSWORD":"PASSWORD",         # Insert the password to login in the storage machine (using the same username as the overlord)
    "MONGO_URL":"mongodb://localhost/db",  # Set the URL where MongoDB server is listening
    "MINION_URL":["tcp://localhost:8238"], # Set the list of minions the overlord will try to use
    "STORAGE_URL":"tcp://localhost:8237",  # Set the URL where the storage service will be listening.
    "AUTOUPDATE":"master"                  # (Experimental) Set the branch for autoupdates
}
```

In the case of the storage service the file ***storage.cfg*** is used:
```
{
    "appstorage": "/path/to/appstorage",       # Specify the file system path where all the application data will be stored
	"inputstorage": "/path/to/inputstorage",   # Specify the file system path where all the input files will be stored
	"outputstorage": "/path/to/outputstorage", # Specify the file system path where all the output files will be stored
	"public_url": "192.168.1.10",              # Specify the external IP on which the storage service will be listening
	"git_port": "9418",                        # Specify the port on which the Git daemon is listening (in the local machine)
	"username": "scife",                       # Specify the username on which the storage service is executing
	"listen": "tcp://0.0.0.0:8237",            # Specify the listening direction of the storage service
	"db": "mongodb://localhost:27017/db"       # Specify the URL on which MongoDB is listening
}
```

Review the ***start.sh*** script for an example of usage.

## Sources ##
This project is composed by the next components:

* Overlord: Acts as a scheduler for diferent cloud providers. All orchestration, provisioning and workflow is done here.
    * Scheduler: Handles all workflow related logic, using the other modules.
    * Storage: Handles all I/O related operations.
    * Instance: Handles all minion instances metadata, creates them and sends commands to the minions.
    * Application: Handles all applications metadata.
    * Experiment: Handles all experiments metadata, which are instances of the applications.
    * Execution: Handles all executions metadata, which are instances of the experiments.
    * Database: Provides the interface to MongoDB for the other components.
    * Task: Provides a task system where tasks are saved to the database, allowing their tracking even if the system is reset.
    * Users: Provides a user system and their permissions.
    * Utils: Provides general utilities for the other modules like SSH connections or UUID generation.
    * **Routes**: In this folder the HTTP API is defined.
* Minions: Acts as independent services that offers a uniform interface for the scheduler.

## Required packages ##

To install required packages for *NodeJS*, just execute `npm install` command in the folder location.