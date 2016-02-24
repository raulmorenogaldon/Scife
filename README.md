# MultiCloud

This project consists in two parts:

- **Overlord**: Using Node.js, act as a scheduler for diferent cloud providers.
- **Minions**: Using python, act as an independent services that offers a uniform interface for the overlord.

Example
A system with 2 possible cloud providers. For every provider we use a minion. Overlord can now allocate VMs in any of the two providers using its minion. Also, it can retrieve full information about the instances.

A new provider is added. In this case we only need to report this to the overlord without changing its code.

### Required packages

Python packages:

* *python-dev*: `sudo apt-get install python-dev`
* *setuptools*: `sudo apt-get install python-setuptools`
* *numpy*: `sudo apt-get install python-numpy`
* *pip*: `sudo apt-get install python-pip`
* *zerorpc*: `sudo pip install zerorpc`
* *[libcloud](https*://libcloud.apache.org/)*: `pip install apache-libcloud`
* *libevent-dev*: `apt-get install libevent`
* *libzmq3*: `apt-get install libzmq3 libzmq3-dev`
* *greenlet*: `pip install greenlet`
* *gevent*: `pip install gevent`

Node.js packages:
To install required packages for *nodejs*, only run `npm install` code in the folder location. Optionaly, you can install Yeoman with she folow command: `sudo npm install -g yo`