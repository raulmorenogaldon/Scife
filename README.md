# MultiCloud

This project consists in two parts:

- **Overlord**: Using NodeJS, act as a scheduler for diferent cloud providers.
- **Minions**: Using NodeJS, act as independent services that offers a uniform interface for the scheduler.

Example:
A system with 2 possible cloud providers. For each provider we use a minion. The overlord can now allocate VMs in any of the two providers using its minions. Also, it can retrieve full information about the instances.

If a new provider is added, we only need to report this to the overlord without changing its code.

### Required packages

NodeJS packages:
To install required packages for *NodeJS*, just execute `npm install` command in the folder location.