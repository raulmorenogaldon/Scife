# MultiCloud

This project consists in two parts:

- **Overlord**: Using Node.js, act as a scheduler for diferent cloud providers.
- **Minions**: Using python, act as an independent services that offers a uniform interface for the overlord.

Example
A system with 2 possible cloud providers. For every provider we use a minion. Overlord can now allocate VMs in any of the two providers using its minion. Also, it can retrieve full information about the instances.

A new provider is added. In this case we only need to report this to the overlord without changing its code. (TODO:¿config file, environment vars...?)

To install required packages for nodejs, only run `npm install` code in the *overlord* folder.
