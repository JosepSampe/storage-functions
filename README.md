<p align="center"> <img src="docs/img/zion.png" width="200"></p>

Zion is a Serverless Computing Framework for OpenStack Swift, allowing to run storage functions directly on the data.

Zion aims to solve the scalability and resource contention problems of active storage, while benefiting from data locality to reduce latency by placing computations close to the data. Our model is data-driven and not event-driven, because our computations are located in the data pipeline, and intercept the data flows that arrive and return from the object store.


## Architecture
![Architecture](docs/img/architecture.png?raw=true "Architecture")

### Interception Software and Metadata Service
We built a new Swift interception middleware for Zion to accomplish two primary tasks: 1. The management of function code deployment and libraries, including the triggers that cause the functions to be run; and 2. Redirection of requests and responses through the computation layer when they need to be processed by any function

### Computation layer

In Zion, the computation layer is composed by a pool of compute nodes. They are located between the proxies and the storage nodes, and they are all managed by the *Zion Service Manager*.

Each function is run inside a separate Docker container, what is called a *worker*. Zion’s runtime is Java-based, and consequently, every function is run in a Java Virtual Machine (JVM). At a very high level, a worker can be viewed as a container running a specific function. Every new invocation to the function is handled by a new thread inside the JVM of a worker. This means that a single worker can handle more than one request at a time. By default, each worker uses 1 exclusive CPU. The  *Zion Service Manager* is responsible to monitor function workers and scale them up and down depending of the CPU usage.

![Compute Node](docs/img/compute_node.png?raw=true "Compute Node")


## Installation

### All-In-One Machine
For testing purposes, it is possible to install an All-In-One (AiO) machine with all the Zion components and requirements.
We prepared a script for automating this task. The requirements of the machine are a clean installation of **Ubuntu Server 20.04**, **2CPU Cores**, at least **2GB** of RAM, and a **fixed IP address**. It is preferable to upgrade the system to the latest versions of the packages with `apt update && apt dist-upgrade` before starting the installation, and set the server name as `controller` in the `/etc/hostname` file. Then, download the `aio_u20_ussuri.sh` script and run it as sudo:

```bash
curl -fsSL https://git.io/JJq4t | sudo bash /dev/stdin install
```

The script first installs Keystone, Swift and Horizon (Ussuri release), then it proceeds to install the Zion framework package. Note that the script uses weak passwords for the installed services. If you want more secure services, please change them at the top of the script.

By default, the script has low verbosity. To see the full installation log, run the following command in another terminal:

```bash
tail -f /tmp/zion_aio_installation.log
```

The script takes long to complete (~10 minutes) (it depends of the network connection). Once completed, you can access to the dashboard by typing the following URL in the web browser: `http://<node-ip>/horizon`.

If you already ran the installation script, you can update the Zion framework from this repository by the following command:

```bash
curl -fsSL https://git.io/JJq4t | sudo bash /dev/stdin update
```
