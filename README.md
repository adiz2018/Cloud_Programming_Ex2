# Cloud_Programming_Ex2
Cloud programming course - assignment 2

The repository contains the following files:
1. aws_utils.py - contains a class that deals with everything that we needed for creating AWS EC2 instances and handling IAM roles,policies, key-pairs and security groups.
2. endpoint.py - contains the endpoint class and the server that recieves requests on port 5000.
3. worker.py - contains the worker functions and is executed on the workers.
4. tasks.py - contains a class that defines the system jobs attributes.
5. main.py - the script that starts the system and it should be executed first. The script starts the 2 endpoints and connects them to eachother.
6. requirements.txt - a file that contains the python libraries that need be installed on the endpoints and workers.
7. endpoint_setup.sh - the bash script that will be executed on the endpoint instances when they are created.
8. worker_setup.sh - the bash script that will be executed on the worker instances when they are created.
9. file.txt - an example file that can be used to send the enqueue request to the endpoints.
10. failure_modes.pdf - A file detailing failure modes and how to deal with them if this was a real-world project.

## How to run
You should execute the file "main.py" to start the system.

