Dockerizing the Key-Value Store Application

Objective:
The main objective of this assignment is to encapsulate the key-value store server application in a Docker container. This will ensure that the application runs consistently across different environments.

Prerequisites:

Basic understanding of Docker.
Familiarity with writing and understanding Dockerfiles.
A working key-value store application (provided in the previous assignment).
Requirements:

Base Image: Use an appropriate base image, preferably a lightweight Linux distribution (like Alpine) with Python installed.

Application Files: Ensure the key-value store application's source files are added to the Docker image.

Dependencies: If the application has any external dependencies (e.g., libraries), ensure they're installed within the Docker image.

Exposure: The key-value store server listens on a specific port. Ensure this port is exposed so that clients can connect to it when the container is running.

Entry Point: Define a command or entry point so that the server application starts automatically when the Docker container is launched.

Steps:

Create a directory for your Docker context. Place your key-value store
Important Note: This assignment may help you in your coding assignment 2.