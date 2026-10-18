Version 1 setup, design, and benchmark interpretation: see [RUNNING.md](RUNNING.md).

---

Objective: The aim of this assignment is to provide students with hands-on experience in designing and implementing a basic cloud computing application. By creating a simple single-server key-value store, students will understand the fundamental operations underpinning more complex distributed storage systems.

Description:

You are tasked with designing a single-server key-value store that supports the following operations:

GET: Retrieve the value associated with a given key.
PUT: Store a value associated with a given key.
DEL: Remove a key and its associated value from the store.
Requirements:

Server Implementation:

Your server should run as a standalone application, listening on a specific port for incoming client requests.
You can use any programming language or framework of your choice, but your implementation details and choice should be justified in your report.
Concurrent Requests:

The server should be able to handle multiple concurrent requests. Implement appropriate mechanisms to handle concurrent PUT or DEL operations on the same key.
Persistence:

While in-memory storage can be used, implementing a persistence mechanism (e.g., periodically saving data to disk) will earn additional points.
Error Handling:

The server should be able to handle errors gracefully. For instance, if a client tries to GET a non-existent key, the server should return an appropriate error message.
Logging:

Implement a logging mechanism to record all operations (GET, PUT, DEL) with timestamps.
Submission:

Source Code: Preferred way is to share link to GitHub repository - (share with username: chalianwar). For extreme cases, you can share all source code files, along with instructions on how to compile and run the server via e-mail.

Report: A brief report (1 page) that covers:

Design decisions and justification.
Challenges faced during implementation and how they were overcome.
Any assumptions made.
Potential improvements and features for future versions.
Demo: Be prepared to provide a short demonstration of your key-value store.