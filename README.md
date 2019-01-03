# Cloud-Computing-Concepts-Part-2
Coursera course : https://www.coursera.org/specializations/cloud-computing
# What is the project?
*	Building a **Fault-Tolerant Key-Value Store**.

*	The main functionalities of the Key-Value Store :
	*	**CRUD operations :** A key-value store supporting CRUD operations (Create, Read, Update, Delete).
	*	**Load-balancing :** via a consistent hashing ring to hash both servers and keys.
	*	**Fault-tolerance up to two failures :** by replicating each key three times to three successive nodes in the ring, 	    starting from the first node at or to the clockwise of the hashed key.
	*	**Quorum consistency level** for both reads and writes (at least two replicas).
	*	**Stabilization :** after failure (recreate three replicas after failure).
      
# Principle of **Fault-Tolerant Key-Value Store** : 
![image](https://github.com/kevin85421/Cloud-Computing-Concepts-Part-2/blob/master/kvstore.png)

# How do I run the Grader on my computer ?
*	There is a grader script KVStoreGrader.sh. The tests include:
      * **Basic CRUD** tests that test if three replicas respond
      * **Single failure** followed immediately by operations which should succeed (as quorum can still be
reached with 1 failure)
      * **Multiple failures** followed immediately by operations which should fail as quorum cannot be
reached
      * Failures followed by a time for the system to re-stabilize, followed by operations that should
succeed because the key has been re-replicated again at three nodes.
```
	$ chmod +x KVStoreGrader.sh
	$ ./KVStoreGrader.sh
```
# Result
*	Points achieved: 90 out of 90 [100%]
	
