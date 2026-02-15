# Raft Consensus Algorithm

This project is an implementation of the **Raft distributed consensus algorithm** in Go. Raft is a fault-tolerant protocol used to manage replicated logs in distributed systems and ensures that multiple nodes agree on the same sequence of operations, even in the presence of failures.

Raft simplifies consensus by dividing the problem into three main parts:
- Leader election  
- Log replication  
- Safety and consistency  

---

## 📌 Features

- Leader election mechanism  
- Log replication between nodes  
- Commit index tracking  
- Fault-tolerant design  
- Simple architecture for educational purposes  

---

## 🧠 What is Raft?

Raft is a consensus algorithm designed to be understandable and practical. It allows a cluster of servers to function as a single reliable system. Each server can be in one of three states:

- **Leader** – handles client requests and replicates logs  
- **Follower** – responds to requests from leaders and candidates  
- **Candidate** – runs for leadership during elections  

Raft ensures:
- Safety (no conflicting logs)
- Liveness (system continues to operate if a majority is available)

---

## 🚀 Getting Started

### Prerequisites

- Go (version 1.18 or later)

---

### Installation

Clone the repository:

```bash
git clone https://github.com/azhgh22/Raft-Consensus-Algorithm.git
cd Raft-Consensus-Algorithm
