# Hadoop Demonstration: Hands-On Guide (Docker Edition)

A 2-hour, instructor-led walkthrough of **HDFS** (storing data) and **MapReduce** (processing data) on a small Hadoop cluster that runs entirely on your laptop using Docker.

This guide follows the session timetable:

| Section | Topic | Time |
|---|---|---|
| 1 | Setup and Context | 10 min |
| 2 | HDFS Demonstration | 40 min |
| 3 | MapReduce Demonstration | 50 min |
| 4 | Conclusion | 10 min |

Optional appendix at the end: how to run the same thing on AWS.

> **A note on how to use this guide.** Every command comes with a short explanation of *what it does* and *why we run it*. Where I show sample output, treat it as "something like this", since IDs, timestamps and a few numbers will differ on your machine. The numbers that come from our own dataset (file size, block count, word counts) are exact.

---

## Table of Contents

1. [What we are building](#1-what-we-are-building)
2. [Before you start](#2-before-you-start)
3. [Project folder and files](#3-project-folder-and-files)
4. [The datasets](#4-the-datasets)
5. [Section 1: Setup and Context](#5-section-1-setup-and-context-10-min)
6. [Section 2: HDFS Demonstration](#6-section-2-hdfs-demonstration-40-min)
7. [Section 3: MapReduce Demonstration](#7-section-3-mapreduce-demonstration-50-min)
8. [Section 4: Conclusion](#8-section-4-conclusion-10-min)
9. [Cleaning up](#9-cleaning-up)
10. [Troubleshooting](#10-troubleshooting)
11. [Quick command cheat sheet](#11-quick-command-cheat-sheet)
12. [Optional: running this on AWS](#12-optional-running-this-on-aws)

---

## 1. What we are building

Hadoop has two halves, and the demo touches both:

- **HDFS (Hadoop Distributed File System)** is the *storage* layer. It chops big files into **blocks**, spreads them over several machines, and keeps extra **copies (replicas)** so that losing one machine does not lose data.
- **YARN + MapReduce** is the *processing* layer. YARN hands out CPU and memory on the cluster, and MapReduce is the programming model that runs your logic in parallel next to the data.

Our mini cluster is made of four containers:

| Container | Hadoop role | What it does |
|---|---|---|
| `namenode` | HDFS **NameNode** | The "librarian". Keeps the directory tree and knows which blocks make up each file and where they live. Stores *no* file data itself. |
| `datanode` (we will run 3) | HDFS **DataNode** | The "shelves". Stores the actual blocks on disk and reports to the NameNode with regular heartbeats. |
| `resourcemanager` | YARN **ResourceManager** | The "scheduler". Decides which machine runs which task. |
| `nodemanager` | YARN **NodeManager** | The "worker supervisor". Launches and watches task containers on its machine. |

Two web pages let us *see* the cluster:

- NameNode UI: <http://localhost:9870>
- ResourceManager UI: <http://localhost:8088>

**Heads-up on scope:** this is a learning cluster. It shows the concepts and the execution flow. It is not tuned for production.

---

## 2. Before you start

You need:

- **Docker Desktop** (Windows/macOS) or **Docker Engine** (Linux), with the Compose plugin. Check with:
  ```bash
  docker --version
  docker compose version
  ```
- **Python 3** on your host machine, only to generate the sample data. Check with `python3 --version`.
- **About 6 GB of free RAM** to give Docker (Docker Desktop: *Settings → Resources*). The NodeManager asks for a big chunk of memory by default, and if Docker is starved, MapReduce jobs will hang at "ACCEPTED" later.
- **Free ports** `9870` and `8088`.
- Basic Linux command line comfort (`ls`, `cat`, `head`, `mkdir`). That is all.

> **Do all of this before the session.** Pulling the image takes a few minutes, and the session time is better spent on Hadoop itself than on downloads.

---

## 3. Project folder and files

Create a folder and move into it:

```bash
mkdir hadoop-demo && cd hadoop-demo
mkdir data
```

- `mkdir hadoop-demo && cd hadoop-demo` creates the working folder and steps into it. The `&&` means "only do the second command if the first one worked".
- `mkdir data` creates a sub-folder. We will mount it inside the containers so Hadoop can see our files.

You will end up with this layout:

```
hadoop-demo/
├── docker-compose.yml     # describes the 4 services
├── config                 # Hadoop settings, passed in as environment variables
├── generate_data.py       # creates the sample datasets
└── data/
    ├── employees.csv      # created by generate_data.py
    └── sample_text.txt    # created by generate_data.py
```

### 3.1 `docker-compose.yml`

```yaml
services:
  namenode:
    image: apache/hadoop:3
    hostname: namenode
    command: ["hdfs", "namenode"]
    ports:
      - "9870:9870"
    env_file:
      - ./config
    environment:
      ENSURE_NAMENODE_DIR: "/tmp/hadoop-root/dfs/name"
    volumes:
      - ./data:/data

  datanode:
    image: apache/hadoop:3
    command: ["hdfs", "datanode"]
    env_file:
      - ./config

  resourcemanager:
    image: apache/hadoop:3
    hostname: resourcemanager
    command: ["yarn", "resourcemanager"]
    ports:
      - "8088:8088"
    env_file:
      - ./config

  nodemanager:
    image: apache/hadoop:3
    command: ["yarn", "nodemanager"]
    env_file:
      - ./config
```

What the interesting lines mean:

- `image: apache/hadoop:3` is the official Apache Hadoop 3 image. All four services use the same image and only differ in the **command** they run.
- `command: ["hdfs", "namenode"]` starts the NameNode process. The other services start a DataNode, a ResourceManager and a NodeManager the same way.
- `ENSURE_NAMENODE_DIR` tells the image to format the NameNode's storage folder on first start. Formatting is what creates an empty HDFS.
- `ports: "9870:9870"` publishes the NameNode web UI on your laptop (`host:container`). Port `8088` does the same for YARN.
- `volumes: ./data:/data` makes your local `data` folder appear as `/data` inside the NameNode container. This is how our CSV and text files get in.
- The `datanode` service has no hostname and no ports on purpose. That lets us run several copies with `--scale` without name or port clashes.

### 3.2 `config`

This file has no extension. The image turns each line into a setting in the right Hadoop XML file (`CORE-SITE.XML_...` goes into `core-site.xml`, and so on).

```properties
CORE-SITE.XML_fs.default.name=hdfs://namenode
CORE-SITE.XML_fs.defaultFS=hdfs://namenode
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020
HDFS-SITE.XML_dfs.replication=2
MAPRED-SITE.XML_mapreduce.framework.name=yarn
MAPRED-SITE.XML_yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
MAPRED-SITE.XML_mapreduce.map.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
MAPRED-SITE.XML_mapreduce.reduce.env=HADOOP_MAPRED_HOME=$HADOOP_HOME
YARN-SITE.XML_yarn.resourcemanager.hostname=resourcemanager
YARN-SITE.XML_yarn.nodemanager.pmem-check-enabled=false
YARN-SITE.XML_yarn.nodemanager.delete.debug-delay-sec=600
YARN-SITE.XML_yarn.nodemanager.vmem-check-enabled=false
YARN-SITE.XML_yarn.nodemanager.aux-services=mapreduce_shuffle
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-applications=10000
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-am-resource-percent=0.1
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.queues=default
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.capacity=100
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.user-limit-factor=1
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.maximum-capacity=100
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.state=RUNNING
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.acl_submit_applications=*
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.acl_administer_queue=*
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.node-locality-delay=40
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.queue-mappings=
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.queue-mappings-override.enable=false
```

You do not need to memorise this. The lines worth explaining in class are:

| Setting | Meaning |
|---|---|
| `fs.defaultFS=hdfs://namenode` | "When someone says `/data`, they mean HDFS on the machine called `namenode`." |
| `dfs.replication=2` | Keep **2 copies** of every block by default. The Hadoop default is 3, but 2 is enough to see replication on a small cluster. |
| `mapreduce.framework.name=yarn` | Run MapReduce jobs on YARN (not locally inside one process). |
| `yarn.nodemanager.aux-services=mapreduce_shuffle` | Turns on the **shuffle** service, which is how map output travels to reducers. |
| `pmem-check-enabled` / `vmem-check-enabled = false` | Stops YARN from killing containers for memory-accounting reasons that tend to misfire inside Docker. |
| `delete.debug-delay-sec=600` | Keeps task logs and files around for 10 minutes after a job, handy for poking around. |

The `CAPACITY-SCHEDULER` lines just define one simple queue called `default` that can use 100% of the cluster.

### 3.3 `generate_data.py`

This script builds both datasets using only the Python standard library. It is also available as a separate file next to this guide.

```python
#!/usr/bin/env python3
"""
generate_data.py - builds the two sample datasets used in the Hadoop demo.

  data/employees.csv   ~300,000 rows of made-up employee records (HDFS demo + streaming bonus)
  data/sample_text.txt ~60,000 lines of text about data engineering (WordCount demo)

Only the Python standard library is used, so there is nothing to pip install.
A fixed random seed means everyone gets the same files (and the same counts).
"""
import csv
import random

random.seed(42)

# ---------------------------------------------------------------- employees.csv
DEPARTMENTS = {
    "Sales":            ["Sales Executive", "Sales Representative", "Account Manager"],
    "Research":         ["Research Scientist", "Lab Technician", "Research Director"],
    "Engineering":      ["Software Engineer", "Data Engineer", "DevOps Engineer", "QA Engineer"],
    "Human Resources":  ["HR Executive", "Recruiter"],
    "Finance":          ["Financial Analyst", "Accountant"],
    "Marketing":        ["Marketing Specialist", "Content Strategist"],
}
DEPT_WEIGHTS = [26, 14, 30, 6, 12, 12]
CITIES = ["Bengaluru", "Hyderabad", "Pune", "Chennai", "Mumbai", "Delhi", "Kolkata", "Ahmedabad"]

with open("data/employees.csv", "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["employee_id", "age", "gender", "department", "job_role", "city",
                "monthly_income", "years_at_company", "job_satisfaction",
                "overtime", "attrition"])
    for i in range(1, 300_001):
        dept = random.choices(list(DEPARTMENTS), weights=DEPT_WEIGHTS)[0]
        role = random.choice(DEPARTMENTS[dept])
        age = random.randint(21, 60)
        years = min(random.randint(0, 25), age - 20)
        income = int(random.gauss(55000, 18000) + years * 1500)
        income = max(income, 18000)
        overtime = random.choices(["Yes", "No"], weights=[28, 72])[0]
        satisfaction = random.randint(1, 4)
        # attrition is a bit more likely with overtime and low satisfaction
        p = 0.08 + (0.10 if overtime == "Yes" else 0) + (0.07 if satisfaction == 1 else 0)
        attrition = "Yes" if random.random() < p else "No"
        w.writerow([i, age, random.choice(["Male", "Female"]), dept, role,
                    random.choice(CITIES), income, years, satisfaction,
                    overtime, attrition])

# -------------------------------------------------------------- sample_text.txt
WORDS = """hadoop hdfs yarn mapreduce data cluster node block replica namenode datanode
job task map reduce shuffle sort key value record file directory batch pipeline
storage compute scale distributed parallel fault tolerant commodity server disk
network input output split partition counter log schema table query analytics
spark hive kafka warehouse lake ingest transform load stream process engineer
the a of to and in is for with on that by from as at this are be or it""".split()
# first words get picked far more often -> a nice skewed word-count result
weights = [1 / (rank + 3) for rank in range(len(WORDS))]

with open("data/sample_text.txt", "w") as f:
    for _ in range(60_000):
        n = random.randint(8, 18)
        f.write(" ".join(random.choices(WORDS, weights=weights, k=n)) + "\n")

print("Done: data/employees.csv and data/sample_text.txt")
```

---

## 4. The datasets

We use two small, self-made datasets so everyone sees identical numbers and nobody needs a Kaggle login. Generate them from inside the `hadoop-demo` folder:

```bash
python3 generate_data.py
ls -lh data/
```

- `python3 generate_data.py` runs the script and writes the two files into `data/`.
- `ls -lh data/` lists them with human-readable sizes.

You should see roughly:

```
-rw-r--r-- 1 you you  20M  employees.csv
-rw-r--r-- 1 you you 4.8M  sample_text.txt
```

| File | Size | Rows | Used for |
|---|---|---|---|
| `employees.csv` | 19,943,846 bytes (about 19 MB) | 300,000 data rows + 1 header | HDFS commands, blocks and replication, and the bonus job |
| `sample_text.txt` | 5,041,420 bytes (about 4.8 MB) | 60,000 lines | WordCount |

The CSV columns:

```
employee_id, age, gender, department, job_role, city, monthly_income,
years_at_company, job_satisfaction, overtime, attrition
```

Why two files? HDFS does not care what is in a file, so the CSV is great for showing storage behaviour. WordCount, though, is a *text* job, and a plain text file makes its output easy to read.

**Want real data instead?** The session notes suggest these Kaggle datasets. Keep anything you pick under roughly 50 to 100 MB for a demo:

- Employee Attrition Dataset: <https://www.kaggle.com/datasets/personacarved/employee-attrition-dataset>
- Web Server Access Logs (labeled): <https://www.kaggle.com/datasets/mahendradata/web-server-access-logs-labeled>

Drop the downloaded file into `data/` and use its name in place of `employees.csv` below.

---

## 5. Section 1: Setup and Context (10 min)

### 5.1 Talking points (about 5 minutes)

Keep it conversational. The goal is to give learners a reason to care before they see any commands.

- **The problem.** One machine has limited disk, memory and CPU. When data reaches terabytes, either the file does not fit, or reading it takes hours.
- **The Hadoop idea.** Instead of buying one giant machine, use many ordinary ones. Split the data, spread the pieces around, and send the *computation to the data* rather than the data to the computation.
- **Where it fits today.** Hadoop is a **batch** technology: big jobs, run on a schedule, results afterwards. It is not for millisecond responses. Many modern tools (Spark, Hive, cloud data lakes) grew out of Hadoop ideas, so learning the concepts pays off everywhere.
- **What to watch for in the demo.** Pay attention to *execution flow*: where data goes, who does what, and what the logs say. Do not worry about memorising flags.

> **AI tip for the session.** Encourage learners to paste an unfamiliar error or log snippet into an LLM and ask "explain this Hadoop log line in plain English". It is a great way to learn to read logs.

### 5.2 Start the cluster

From the `hadoop-demo` folder:

```bash
docker compose up -d --scale datanode=3
```

Breaking that down:

- `docker compose up` creates and starts every service in `docker-compose.yml`.
- `-d` means "detached": run in the background and give me my terminal back.
- `--scale datanode=3` starts **three** DataNode containers instead of one. With three, replication and block placement become visible.

The first run downloads the image, so it may take a few minutes. Afterwards it starts in seconds.

### 5.3 Confirm that everything is running

```bash
docker compose ps
```

This lists the containers and their state. You want to see `namenode`, `resourcemanager`, `nodemanager` and **three** `datanode` containers, all `running` (or `Up`).

Now the more meaningful check, asking Hadoop itself whether the DataNodes have joined:

```bash
docker compose exec namenode hdfs dfsadmin -report
```

- `docker compose exec namenode` runs a command *inside* the `namenode` container.
- `hdfs dfsadmin -report` asks the NameNode for a cluster summary: capacity, how much is used, and every DataNode it knows about.

Look for a line like `Live datanodes (3)`. If it says 0 or 1, wait 20 to 30 seconds and run it again. DataNodes need a moment to register.

Then check that YARN sees a worker:

```bash
docker compose exec resourcemanager yarn node -list
```

- `yarn node -list` asks the ResourceManager which NodeManagers (workers) are registered.
- You should see one node in state `RUNNING`.

Finally, open the two web pages in your browser and have a quick look together:

- <http://localhost:9870> → the **Overview** page shows capacity and live DataNodes. *Datanodes* in the top menu lists each one.
- <http://localhost:8088> → the YARN page shows the cluster's applications (empty for now) and its resources.

### 5.4 Open a shell inside the cluster

Typing `docker compose exec namenode` before every command gets tiring. Open one interactive shell and stay in it:

```bash
docker compose exec namenode bash
```

- `bash` starts an interactive shell inside the container. Your prompt will change to something like `bash-4.x$`.
- From here on, every command in Sections 2 and 3 is run **inside this shell** unless the text says otherwise.
- Our local `data` folder is available here as `/data`. Quick check:

```bash
ls -lh /data
```

You should see `employees.csv` and `sample_text.txt`.

> If a command fails with "permission denied" while reading `/data`, run `chmod -R a+r data` on your **host** machine and try again.

---

## 6. Section 2: HDFS Demonstration (40 min)

### 6.1 Concepts first (about 8 minutes)

Draw this on a whiteboard or share a diagram:

```
                      ┌──────────────────┐
   hdfs dfs -ls  ───▶ │     NameNode     │   metadata only:
   hdfs dfs -put      │ (the librarian)  │   - folder tree
                      └────────┬─────────┘   - file → blocks
                               │             - block → DataNodes
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌───────────┐    ┌───────────┐    ┌───────────┐
        │ DataNode 1│    │ DataNode 2│    │ DataNode 3│   actual blocks
        │ blk_1 blk_3│   │ blk_1 blk_2│   │ blk_2 blk_3│   (each block stored
        └───────────┘    └───────────┘    └───────────┘    on 2 DataNodes)
```

Explain these four ideas:

1. **Blocks.** A file is split into fixed-size blocks. The default is **128 MB**. A 1 GB file becomes 8 blocks. (The last block of a file is only as big as it needs to be, not padded.)
2. **NameNode.** Holds the *metadata*: which files exist, which blocks belong to each file, and which DataNodes hold each block. It does not hold file contents.
3. **DataNode.** Holds the actual block files on its local disk. It sends the NameNode a **heartbeat** every few seconds and a **block report** from time to time.
4. **Replication.** Every block is copied to several DataNodes (our setting: 2). If one DataNode disappears, the NameNode notices the missing heartbeats, and re-copies the lost blocks from the survivors until the replication target is met again.

Our 19 MB file would be a single block at the default size, which is boring. So during the demo we will **deliberately use a small 4 MB block size** to get several blocks to look at.

### 6.2 Check where we are: `ls`

```bash
hdfs dfs -ls /
```

- `hdfs dfs` is the front door for file commands on HDFS. It works much like the Linux commands you know, but talks to the NameNode instead of your local disk.
- `-ls /` lists the root folder of HDFS.

On a fresh cluster the output is empty. That is expected: HDFS is a *separate* file system from the container's own disk. Compare:

```bash
ls /            # the container's normal Linux folders
hdfs dfs -ls /  # HDFS, which is empty
```

Two different worlds. This is a good moment to pause and let that sink in.

### 6.3 Create folders: `mkdir`

```bash
hdfs dfs -mkdir -p /user/hadoop
hdfs dfs -mkdir -p /user/hadoop/data
```

- `-mkdir` creates a directory in HDFS.
- `-p` means "also create any missing parent folders, and do not complain if it already exists". It is the same flag as in Linux.
- `/user/hadoop` is the HDFS home directory for the user we are logged in as (`hadoop`). Hadoop expects this folder to exist. Without it, some commands and jobs fail in confusing ways.

Check:

```bash
hdfs dfs -ls /user/hadoop
```

You should see the `data` folder listed. The first column (`drwxr-xr-x`) is the permission string, exactly like Linux.

### 6.4 Upload a file: `put`

First a plain upload of the small text file:

```bash
hdfs dfs -put /data/sample_text.txt /user/hadoop/data/
```

- `-put` copies a file **from the local file system into HDFS**.
- The first path (`/data/sample_text.txt`) is the local source. The second (`/user/hadoop/data/`) is the HDFS destination folder.

Now the CSV, this time with a custom block size so we get several blocks:

```bash
hdfs dfs -D dfs.blocksize=4194304 -put /data/employees.csv /user/hadoop/data/
```

- `-D dfs.blocksize=4194304` overrides a Hadoop setting **for this one command**. `4194304` bytes is exactly 4 MB (4 × 1024 × 1024).
- Everything else is the same as before. The override only changes how *this* file is cut up.

Check what we have:

```bash
hdfs dfs -ls -h /user/hadoop/data
```

- `-ls` lists, and `-h` shows sizes in human-readable form (MB instead of raw bytes).
- Look at the **second column** of the output: it shows the replication factor, which should be `2`.

You should see something like:

```
-rw-r--r--   2 hadoop supergroup     19.0 M ... /user/hadoop/data/employees.csv
-rw-r--r--   2 hadoop supergroup      4.8 M ... /user/hadoop/data/sample_text.txt
```

### 6.5 See the blocks: `fsck`

This is the most revealing command in the whole demo.

```bash
hdfs fsck /user/hadoop/data/employees.csv -files -blocks -locations
```

- `fsck` is HDFS's file system check. It is mainly used for health checks, but it is also a great way to look at the layout of a file.
- `-files` prints each file it checks.
- `-blocks` prints the list of blocks that make up the file.
- `-locations` prints which DataNodes hold each block.

Expect output along these lines:

```
/user/hadoop/data/employees.csv 19943846 bytes, replicated: replication=2, 5 block(s):  OK
0. BP-...:blk_1073741830_1006 len=4194304 Live_repl=2 [DatanodeInfoWithStorage[172.x.x.x:9866,...], DatanodeInfoWithStorage[172.x.x.x:9866,...]]
1. BP-...:blk_1073741831_1007 len=4194304 Live_repl=2 [...]
2. ...
3. ...
4. BP-...:blk_1073741834_1010 len=3166630 Live_repl=2 [...]

Status: HEALTHY
 Number of data-nodes:  3
 ...
```

Walk through it with the class:

- **5 blocks.** The file is 19,943,846 bytes. Four full 4 MB blocks use 16,777,216 bytes, and the **last block holds the remaining 3,166,630 bytes**. HDFS does not pad the last block.
- **`Live_repl=2`.** Each block currently has 2 live copies.
- **The IP addresses** show *which* DataNodes hold each block. Different blocks sit on different DataNode pairs. Total block copies stored across the cluster: 5 blocks × 2 = **10**.
- **`Status: HEALTHY`** means no missing or under-replicated blocks.

Now compare with the small file, which fits in a single block:

```bash
hdfs fsck /user/hadoop/data/sample_text.txt -files -blocks
```

It should show **1 block** (it is about 4.8 MB, well under the default 128 MB).

> **Optional visual.** Open <http://localhost:9870> → *Utilities → Browse the file system* → `/user/hadoop/data` → click `employees.csv`. The page shows the same block list and which DataNodes hold each one.

### 6.6 Change replication: `setrep`

```bash
hdfs dfs -setrep -w 3 /user/hadoop/data/employees.csv
```

- `-setrep` changes the replication factor of an existing file. Here, from 2 to 3.
- `-w` makes the command **wait** until the new copies actually exist. You will see it count up and finish.

Prove it:

```bash
hdfs fsck /user/hadoop/data/employees.csv -files -blocks
```

Each block should now show `Live_repl=3`. With 3 DataNodes, every DataNode now holds a copy of every block. Then put it back so the rest of the demo matches the sample output:

```bash
hdfs dfs -setrep -w 2 /user/hadoop/data/employees.csv
```

**Discussion point.** More copies means better safety and more places to read from. It also costs more disk. Replication is a trade-off, not a free lunch.

### 6.7 Read files straight from HDFS: `cat`, `head`, `tail`

```bash
hdfs dfs -cat /user/hadoop/data/employees.csv | head -5
```

- `-cat` streams the file's contents to the terminal.
- `| head -5` is a normal Linux pipe: keep only the first 5 lines and stop. This matters, because without it you would print 300,000 lines.

```bash
hdfs dfs -head /user/hadoop/data/employees.csv
hdfs dfs -tail /user/hadoop/data/employees.csv
```

- `-head` shows the first 1 KB of the file.
- `-tail` shows the last 1 KB. It is handy for checking whether a file was written fully.

**What just happened behind the scenes?** When you ran `-cat`, the client first asked the **NameNode** "where are the blocks of this file?". The NameNode answered with a list of DataNodes. The client then read the data **directly from the DataNodes**. The NameNode never carries file data, which is why it does not become the bottleneck for reads.

### 6.8 Size and space: `du`, `df`, `count`

```bash
hdfs dfs -du -h /user/hadoop/data
```

- `-du` shows the disk usage of each item in a folder. `-h` makes it readable.
- Newer Hadoop versions print two numbers per line. The first is the file size. The second is the **total space used including replicas**, which for our CSV is about twice the file size.

```bash
hdfs dfs -df -h /
```

- `-df` shows the capacity, used and free space of the whole HDFS.

```bash
hdfs dfs -count -h /user/hadoop
```

- `-count` reports the number of directories, files and total bytes under a path.

### 6.9 Copy, move, rename inside HDFS: `cp`, `mv`

```bash
hdfs dfs -mkdir -p /user/hadoop/backup
hdfs dfs -cp /user/hadoop/data/sample_text.txt /user/hadoop/backup/
hdfs dfs -mv /user/hadoop/backup/sample_text.txt /user/hadoop/backup/text_copy.txt
hdfs dfs -ls /user/hadoop/backup
```

- `-cp` copies a file **within HDFS** (no round trip through your local disk).
- `-mv` moves or renames. A rename in HDFS only changes metadata on the NameNode. No block data is moved, which is why it is instant even for huge files.

### 6.10 Download from HDFS: `get`

```bash
hdfs dfs -get /user/hadoop/backup/text_copy.txt /tmp/text_copy.txt
ls -lh /tmp/text_copy.txt
```

- `-get` is the opposite of `-put`: it copies a file **from HDFS to the local file system**. Here, the container's `/tmp` folder.
- `ls -lh` (a normal Linux command) proves the file is now on the local disk.

A handy relative: `-getmerge` stitches many HDFS files into one local file. We will use it with job output in Section 3.

### 6.11 Add data to a file and create an empty one

```bash
echo "bonus line added later" > /tmp/extra.txt
hdfs dfs -appendToFile /tmp/extra.txt /user/hadoop/backup/text_copy.txt
hdfs dfs -tail /user/hadoop/backup/text_copy.txt
hdfs dfs -touchz /user/hadoop/backup/empty.flag
```

- `-appendToFile` adds the contents of a local file to the end of an HDFS file. HDFS files are **write-once, append-only**: you can add to the end, but you cannot edit the middle of a file. That design is part of why HDFS is fast and simple at scale.
- `-touchz` creates a zero-byte file. Jobs often use such "flag files" to signal that something finished.

### 6.12 Delete: `rm`

```bash
hdfs dfs -rm /user/hadoop/backup/empty.flag
hdfs dfs -rm -r /user/hadoop/backup
```

- `-rm` deletes a file.
- `-rm -r` deletes a folder and everything inside it (the `-r` is "recursive").

In this demo setup, deleted files are gone immediately. On many real clusters, the HDFS **trash** feature moves them to a `.Trash` folder first so you can recover them. Mention this as a real-world safety net, and let learners know that `-rm -skipTrash` bypasses it.

### 6.13 Try it: a mini failure experiment (optional, 5 min)

This one makes replication feel real. **Open a second terminal on your host machine** (not inside the container) and stop one DataNode:

```bash
docker compose ps
docker stop <name-of-one-datanode-container>
```

- `docker compose ps` shows the container names. They look like `hadoop-demo-datanode-1`, `-2`, `-3`.
- `docker stop <name>` shuts one down, as if a machine had died.

Now, back in the first terminal:

```bash
hdfs dfs -cat /user/hadoop/data/employees.csv | head -3
```

The read **still works**, because every block had another copy on a DataNode that is alive. By default the NameNode waits about 10 minutes of missed heartbeats before it declares the DataNode dead and starts re-replicating its blocks onto the others. You can watch the NameNode UI (*Datanodes* tab) to see the node's "last contact" counter climbing.

Bring it back when you are done:

```bash
docker start <name-of-the-datanode-container>
```

### 6.14 Section 2 recap

| Command | What it does |
|---|---|
| `hdfs dfs -ls` | List a folder |
| `hdfs dfs -mkdir -p` | Create a folder (and parents) |
| `hdfs dfs -put` | Local → HDFS |
| `hdfs dfs -get` | HDFS → Local |
| `hdfs dfs -cat / -head / -tail` | Read a file from HDFS |
| `hdfs dfs -du / -df / -count` | Space and size information |
| `hdfs dfs -cp / -mv` | Copy / move inside HDFS |
| `hdfs dfs -setrep` | Change how many copies of a file exist |
| `hdfs dfs -rm -r` | Delete |
| `hdfs fsck` | See blocks, replicas and file health |

---

## 7. Section 3: MapReduce Demonstration (50 min)

### 7.1 The model in plain words (about 10 minutes)

MapReduce splits any computation into three steps:

1. **Map.** Each mapper reads one slice of the input (an *input split*, usually one HDFS block) and turns every record into **key/value pairs**.
2. **Shuffle and sort.** Hadoop automatically collects all pairs with the *same key*, sends them to the same reducer, and sorts them by key. You write no code for this step, but it is usually the most expensive part of the job, because it moves data across the network.
3. **Reduce.** A reducer receives each key together with the list of all its values, and combines them into a result.

Use a tiny example on the whiteboard, WordCount on the line `to be or not to be`:

```
INPUT           MAP                         SHUFFLE & SORT          REDUCE
to be or   →   (to,1) (be,1) (or,1)    →   be → [1,1]        →    (be, 2)
not to be  →   (not,1) (to,1) (be,1)   →   not → [1]         →    (not, 1)
                                            or → [1]         →    (or, 1)
                                            to → [1,1]       →    (to, 2)
```

Two helpful extras to mention:

- **Combiner.** An optional "mini-reducer" that runs on the map side before the shuffle. It adds up counts locally so less data crosses the network. The built-in WordCount uses one. We will see it in the counters.
- **Data locality.** YARN tries to run each mapper on a machine that already holds that block, so the data does not have to travel.

### 7.2 Find the built-in examples

Hadoop ships with ready-made example jobs inside a jar file. Let us locate it:

```bash
ls $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar
```

- `$HADOOP_HOME` is an environment variable pointing at the Hadoop install folder (`/opt/hadoop` in this image).
- The `*` is a wildcard that matches the version number, so you do not have to know it.

To save typing, store the path in a variable:

```bash
EXAMPLES=$(ls $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar)
echo $EXAMPLES
```

Now list every example job the jar contains:

```bash
hadoop jar $EXAMPLES
```

- `hadoop jar <file.jar>` runs a Java program packaged as a jar on the cluster.
- With no job name it just prints the list of programs available: `wordcount`, `grep`, `pi`, `terasort` and others.

### 7.3 Run WordCount

```bash
hadoop jar $EXAMPLES wordcount /user/hadoop/data/sample_text.txt /user/hadoop/output/wordcount
```

Piece by piece:

- `hadoop jar $EXAMPLES` runs the examples jar.
- `wordcount` picks which program inside the jar to run.
- `/user/hadoop/data/sample_text.txt` is the **input**, a path in HDFS.
- `/user/hadoop/output/wordcount` is the **output folder**, also in HDFS.

> **Important rule.** The output folder **must not exist** before the job starts. If it does, the job fails immediately with `FileAlreadyExistsException`. Hadoop does this on purpose so that a job can never silently overwrite earlier results. To re-run, delete it first: `hdfs dfs -rm -r /user/hadoop/output/wordcount`.

While the job runs, **switch to <http://localhost:8088>** and refresh. You will see your application go through states, usually `ACCEPTED → RUNNING → FINISHED`. Click it to see its map and reduce progress.

### 7.4 Read the console log (job submission to completion)

The terminal prints a lot. You do not need to understand every line. Here are the parts worth pointing at, in order:

| What you see in the log (paraphrased) | What it means |
|---|---|
| `Connecting to ResourceManager at resourcemanager/...` | The client found YARN and is about to submit. |
| `Total input files to process : 1` | Hadoop found one input file. |
| `number of splits:N` | The input was cut into N **input splits**. Each one becomes one map task. |
| `Submitting tokens for job: job_...` | The job got an ID such as `job_1760000000000_0001`. |
| `The url to track the job: http://resourcemanager:8088/proxy/application_...` | The same job as seen in the YARN web UI. |
| `Running job: job_...` | YARN accepted it and has started an **ApplicationMaster**, the per-job coordinator. |
| `map 0% reduce 0%` → `map 100% reduce 0%` → `map 100% reduce 100%` | Progress through the two phases. |
| `Job job_... completed successfully` | All tasks finished. |

> Our text file is about 4.8 MB, well under the default block size, so you should expect a small number of splits (most likely just 1) and therefore 1 map task. That is not a bug. It shows that the number of mappers follows the size of the input.

### 7.5 Interpret the counters

After success, Hadoop prints **counters**: statistics collected while the job ran. Scroll up in the terminal to find them, or run `hdfs dfs -cat` later and compare. The ones to explain:

| Counter group | Counter | What it tells you |
|---|---|---|
| Job Counters | `Launched map tasks` / `Launched reduce tasks` | How many map and reduce tasks ran. |
| Job Counters | `Data-local map tasks` | Mappers that ran on a node already holding their block (data locality in action). |
| Map-Reduce Framework | `Map input records` | Lines read from the input. For our file this should be **60,000**. |
| Map-Reduce Framework | `Map output records` | Key/value pairs emitted by the mappers. This is the **total number of words**, **779,481**. |
| Map-Reduce Framework | `Combine input records` / `Combine output records` | Records before and after the combiner. Output is far smaller because it adds up counts locally. |
| Map-Reduce Framework | `Reduce input groups` | The number of **distinct keys**. Our file uses **76** different words, so expect 76. |
| Map-Reduce Framework | `Reduce input records` | Records that actually reached the reducers after combining. Much smaller than `Map output records`. |
| Map-Reduce Framework | `Reduce output records` | Lines written to the final output. Again, 76. |
| Map-Reduce Framework | `Shuffled Maps` | How many map outputs were fetched by reducers. |
| File System Counters | `HDFS: Number of bytes read / written` | Total I/O against HDFS. |

A good question to ask the class: *"Map output records is 779,481 but Reduce input records is tiny. Where did all those records go?"* The answer is the combiner.

### 7.6 Where did the output go?

```bash
hdfs dfs -ls /user/hadoop/output/wordcount
```

You should see two files:

```
-rw-r--r--   2 hadoop supergroup          0 ... /user/hadoop/output/wordcount/_SUCCESS
-rw-r--r--   2 hadoop supergroup        ... ... /user/hadoop/output/wordcount/part-r-00000
```

- **`_SUCCESS`** is an empty marker file. Its presence means the job finished cleanly. Downstream jobs often check for it before starting.
- **`part-r-00000`** is the actual result. `part` means "a piece of the output", `r` means it was written by a **reducer** (a map-only job would give `part-m-...`), and `00000` is the reducer number. One reducer, one file. If you used 3 reducers you would get `part-r-00000`, `-00001` and `-00002`.

Look at the result:

```bash
hdfs dfs -cat /user/hadoop/output/wordcount/part-r-00000 | head -20
```

Each line is `word<TAB>count`, sorted alphabetically by word, because the shuffle sorts by key. Sort by *count* using normal Linux tools to see the most common words:

```bash
hdfs dfs -cat /user/hadoop/output/wordcount/part-r-00000 | sort -k2 -n -r | head -10
```

- `sort -k2` sorts on the second column (the count), `-n` makes it numeric, and `-r` puts the biggest first.
- With our seed, `hadoop` comes out on top with **75,681**, then `hdfs` (56,344), `yarn` (45,222), `mapreduce` (38,121) and `data` (32,106).

Double-check the total with one line of arithmetic:

```bash
hdfs dfs -cat /user/hadoop/output/wordcount/part-r-00000 | awk '{s+=$2} END {print s}'
```

- `awk` adds up the second column. The answer should be **779,481**, which matches `Map output records`. The totals agree because nothing was lost, only grouped.

Bring the output down as a single local file:

```bash
hdfs dfs -getmerge /user/hadoop/output/wordcount /tmp/wordcount_result.txt
head /tmp/wordcount_result.txt
```

- `-getmerge` concatenates every file in an HDFS folder into one local file. Very handy when a job leaves many `part-` files.

### 7.7 Show where input and output live

```bash
hdfs dfs -ls -R /user/hadoop
```

- `-R` lists recursively, so you see the whole tree: the `data/` folder holding our inputs, and the `output/` folder holding the results.

The learning point: **MapReduce reads its input from HDFS and writes its output back to HDFS.** Between jobs there is no "memory" to pass results along. The next job simply reads this folder.

### 7.8 Look at the job after the fact

Open <http://localhost:8088> and click the finished application, then the **History** or **Tracking URL** link. You can inspect the map and reduce tasks, their start and finish times, and their counters.

From the command line you can list jobs and applications too:

```bash
yarn application -list -appStates FINISHED
```

- `yarn application -list` shows applications known to YARN.
- `-appStates FINISHED` filters to completed ones. Without a filter you only see the running ones.

To read the logs of a finished application (replace the ID with yours):

```bash
yarn logs -applicationId application_XXXXXXXXXXXXX_0001 | head -50
```

> If this prints something like "log aggregation has not been enabled", that is fine for a demo setup. The task logs are still reachable through the YARN web UI, and `delete.debug-delay-sec` in our config keeps the working files around on the NodeManager for a while.

### 7.9 Experiment: run it again, and a second job

**Run again to hit the "output exists" error** (an instructive failure):

```bash
hadoop jar $EXAMPLES wordcount /user/hadoop/data/sample_text.txt /user/hadoop/output/wordcount
```

It fails within seconds with `FileAlreadyExistsException: Output directory ... already exists`. Ask learners why Hadoop is so strict. (Answer: to protect previous results from being overwritten by accident.)

**Try the `pi` job** to see a purely computational example with almost no data:

```bash
hadoop jar $EXAMPLES pi 4 1000
```

- `pi` estimates π using a statistical method.
- `4` is the number of map tasks. `1000` is the number of samples per map.
- Small numbers mean a rough estimate (like 3.14...). It is meant to show many tasks running in parallel, not accuracy.

### 7.10 Bonus: MapReduce on the employee CSV with Hadoop Streaming

WordCount is nice, but learners often ask "can I use this on a real table?". **Hadoop Streaming** lets any program that reads standard input and writes standard output act as a mapper or reducer. We will use two ordinary Linux commands, so there is no Java to write.

Goal: count employees per department.

```bash
STREAMING=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar)

hadoop jar $STREAMING \
  -input  /user/hadoop/data/employees.csv \
  -output /user/hadoop/output/dept_count \
  -mapper  "cut -d, -f4" \
  -reducer "uniq -c"
```

What each part does:

- `$STREAMING` is the path to the streaming jar. It lives in a different folder from the examples jar.
- `-input` / `-output` are the HDFS input file and the (not yet existing) output folder.
- `-mapper "cut -d, -f4"` runs `cut` on every line. `-d,` says "fields are separated by commas" and `-f4` says "give me the 4th field", which is `department`. So the map step turns each employee row into just the department name (the key).
- `-reducer "uniq -c"` receives the keys already **sorted** by the shuffle, so identical departments sit next to each other. `uniq -c` collapses repeated lines and prefixes each with its count.

View the result:

```bash
hdfs dfs -cat /user/hadoop/output/dept_count/part-*
```

Expected counts, which you can verify against the data:

| Department | Employees |
|---|---|
| Engineering | 89,692 |
| Sales | 78,515 |
| Research | 41,986 |
| Finance | 35,934 |
| Marketing | 35,874 |
| Human Resources | 17,999 |
| `department` (the header row) | 1 |

Yes, the header row is counted as if it were a department. That is a fun teaching moment about **cleaning data in the mapper**: a real job would skip the header line.

> Since the 19 MB file has 5 blocks, you should also see **5 map tasks** in the job counters (`Launched map tasks=5`), one per block. That is the clearest demonstration in this guide of *one split → one map task*.

> **Note.** The exact formatting of `uniq -c` output (leading spaces, tab placement) can vary slightly. The numbers are what matter.

### 7.11 Section 3 recap

- MapReduce = **Map → Shuffle/Sort → Reduce**, all running on YARN.
- Number of **map tasks follows the number of input splits** (roughly, blocks). Number of **reduce tasks** is chosen by the job (the default is 1).
- Input and output both live in **HDFS**. The output folder must not exist beforehand.
- **Counters** are the quickest way to check a job's sanity: did it read the right number of records, and how many keys came out?

---

## 8. Section 4: Conclusion (10 min)

### 8.1 What we covered

- **Why Hadoop:** distributed storage plus batch processing on commodity machines.
- **HDFS:** NameNode keeps metadata, DataNodes keep blocks, replication protects against failure. Files are write-once, append-only.
- **HDFS commands:** `ls`, `mkdir`, `put`, `get`, `cat`, `rm`, `setrep`, `fsck` and friends.
- **MapReduce:** Map, Shuffle/Sort, Reduce. We ran WordCount, read its logs and counters, and located its output in HDFS.

### 8.2 How this connects to modern tools

| Hadoop idea | Where you meet it today |
|---|---|
| HDFS blocks and replication | Cloud object storage (Amazon S3, Azure Data Lake Storage, Google Cloud Storage) handles durability for you. The idea of partitioned files carries over. |
| MapReduce (disk between stages) | **Apache Spark** uses the same split-by-key thinking but keeps intermediate data in memory, so it is usually much faster. |
| Writing Java for every job | **Hive** and **Spark SQL** let you write SQL that is translated into distributed jobs. |
| YARN scheduling | Kubernetes and managed cluster services handle the same "who gets which resources" question. |
| Batch pipelines | The same pattern still powers nightly ETL jobs and data lake builds. |

The commands will change. The vocabulary (partitions, shuffle, replication, locality, fault tolerance) will keep showing up.

### 8.3 Suggested practice after the session

- Re-run WordCount on the CSV file itself. What do the results look like, and why are they less useful?
- Use `-setrep` on `sample_text.txt` and check `fsck` before and after.
- Use the `grep` example job (`hadoop jar $EXAMPLES grep <input> <output> 'regex'`) to count how often `Yes` appears.
- Ask an LLM to explain a block of log output from your own run, then check its explanation against what you learned.

---

## 9. Cleaning up

Leave the container shell first:

```bash
exit
```

Then from the `hadoop-demo` folder on your host:

```bash
docker compose down
```

- `docker compose down` stops and removes the containers and the network. HDFS data lives inside the containers, so it disappears with them, and the next `up` gives you a clean, empty cluster. That is a feature for repeatable demos.

If you also want to free the disk space used by the image:

```bash
docker rmi apache/hadoop:3
```

---

## 10. Troubleshooting

Use the failures as teaching moments. Most of them are about timing or memory.

| Symptom | Likely cause | What to do |
|---|---|---|
| `dfsadmin -report` shows 0 live DataNodes | DataNodes are still starting | Wait 30 seconds and run it again. Check `docker compose logs datanode`. |
| `docker compose up` says a port is already in use | Another program uses 9870 or 8088 | Stop that program, or change the left-hand number in `ports:` (for example `"19870:9870"`). |
| `hdfs dfs -ls` says "No such file or directory" for `/user/hadoop` | The home folder was never created | Run `hdfs dfs -mkdir -p /user/hadoop`. |
| `Name node is in safe mode` | NameNode just started and is waiting for DataNodes to report blocks | Wait a bit. If it persists, `hdfs dfsadmin -safemode get` shows the state. |
| `put: Permission denied` on the local file | The container cannot read your mounted file | On the host: `chmod -R a+r data`. |
| `FileAlreadyExistsException` when running a job | Output folder already exists | `hdfs dfs -rm -r <output-folder>` and run again. |
| Job stuck at `ACCEPTED`, `map 0%` forever | Not enough memory for YARN containers | Give Docker more RAM (6 GB or more), then `docker compose down` and `up` again. |
| `Container ... is running beyond memory limits` | YARN memory checks | Our `config` already disables these checks. Make sure the lines are present and you restarted the cluster. |
| `hadoop: command not found` | You are in your host shell, not inside the container | Run `docker compose exec namenode bash` first. |
| `Cannot connect to the Docker daemon` | Docker is not running | Start Docker Desktop or the Docker service. |
| Browser cannot open `localhost:9870` | Container not up or port not published | `docker compose ps` and check the `ports` mapping. |

**Starting from scratch is always an option:** `docker compose down` followed by `docker compose up -d --scale datanode=3` costs a minute and clears every strange state.

---

## 11. Quick command cheat sheet

```bash
# ---- Cluster (host machine) ----
docker compose up -d --scale datanode=3      # start
docker compose ps                            # status
docker compose exec namenode bash            # shell inside
docker compose down                          # stop and remove

# ---- Health ----
hdfs dfsadmin -report                        # DataNodes, capacity
yarn node -list                              # YARN workers
hdfs fsck / -files -blocks -locations        # block layout

# ---- HDFS basics ----
hdfs dfs -ls [-R] [-h] <path>
hdfs dfs -mkdir -p <path>
hdfs dfs -put <local> <hdfs>
hdfs dfs -get <hdfs> <local>
hdfs dfs -cat <file> | head
hdfs dfs -cp <src> <dst>
hdfs dfs -mv <src> <dst>
hdfs dfs -rm [-r] <path>
hdfs dfs -setrep -w <n> <file>
hdfs dfs -du -h <path>
hdfs dfs -getmerge <hdfs-dir> <local-file>

# ---- MapReduce ----
EXAMPLES=$(ls $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar)
hadoop jar $EXAMPLES wordcount <input> <output>
yarn application -list -appStates FINISHED
yarn logs -applicationId <application_id>
```

---

## 12. Optional: running this on AWS

Everything above runs on a laptop. If you would rather use a cloud machine, there are two sensible routes:

| Route | Best for | Effort |
|---|---|---|
| **A. One EC2 instance running the same Docker setup** | Repeating exactly this guide, nothing new to learn | Low |
| **B. Amazon EMR** | Seeing what a managed, real multi-node Hadoop cluster looks like | Medium |

Both cost real money while they run. Read the [cost safety checklist](#aws-cost-safety-checklist) at the end of this section before you click anything.

> AWS changes its console layout from time to time. Button names below are accurate in spirit, but may move a little. The official AWS documentation is the final authority.

### 12.1 Route A: EC2 + Docker (recommended)

**What you will have at the end:** one Ubuntu virtual machine in AWS running the exact same four-service cluster, which you control over SSH.

#### Step 1: Create an AWS account and sign in

Use an existing account or create one at <https://aws.amazon.com>. You will need a payment method. Sign in to the AWS Management Console and, in the top-right corner, pick a **region** near you (for example *Asia Pacific (Mumbai) ap-south-1*). Keep using the same region for everything.

#### Step 2: Create a key pair (your SSH login)

1. In the console search bar, type **EC2** and open it.
2. In the left menu, choose **Network & Security → Key Pairs → Create key pair**.
3. Name it `hadoop-demo-key`, choose type **RSA** and format **.pem**, then click **Create key pair**.
4. Your browser downloads `hadoop-demo-key.pem`. Keep it safe. AWS will not show it again.

On macOS or Linux, restrict its permissions, because SSH refuses keys that other people can read:

```bash
chmod 400 ~/Downloads/hadoop-demo-key.pem
```

- `chmod 400` makes the file readable only by you.

#### Step 3: Launch the instance

In **EC2 → Instances → Launch instances**:

| Setting | Value | Why |
|---|---|---|
| Name | `hadoop-demo` | Just a label. |
| AMI | **Ubuntu Server 24.04 LTS** (or 22.04 LTS) | A common, well-documented Linux. |
| Instance type | **t3.large** (2 vCPU, 8 GB RAM) | Our four-plus containers need roughly 6 GB. A `t3.medium` (4 GB) is too small and jobs will hang. |
| Key pair | `hadoop-demo-key` | The key you just made. |
| Storage | **30 GiB gp3** | The image and data need room. The default 8 GiB can run out. |
| Network → Security group | Create new, allow **SSH (port 22)** from **My IP** | Only you can connect. Do not use "Anywhere". |

Click **Launch instance**, then wait until **Instance state** shows *Running* and the status checks say *2/2 checks passed*.

#### Step 4: Connect over SSH

Select the instance and copy its **Public IPv4 address**. Then on your own machine:

```bash
ssh -i ~/Downloads/hadoop-demo-key.pem ubuntu@<PUBLIC_IP>
```

- `-i` points to your key file.
- `ubuntu` is the default username on Ubuntu images.
- The first time, SSH asks whether to trust the host. Type `yes`.

(The EC2 console also has a **Connect** button that opens a browser-based terminal if SSH gives you trouble.)

#### Step 5: Install Docker on the instance

Run these inside the SSH session:

```bash
sudo apt-get update
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
```

- `sudo apt-get update` refreshes the package list.
- The `curl ... | sudo sh` line downloads and runs Docker's official install script. It installs Docker Engine and the `docker compose` plugin.
- `usermod -aG docker $USER` lets your user run Docker without `sudo`.

Log out and back in so the group change applies:

```bash
exit
ssh -i ~/Downloads/hadoop-demo-key.pem ubuntu@<PUBLIC_IP>
docker --version
docker compose version
```

Both version commands should print a version number.

#### Step 6: Recreate the project

On the instance:

```bash
mkdir -p hadoop-demo/data && cd hadoop-demo
```

Create the three files exactly as shown in [Section 3](#3-project-folder-and-files). The easiest ways:

- **Copy from your laptop** (run this on your laptop, in the `hadoop-demo` folder):
  ```bash
  scp -i ~/Downloads/hadoop-demo-key.pem docker-compose.yml config generate_data.py ubuntu@<PUBLIC_IP>:~/hadoop-demo/
  ```
  `scp` copies files over SSH. The destination format is `user@host:path`.
- **Or paste them** with an editor on the instance, for example `nano docker-compose.yml`.

Then, on the instance, generate the data (Ubuntu already includes Python 3):

```bash
cd ~/hadoop-demo
python3 generate_data.py
ls -lh data
```

#### Step 7: Start Hadoop and run the demo

```bash
docker compose up -d --scale datanode=3
docker compose exec namenode bash
```

From here, follow Sections 5 to 7 of this guide exactly as written.

#### Step 8: View the web UIs safely (SSH tunnel)

The Hadoop web pages have **no login**. Do not open ports 9870 or 8088 to the internet. Instead, tunnel them through SSH. On your **laptop**, in a new terminal:

```bash
ssh -i ~/Downloads/hadoop-demo-key.pem -L 9870:localhost:9870 -L 8088:localhost:8088 ubuntu@<PUBLIC_IP>
```

- `-L 9870:localhost:9870` means "connect port 9870 on my laptop to port 9870 on the instance, through the encrypted SSH connection".
- While this terminal stays open, <http://localhost:9870> and <http://localhost:8088> in your laptop's browser show the AWS cluster.

#### Step 9: Stop paying when you are done

- **Pausing:** in the EC2 console, select the instance and choose **Instance state → Stop**. You stop paying for compute, but still pay a little for the disk. Note that the public IP will change when you start it again, and HDFS data will be gone after `docker compose down`.
- **Finished for good:** choose **Instance state → Terminate**. This deletes the machine and its disk.

### 12.2 Route B: Amazon EMR (managed Hadoop)

EMR creates and configures a real multi-machine Hadoop cluster for you. There is no Docker involved. It is the closest thing to what companies run, and it makes a nice "what's next" show-and-tell. It costs more than Route A.

#### Step 1: Prepare

You need an AWS account, a region selected, and an **EC2 key pair** (see Route A, Step 2).

#### Step 2: Create the cluster

1. In the console, search for **EMR** and open it.
2. Choose **Create cluster**.
3. Give it a name, like `hadoop-demo`.
4. Under **Applications**, select **Hadoop** only. You do not need Spark, Hive or the rest for this session.
5. Under **Cluster configuration**, a small setup is plenty: **1 primary** and **2 core** nodes using `m5.xlarge` (or the smallest sizes the console suggests).
6. Under **Security and access**, choose your **EC2 key pair**.
7. Leave the default service role and instance profile, or let the console create them.
8. Under **Cluster termination**, turn on **automatic termination after idle time** (for example 1 hour). It is a seat belt against forgetting.
9. Click **Create cluster**.

Wait until the status changes from *Starting* to **Waiting**. This usually takes several minutes.

#### Step 3: Allow SSH to the primary node

Open the cluster's **Properties / Network** section and find the **primary node security group**. Add an inbound rule: **SSH, port 22, source = My IP**. Never use "Anywhere".

#### Step 4: Connect

On the cluster page, copy the **Primary node public DNS**, then:

```bash
ssh -i ~/Downloads/hadoop-demo-key.pem hadoop@<PRIMARY_PUBLIC_DNS>
```

The username on EMR is `hadoop`. You are now on a real cluster's primary node, and `hdfs` and `yarn` commands work immediately.

#### Step 5: Put the data there

On your laptop, copy the datasets over:

```bash
scp -i ~/Downloads/hadoop-demo-key.pem data/employees.csv data/sample_text.txt hadoop@<PRIMARY_PUBLIC_DNS>:~/
```

#### Step 6: Run the commands

The HDFS and MapReduce commands are the same as in Sections 6 and 7, with these differences:

- Use the home-folder paths directly: `hdfs dfs -mkdir -p /user/hadoop/data`, then `hdfs dfs -put ~/employees.csv /user/hadoop/data/` (the local files are in `~/`, not `/data/`).
- The examples jar is usually here:
  ```bash
  EXAMPLES=/usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar
  hadoop jar $EXAMPLES wordcount /user/hadoop/data/sample_text.txt /user/hadoop/output/wordcount
  ```
  If that path does not exist, find it with `ls /usr/lib/hadoop-mapreduce/ | grep examples`.
- The default replication factor on EMR depends on cluster size. The `fsck` output tells you the real number.
- The web UIs on EMR use their own ports and need an SSH tunnel or proxy. The EMR console's *Application user interfaces* tab shows the exact links for your cluster. For this demo, command-line output is enough.

#### Step 7: Terminate

When you finish, select the cluster in the EMR console and choose **Terminate**. If termination protection is on, turn it off first. Check that the status becomes *Terminated*.

### AWS cost safety checklist

- **Set a budget alert.** In the console, open **Billing → Budgets** and create a small monthly budget (for example 5 USD) with an email alert.
- **Stop or terminate the same day.** A `t3.large` costs a few cents an hour, and EMR clusters cost noticeably more. Forgotten machines are the most common source of surprise bills.
- **Check for leftovers.** After terminating, look in EC2 for **Instances**, **Volumes** and **Elastic IPs**, and in EMR for **Clusters**, in the same region.
- **Keep SSH locked to your IP.** Never open Hadoop's web ports to the whole internet.
- **Delete the key pair** from the console and your laptop when you no longer need it.
- Prices differ by region and change over time. Check the AWS pricing pages for the current rates.

---

*End of guide.*
