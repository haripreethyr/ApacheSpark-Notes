**Spark - Catalyst Optimizer, DAGs, DAG Scheduler

When you run an Action (like `.count()` or `.show()`), Spark turns your high-level code into physical CPU tasks through a multi-step process.
In Apache Spark, the Catalyst Optimizer and the DAG (Directed Acyclic Graph) work together like a brain and a roadmap.

The Catalyst Optimizer builds and perfects the plan, while the DAG sequences that plan into physical steps for your cluster to execute.
#### STEP-1: THE CATALYST OPTIMIZER (THE BRAIN):
The <mark style="background: #D2B3FFA6;">Spark's built-in query optimizer engine</mark>. It turns your Python/Scala code and rewrites it as a logical plan behind the scenes to make it run as fast as possible.
It acts like a smart GPS that finds a shorter route to your destination before you even start driving. It optimizes your query using four major phases:
	- **Analysis:** It checks your code for typos, validates table or column names, and ensures the data types match up.
	- **Logical Optimization:** It applies standard Optimization rules to your code. For example:
		- **Predicate Pushdown:** If you write a filter like `.filter("age > 30")` at the very end of your script, catalyst moves (pushes) that filers directly to the file-reading step. This ensures Spark only reads the rows you need, saving massive amounts of memory.
		- **Project Pruning / Column Pruning:** If your CSV has 100 columns but you only use 2, Catalyst ensures Spark drops the other 98 columns immediately during the read phase.
	- **Physical Planning:** It generates multiple physical strategies to actually move the data (e.g., deciding whether to use a _Shuffle Hash Join or a Broadcast Hash Join_) and picks the cheapest one based on cost using a cost model.
	- **Code Generation:** It compiles your complex expression into clean, highly optimized Java bytecode (JVM code) that runs natively on your cluster's CPU cores.
    - **RDD:** is fundamentally a distributed collection of data divided into partitions, with information about how those partitions depend on other partitions.

<img width="1580" height="435" alt="The Catalyst Optimizer" src="https://github.com/user-attachments/assets/6244c830-9eda-4c14-8da3-e970100bb15b" />

<mark style="background: #FFB86CA6;">You describe what you want; Catalyst decides how to get it efficiently.</mark>

#### STEP-2: DAG (DIRECTED ACYCLIC GRAPH) - THE ROADMAP: The execution graph
- Spark converts the physical plan into a DAG(Directed Acyclic Graph) - a step-by-step flowchart of operations where arrows only go forward, never backward. Once the Catalyst Optimizer  decides on the best physical plan, Spark converts that plan into a DAG(Directed Acyclic Graph) and the DAG scheduler ( a separate driver component) takes over. It breaks down the job into stages and tasks for parallel execution across a cluster.
	**Directed:** It has a clear direction of data flow — Step-by-Step lineage of your data. (Step A -> Step B -> Step C).
	**Acyclic:** It has no loops. Data cannot go backward to a previous step.
	**Graph:** It is a visual arrangement of interconnected nodes.
- The DAG is essentially Spark's master blueprint. It maps out exactly how data will flow from the source file, through various transformations, and down to the final action.
- It represents what needs to be done.
- DAG represents dependencies between computational stages/RDDs rather than merely listing transformations in sequence.
- It holds no processing power. It represents dependencies that the scheduler uses to determine how execution should be organized.


>**SUMMARY:**
>- **_Catalyst Optimizer_** figures out WHAT the most efficient way to run your code is.
>- **DAG** figures out HOW to break that efficient plan down into stages and tasks across the worker nodes/servers.


#### STEP-3: THE DAG SCHEDULER — THE OPERATOR/EXECUTIONER:
- Think of the DAG as the map, and the DAG scheduler as the driver who uses that map to navigate the route.
- The DAG scheduler is an active component inside the Spark Driver node. It is the engine that actually reads the static DAG and turns it into action.
- When you call an action like `.show()`, the DAG Scheduler wakes up and performs these critical steps:
	- **Examines the DAG:**  It looks at the entire graph of transformations.
	- **Finds Shuffle Boundaries:** It looks for wide transformations (like `groupBy`) to figure out where data must be shuffled over the network.
	- **Splits the DAG into Stages:** It cuts the DAG at those shuffle points, breaking the large graph into manageable, sequential stages.
	- **Hands off to the task scheduler:** Once it determines the stages, it submits them to the next component (the Task Scheduler) to launch the actual physical tasks on the worker machines.
	- **Handles Failures:** If an entire stage fails because a worker node died, the DAG Scheduler looks at the DAG map to figure out which missing partitions need to be recomputed.

>**How the DAG manages execution?:**
	1. **Breaking things into Stages:** The DAG looks at your code and identifies where Shuffles (data movements across the network) happen. It cuts the blueprint at those shuffle boundaries to create Stages.
	2. **Generating Tasks:** For each stage, Spark creates a task for each relevant partition. The DAG Scheduler submits these tasks as a TaskSet to the Task Scheduler, which schedules them on executor nodes.
	3. **Fault Tolerance:** Because Spark maintains lineage information, lost partitions can be recomputed from their parent data. The DAG Scheduler can resubmit the necessary tasks/stages to recover missing data.

<img width="980" height="790" alt="DAG Scheduler" src="https://github.com/user-attachments/assets/31523868-b1df-462f-a5af-eb06f0ce5618" />

The DAG scheduler's job is to look at this graph and answer: "where do I have to cut this into stages?" It walks the graph, and every time it hits a wide dependency (a shuffle boundary), it cuts a new stage. Narrow dependencies get fused together into the same stage, so they can run in one pipelined pass without materializing intermediate results.

| Feature         | The DAG                                | The DAG Scheduler                                  |
| --------------- | -------------------------------------- | -------------------------------------------------- |
| **What is it?** | A logical structure (Graph).           | A software component (Engine).                     |
| **Role**        | The blueprint of data transformations. | The manager that splits the blueprint into stages. |
| **State**       | Passive and static.                    | Active and dynamic.                                |


#### STEP-4: TASK SCHEDULER - THE DISPATCHER
- The task Scheduler is a component of the Spark Driver responsible for scheduling individual tasks on the available executor resources. 
- Think of it this way:
> **DAG Scheduler decides the stages and creates tasks.
> Task Scheduler decides where and when the individual tasks in those stages should run.**

**What happens after the DAG Scheduler creates stages?**
Suppose we have:
```
Stage 0
Read --> Filter
	|
	|
Shuffle
	|
	|
Stage 1
groupBy --> count
```
Imagine Stage 1 has 4 partitions. Spark needs 4 tasks to process those 4 partitions

Stage 1
Task 0 --> Partition 0
Task 1 --> Partition 1
Task 2 --> Partition 2
Task 3 --> Partition 3

The DAG scheduler determines the stage and creates the task set. The Task Scheduler then takes those tasks and works with cluster manager to get them running on executor resources.

**What does the Task Scheduler actually do?**
1. _Receives tasks from the DAG Scheduler:_ The DAG Scheduler creates **TaskSet** for a stage. The Task Scheduler receives this collection of tasks.
2. _Schedules tasks onto executors:_ The Task Scheduler tries to find available executor resources where those tasks can run. If an executor has only 4 cores available, then only 4 tasks can execute concurrently. 
`Task finishes --> Executor core becomes available --> Another task can be scheduled`
3. _Works with the Cluster Manager:_ The Task Scheduler does not itself create executor processes. The Cluster Manager is responsible for providing resources/executors to Spark. And the Task Scheduler schedules tasks onto those resources/executors.
4. _Handles task locality:_ Task Scheduler tries to run tasks as clos to their required data as practical. Why? Because moving computation to the data is often cheaper than moving large amounts of data across the network.

|                       | DAG Scheduler<br>(What stages do I need?) | Task Scheduler<br>(Where can I run these tasks?) |
| --------------------- | ----------------------------------------- | ------------------------------------------------ |
| Main Job              | Creates Stages                            | Schedules tasks                                  |
| Looks for             | Shuffle boundaries                        | Available executor resources                     |
| Works with            | Stages                                    | Individual tasks                                 |
| Creates               | Stages/TaskSets                           | Task execution attempts                          |
| Concern               | Stage-level scheduling                    | Task-level scheduling                            |
| Handles locality      | Not its primary responsibility            | Yes                                              |
| Handles task retries? | Stage-level recovery                      | Individual task attempts                         |
| Executes tasks?       | No                                        | No                                               |
| Who executes?         |                                           | Executors                                        |

<img width="870" height="740" alt="Planning   Execution" src="https://github.com/user-attachments/assets/cd120004-e4b2-43f1-a3ec-f4fca681d148" />


**Q&A:**
**_1. When we say the driver requests executors, is it the Task Scheduler requesting the cluster manager for the resources?_**
NO. The Task Scheduler does not request executors from the Cluster Manager. More precisely, Spark's SchedulerBackend / Cluster Manager integration handles the resource/executor side.
Driver/SparkContext + SchedulerBackend communicate with the Cluster Manager to obtain executor resources.

----


| Component      | Think                                              |
| -------------- | -------------------------------------------------- |
| Catalyst       | How can I optimize this query?                     |
| DAG Scheduler  | Where are the stage boundaries?                    |
| Task Scheduler | Which task should run on which available resource? |
| Executors      | I'll actually run the code.                        |

