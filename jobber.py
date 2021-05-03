import time
import pandas as pd
import threading
from queue import Queue


SUCCESS = "SUCCESS"
FAIL = "FAIL"
NOT_FOUND = "NOT_FOUND"


class JLogger:
    def __get_time(self):
        return time.strftime("%H:%M:%S", time.gmtime())


    def header(self, msg):
        msg_len = len(msg)
        prefix = f"[{self.__get_time()}] # "
        prefix_len = len(prefix)
        tail_len = 99 - msg_len - prefix_len
        tail = "#" * tail_len
        print(prefix + f"{msg} " + tail)


    def info(self, msg):
        print(f"[{self.__get_time()}] [INFO] {msg}")


    def err(self, msg):
        print(f"[{self.__get_time()}] [ERROR] {msg}")


jlog = JLogger()


class Jobber:
    def __init__(self, concurrency):
        self.registry = {}
        self.dependency_tree = {}
        self.work_log = None
        self.concurrency = concurrency


    def decorator(self,dependencies=[]):
        def registrar(func):
            self.registry[func.__name__] = func
            self.dependency_tree[func.__name__] = set(dependencies)
            return func

        return registrar


    def get_registry(self):
        return self.registry


    def get_dependencies(self):
        d = Dependencies(self.dependency_tree)
        try:
            return d.resolve()
        except Exception as e:
            loop = e.args[0]['loop']
            node = e.args[0]['node']
            err_msg = "Circular dependency detected"\
                    + f" between {loop} and {node}\n"\
                    + f"  {loop}: {self.dependency_tree[loop]}\n"\
                    + f"  {node}: {self.dependency_tree[node]}"
            jlog.err(err_msg)
            raise SystemExit(1)


    def work(self):
        jc = JobberConsumer(
            self.get_registry(),
            self.get_dependencies(),
            self.concurrency
        )
        jc.start()
        self.work_log = jc.get_logs()

    def get_report(self):
        return self.work_log



class Node:
    """ todo """
    def __init__(self, name):
        self.name = name
        self.edges = []

    def addEdge(self, node):
        """ todo """
        self.edges.append(node)

    def __str__(self):
        edges_name = [n.name for n in self.edges]
        return f"{self.name}:[{','.join(edges_name)}]"


class Dependencies:
    nodes = {}
    resolved = []
    ordered_dep_tree = []
    tmp = {}


    def __init__(self, dep_tree):
        self.dep_tree = dep_tree
        self.current_node = None


    def processTable(self, _task):
        """ todo """
        self.nodes[_task] = Node(_task)
        if self.dep_tree[_task]:
            for dep in self.dep_tree[_task]:
                self.nodes[_task].addEdge(Node(dep))


    def dep_resolve(self, node, counter=[]):
        """ todo """
        if len(counter) > len(node.edges) + 1:
            raise Exception({
                "message": f"Recursion limit reached",
                "loop": node.name,
                "node": self.current_node,
                "dpendencies": counter,
            })

        for edge in node.edges:
            if edge.name not in self.resolved:
                counter.append(node.name)
                self.dep_resolve(self.nodes[edge.name], counter)


        if node.name not in self.resolved:
            self.resolved.append(node.name)


    def findMaxChildDepth(self, _task):
        """ todo """
        maxDepth = 0
        for dep in self.dep_tree[_task]:
            if self.tmp[dep] > maxDepth:
                maxDepth = self.tmp[dep]

        return maxDepth + 1


    def resolve(self):
        for task in self.dep_tree:
            self.processTable(task)

        for n in self.nodes:
            self.current_node = n
            self.dep_resolve(self.nodes[n])

        for task in self.resolved:
            if self.dep_tree[task]:
                i = self.findMaxChildDepth(task)
            else:
                i = 0

            self.tmp[task] = i

            try:
                self.ordered_dep_tree[i]
            except Exception:
                for x in range(len(self.ordered_dep_tree)-1, i):
                    self.ordered_dep_tree.append([])

            self.ordered_dep_tree[i].append(task)

        return self.ordered_dep_tree


class JobberConsumer:


    def __init__(self, tasks, tasks_workflow, concurrency_range=2):
        self.tasks= tasks
        self.tasks_workflow = tasks_workflow
        self.starting_ts = threading.active_count()
        self.q_log = Queue(maxsize=0)
        self.q_worker = Queue(maxsize=0)
        self.concurrency_range = concurrency_range
        self.history_df = None
        self.history_full = []
        self.history_not_found = []
        self.history_success = []
        self.history_fail = []


    def __log_to_df(self):
        columns = ['id', 'step', 'action', 'status', 'message']
        my_name = threading.current_thread().name

        history_tmp = []

        while True:
            imessage = self.q_log.get()
            action_key = imessage['action_key']

            if action_key == "STOP":
                jlog.info(f"Thread {my_name}: STOP signal received")
                self.history_not_found = [t for t in history_tmp
                                          if NOT_FOUND == t[3]]
                self.history_success = [t for t in history_tmp
                                        if SUCCESS == t[3]]
                self.history_fail = [t for t in history_tmp if FAIL == t[3]]

                jlog.info(
                        "TOTAL: {} | SUCCESS: {} | NOT_FOUND: {}".format(
                        len(self.history_full),
                        len(self.history_success),
                        len(self.history_not_found),
                    )
                    + " | FAIL: {} ".format(
                        len(self.history_fail)
                    )
                )

                self.history_df = pd.DataFrame(self.history_full)
                self.history_df.columns = columns
                break

            if action_key == 'w':
                action_val = imessage['action_val']
                self.history_full.append(action_val)
                history_tmp.append(action_val)

            elif action_key == 'h':
                action_val = imessage['action_val']
                history_tmp.append(action_val)

            elif action_key == 'r':
                try:
                    self.history_not_found = [t for t in history_tmp
                                              if NOT_FOUND == t[3]]
                    self.history_success = [t for t in history_tmp
                                            if SUCCESS == t[3]]
                    self.history_fail = [t for t in history_tmp
                                         if FAIL == t[3]]

                    self.q_worker.put(["STOP"])
                except Exception as e:
                    jlog.err(e)

            self.q_log.task_done()


    def __run_consumer(self):
        my_name = threading.current_thread().name
        while True:
            imessage = self.q_worker.get()
            worker_id = time.time()
            jlog.info(f"Thread {my_name}_{worker_id}: {imessage}")

            if not self.q_worker.empty():
                jlog.info(f"Thread {my_name}_{worker_id} queue items left:")
                for item in list(self.q_worker.queue):
                    print(f"  {item}")

            if not imessage:
                jlog.err(f"Thread {my_name}_{worker_id}: imessage IS EMPTY!")
                self.q_worker.task_done()
                break

            if imessage[0] == "STOP":
                jlog.info(
                    f"Thread {my_name}_{worker_id}: STOP signal received")
                self.q_worker.task_done()
                break

            for task in imessage:
                status = "UNKNOWN"
                message = ""
                try:
                    self.tasks[task]()
                    status = SUCCESS
                except Exception as e:
                    status = FAIL
                    message = f"{type(e).__name__}: {str(e)}"
                finally:
                    jlog.info(f"Thread {my_name}_{worker_id}:"\
                            + f" {status} {task} {message}")

                _l_activity = {
                    "action_key": "w",
                    "action_val": (
                        int(time.time()*1000),  # id
                        "run_consumer",         # step
                        task,                   # action
                        status,                 # status
                        message,                # message
                    )
                }
                self.q_log.put(_l_activity)

            self.q_log.put({"action_key": 'r'})
            self.q_worker.task_done()


    def get_logs(self):
        return self.history_df


    def start(self):

        jlog.header("LOGGING THREAD SETUP")

        t_logger = threading.Thread(
            target=self.__log_to_df,
            args=(),
            name="Holy Logger"
        )
        t_logger.setDaemon(True)

        t_start = time.time()
        t_logger.start()

        jlog.header("JOB STARTED")

        l_activity = {
            "action_key": "h",
            "action_val": (
                int(time.time()*1000),          # id
                "MAIN",                         # step
                "Run parallel consumenrs",      # action
                "STARTING",                     # status
                str(time.time() - t_start),     # message
            )
        }
        self.q_log.put(l_activity)

        jlog.header("Activity message sent")

        for dependency_set in self.tasks_workflow:
            concurrency = 0
            og_chunks = [dependency_set[i::self.concurrency_range]
                         for i in range(self.concurrency_range)]

            for task_list in og_chunks:
                if task_list:
                    self.q_worker.put(task_list)
                    concurrency += 1

            jlog.header("Workers messages sent")

            threads = []
            if concurrency > self.concurrency_range:
                concurrency = self.concurrency_range

            for i in range(concurrency):
                t_worker = threading.Thread(
                    target=self.__run_consumer,
                    args=(),
                    name="consumer_" + str(i))
                t_worker.setDaemon(True)
                t_worker.start()
                threads.append(t_worker)

            jlog.header("Threads started")

            self.q_worker.join()

            for t_worker in threads:
                t_worker.join()

            jlog.info(
                "SUCCESS: {} | NOT_FOUND: {} ".format(
                    len(self.history_success),
                    len(self.history_not_found)
                )
                + "| FAIL: {}".format(len(self.history_fail))
            )


        self.q_log.join()
        jlog.header("LOGGER STOP")
        self.q_log.put({"action_key": "STOP"})
        t_logger.join()

        jlog.header("JOB FINISHED")
        jlog.info(
            "workers: {} | workers queue size: {} | log queue size: {}"\
            .format(
                (threading.active_count() - self.starting_ts),
                self.q_worker.qsize(),
                self.q_log.qsize(),
            )
        )

