import threading
from queue import Queue


SUCCESS = "SUCCESS"
FAIL = "FAIL"
NOT_FOUND = "NOT_FOUND"


class CircularDependencyException(Exception):
    pass


class DependencyNotFound(Exception):
    pass


class Jobber:
    def __init__(self, concurrency):
        self.registry = {}
        self.dependency_tree = {}
        self.concurrency = concurrency

    def decorator(self, dependencies=[], parallelism=1):
        def registrar(func):
            for i in range(parallelism):
                deps = [d+str(i) for d in dependencies]
                self.registry[func.__name__ + str(i)] = func
                self.dependency_tree[func.__name__ + str(i)] = set(deps)
            return func

        return registrar

    def get_registry(self):
        return self.registry

    def get_dependencies(self):
        d = Dependencies(self.dependency_tree)
        try:
            return d.resolve()
        except CircularDependencyException as e:
            err_msg = "Circular dependency detected"
            print(err_msg, e)
            raise SystemExit(1)
        except DependencyNotFound as e:
            err_msg = "Dependency not found."
            print(err_msg, e)
            raise SystemExit(1)

    def work(self):
        jc = JobberConsumer(
            self.get_registry(),
            self.get_dependencies(),
            self.concurrency
        )
        jc.start()


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
            raise CircularDependencyException({
                "message": "Recursion limit reached",
                "loop": node.name,
                "node": self.current_node,
                "dependencies": counter,
            })

        for edge in node.edges:
            try:
                if edge.name not in self.resolved:
                    counter.append(node.name)
                    self.dep_resolve(self.nodes[edge.name], counter)
            except KeyError:
                raise DependencyNotFound({
                    "message": f"{edge.name}: dependency not found",
                    "loop": node.name,
                    "node": self.current_node,
                    "dependencies": counter,
                })

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
        self.nodes = {}
        self.resolved = []
        self.ordered_dep_tree = []
        self.tmp = {}

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
        self.tasks = tasks
        self.tasks_workflow = tasks_workflow
        self.starting_ts = threading.active_count()
        self.q_worker = Queue(maxsize=0)
        self.concurrency_range = concurrency_range

    def __run_consumer(self):
        while True:
            imessage = self.q_worker.get()

            if not imessage:
                self.q_worker.task_done()
                break

            if imessage[0] == "STOP":
                self.q_worker.task_done()
                break

            for task in imessage:
                try:
                    self.tasks[task]()
                except Exception as e:
                    print(e)

            self.q_worker.task_done()

    def start(self):
        for dependency_set in self.tasks_workflow:
            concurrency = 0
            og_chunks = [dependency_set[i::self.concurrency_range]
                         for i in range(self.concurrency_range)]

            for task_list in og_chunks:
                if task_list:
                    self.q_worker.put(task_list)
                    concurrency += 1

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
                print('started')
                threads.append(t_worker)

            self.q_worker.join()

            for t_worker in threads:
                print('joint')
                t_worker.join()
        print('j end')
