from jobber import Jobber, JLogger
from random import randint
import time

# set concunrrency (best is number of cores)
# and initialize Jobber
concurrency = 2
jobber = Jobber(concurrency)


# get the decorator
jobberd = jobber.decorator


# Set a shared resource for jobber functions (tasks) results
# I'll use a dict to store tasks results, but everythig could be used:
# a redis queue, a pandas dataframe, an s3 bucket..
results= {}


# this is an helper function, not decorated
def __helper():
    x = randint(2,5)
    time.sleep(x)
    return x


# Below some tasks
# Each task has its dependencies defined as an array passed as decorator
# argument.
@jobberd([],2)
def l_function_a():
    try:
        results['l_function_a'] =\
            results['l_function_a'] + __helper()
    except:
        results['l_function_a'] = __helper()


@jobberd([],2)
def l_function_b():
    try:
        results['l_function_b'] =\
            results['l_function_b'] + __helper()
    except:
        results['l_function_b'] = __helper()


@jobberd(['l_function_a'])
def f_function_a():
    try:
        results['f_function_a'] = __helper()\
            + results['f_function_a'] + results['l_function_a']
    except:
        results['f_function_a'] = __helper()\
            + results['l_function_a']


@jobberd(['l_function_b'],2)
def f_function_b():
    try:
        results['f_function_b'] = __helper()\
            + results['f_function_b'] + results['l_function_b']
    except:
        results['f_function_b'] = __helper()\
            + results['l_function_b']


# The following function:
# 1. will be executed last, due to the dependency from 'f_function_d'
#    even if it is defined before
# 2. will fail beacuse of a reference to a non existent key in the
#    `results` dict
@jobberd(['l_function_a','l_function_b','f_function_d'])
def f_function_c():
    results['f_function_c'] = __helper()\
        + results['l_function_a']\
        + results['l_function_b']\
        + results['l_function_d'] # KeyError: 'l_function_d' does not exists
                                  # it is 'f_function_d'


# Use the following decorator to generate circular dependency
#@jobberd(['l_function_a','f_function_c'])
@jobberd(['l_function_a','f_function_b'],2)
def f_function_d():
    try:
        results['f_function_d'] = __helper()\
            + results['f_function_d']\
            + results['l_function_a']\
            + results['f_function_b']
    except:
        results['f_function_d'] = __helper()\
            + results['l_function_a']\
            + results['f_function_b']


if __name__ == '__main__':
    t_start = time.time()
    jobber.work()
    t_end = time.time()

    # Jobber report is a pandas dataframe: you can query it!
    report = jobber.get_report()

    # JLogger is for Jobber internal use, but it ha the `header` function
    # that I like to use :)
    jlog = JLogger()

    print("\n\n")
    jlog.header("TASKS LIST")
    i = 0
    for task in jobber.get_dependencies():
        print(f"Task {i}: {str(task)}")
        i = i + 1

    print("\n")
    jlog.header("REPORT")
    print(report)

    print("\n")
    jlog.header("RESULTS")
    for k in results.keys():
        print(f"  {k}: {results[k]}")

    print("\n")
    jlog.header("TIME STATS")
    print("""  These time stats are a special case because in this exaple
  we know exatcy the duration of every single task, so it's easy to calculate
  the duration af all task one after the other.
  It should be fine to find a way to have "wieghted dependency" or a kind of
  "antiaffinity" between task, in order to assure that tasks we know to have a
  long duration will be scheduled in different queues\n""")
    total_time = 0
    for x in results.keys():
        total_time += results[x]

    actual_time = int(t_end - t_start)
    print("  total time:  " + str(total_time))
    print("  actual time: " + str(actual_time))
    print("\n\n")
