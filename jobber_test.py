from jobber import Jobber
from random import randint
import time


concurrency = 2
jobber = Jobber(concurrency)
jobberd = jobber.decorator


# I use this as shared results dict between
# jobber functions (tasks)
results= {}


def __helper():
    x = randint(2,5)
    time.sleep(x)
    return x


@jobberd()
def l_function_a():
    results['l_function_a'] = __helper()
    return


@jobberd()
def l_function_b():
    results['l_function_b'] = __helper()
    return


@jobberd(['l_function_a'])
def f_function_a():
    results['f_function_a'] = __helper()\
        + results['l_function_a']
    return


@jobberd(['l_function_b'])
def f_function_b():
    results['f_function_b'] = __helper()\
        + results['l_function_b']
    return


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
    return

# Use the following decorator to generate circular dependency
#@jobberd(['l_function_a','f_function_c'])
@jobberd(['l_function_a','f_function_b'])
def f_function_d():
    results['f_function_d'] = __helper()\
        + results['l_function_a']\
        + results['f_function_b']
    return


t_start = time.time()
jobber.work()
t_end = time.time()
report = jobber.get_report()


print("\n\n")
print("# REPORT ###########################################################")
print(report)
print("\n# RESULTS ##########################################################")
for k in results.keys():
    print(f"{k}: {results[k]}")
print("\n# TIME STATS #######################################################")
total_time = 0
for x in results.keys():
    total_time += results[x]

actual_time = int(t_end - t_start)
print("total time:  " + str(total_time))
print("actual time: " + str(actual_time))
print("\n\n")
