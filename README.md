# Jobber

This tool allows to execute a function inside a thread and provides
an easy way to manage dependencies beetween a functions pool.
Any function can also be run in multiple parallel copies.

See `jobber_test.py`

## Quick start

```python
from jobber import Jobber

# set concunrrency (best is number of cores)
# and initialize Jobber
concurrency = 3
jobber = Jobber(concurrency)

# get the decorator
jobberd = jobber.decorator
```

Run same function in parallel

```python
@jobberd([],4)
def fn();
  pass
```

Function fn is queued 4 times.
Because concurrency is set to 3, Jobber will run 3 fn in parallel
and than the fourth.
