# Jobber

This tool allows to execute a function inside a thread and provides
an easy way to manage dependencies beetween a functions pool.
Any function can also be run in multiple parallel copies.

See `jobber_test.py`

## Quick start

```python
from jobber import Jobber
```

Set concunrrency (best is number of cores)
and initialize Jobber

```python
concurrency = 3
jobber = Jobber(concurrency)
```
Get the decorator

```python
jobberd = jobber.decorator
```

### Dependencies

```python
@jobberd([])
def fn1():
  pass

@jobberd([])
def fn2():
  pass

@jobberd(['fn1','fn2'])
def fn3():
  pass
```

`fn1` and `fn2` run in parallel and `fn3` will run only when the
first two are completed.

### Parallelism

```python
@jobberd([],4)
def fn():
  pass
```

Function `fn` is queued 4 times.
Because concurrency is set to 3, Jobber will run 3 fn in parallel
and than the fourth.

### Parallelism and Dependencies

Parallelism between dependency is mandatory and has to equal.

```python
@jobberd([])
def fn1():
  pass

@jobberd([],2)
def fn2():
  pass

@jobberd(['fn2'],2)
def fn3():
  pass
```

`fn1` run in parallel with `fn2-0` and `fn2-1`.
`fn3-0` depends from `fn2-0` and `fn3-1` depends from `fn2-1`

