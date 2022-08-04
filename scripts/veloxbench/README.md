# Velox benchmarks

Work in progress, hooking up Velox benchmarks into conbench

### Create virualenv
    $ cd ~/envs
    $ python3 -m venv veloxbench
    $ source veloxbench/bin/activate

### Install veloxbench dependencies

Note: the directory here is currently in the velox repo: `scripts/veloxbench`

```
(veloxbench) $ cd veloxbench/
(veloxbench) $ pip install -r requirements-test.txt
(veloxbench) $ pip install -r requirements-build.txt
(veloxbench) $ python setup.py develop
```

### Run tests

```
(veloxbench) $ cd veloxbench/
(veloxbench) $ pytest -vv veloxbench/tests/
```

### Format code (before committing)

```
(veloxbench) $ cd veloxbench/
(veloxbench) $ git status
    modified: foo.py
(veloxbench) $ black foo.py
    reformatted foo.py
(veloxbench) $ git add foo.py
```


### Sort imports (before committing)

```
(veloxbench) $ cd veloxbench/
(veloxbench) $ isort .
    Fixing foo.py
(veloxbench) $ git add foo.py
```


### Lint code (before committing)

```
(veloxbench) $ cd veloxbench/
(veloxbench) $ flake8
./foo/bar/__init__.py:1:1: F401 'FooBar' imported but unused
```

### Generate coverage report

```
(veloxbench) $ cd veloxbench/
(veloxbench) $ coverage run --source veloxbench -m pytest veloxbench/tests/
(veloxbench) $ coverage report -m
```
