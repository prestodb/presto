# PyVelox: Python bindings and extensions for Velox

**This library is currently in Alpha stage and does not have a stable release. The API and implementation may change based on
user feedback or performance. Future changes may not be backward compatible.
If you have suggestions on the API or use cases you'd like to be covered, please open a
GitHub issue. We'd love to hear thoughts and feedback.**


## Prerequisites

You will need Python 3.7 or later. Also, we highly recommend installing an [Miniconda](https://docs.conda.io/en/latest/miniconda.html#latest-miniconda-installer-links) environment.

First, set up an environment. If you are using conda, create a conda environment:
```
conda create --name pyveloxenv python=3.7
conda activate pyveloxenv
```

### Install PyVelox

You can install PyVelox from pypi without the need to build it from source as we provide wheels for Linux and macOS (x86_64):
```
pip install pyvelox
```

### From Source

You will need Python 3.7 or later and a C++17 compiler to build PyVelox from source.


#### Install Dependencies

On macOS

[HomeBrew](https://brew.sh/) is required to install development tools on macOS.
Run the script referenced [here](https://github.com/facebookincubator/velox#setting-up-on-macos) to install all the mac specific  dependencies.

On Linux
Run the script referenced [here](https://github.com/facebookincubator/velox#setting-up-on-linux-ubuntu-2004-or-later) to install on linux.


#### Build PyVelox

For local development, you can build with debug mode:
```
make python-build
```

And run unit tests with
```
make python-test
```
