# PyVelox: Python bindings and extensions for Velox

**This library is currently in Alpha stage and does not have a stable release. The API and implementation may change based on
user feedback or performance. Future changes may not be backward compatible.
If you have suggestions on the API or use cases you'd like to be covered, please open a
GitHub issue. We'd love to hear thoughts and feedback.**


## Prerequisites

You will need Python 3.9 or later. We recommend using `uv` for Python Environment Management [uv](https://github.com/astral-sh/uv) is a fast Python package installer and environment manager written in Rust.
Here's how to use it:

1. **Install uv**:
   ```bash
   brew install uv
   # or
   pipx uv
   # or
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Create a virtual environment**:
   ```bash
   uv venv --python=3.12
   ```
   This creates a venv in the current directory. You can specify a path:
   ```bash
   uv venv --python=3.12 /path/to/venv
   ```

3. **Activate the environment**:
   ```bash
   source .venv/bin/activate
   ```

4. **Installing packages**:
   ```bash
   uv pip install pyvelox
   ```

   Or from a requirements file:
   ```bash
   uv pip install -r requirements.txt
   ```


### Install PyVelox

You can install PyVelox from pypi without the need to build it from source as we provide wheels for Linux and macOS (x86_64):
```
uv pip install pyvelox
```

### From Source

You will need Python 3.9 or later and a C++17 compiler to build PyVelox from source.

First, set up a virtual environment and activate it. Now install the build requirements to enable editable builds:
```
uv pip install pyarrow scikit-build-core setuptools_scm[toml]
```

#### Install Dependencies

On macOS

[HomeBrew](https://brew.sh/) is required to install development tools on macOS.
Run the script referenced [here](https://github.com/facebookincubator/velox#setting-up-on-macos) to install all the mac specific  dependencies.

On Linux
Run the script referenced [here](https://github.com/facebookincubator/velox#setting-up-on-linux-ubuntu-2004-or-later) to install on linux.


#### Build PyVelox

For local development, you can build an editable debug build with automatic rebuilds:
```
make python-build
```

And run unit tests with
```
make python-test
```
