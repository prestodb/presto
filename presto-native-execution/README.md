# presto.cpp

## Build Notes

### Dependencies

* [cmake](https://cmake.org/)
* [Boost](https://www.boost.org/) 1.72.0 is required and expected to be found in /usr/local/include/boost.
* [folly](https://github.com/facebook/folly)
* [double-conversion](https://github.com/google/double-conversion)
* [fmt](https://github.com/fmtlib/fmt)
* [Apache Arrow](https://arrow.apache.org/)
* [Google Test/Mock](https://github.com/google/googletest)
* [GFlags](https://github.com/gflags/gflags)
* [GLog](https://github.com/google/glog)
* [libuuid](https://sourceforge.net/projects/libuuid/)


## Building
Run `make` in the root directory to compile the sources. For development, use
`make debug` to build a non-optimized debug version.  Use `make unittest` to build
and run tests.

### Makefile targets
A reminder of the available Makefile targets can be obtained using `make help`
```
    make help
    all                     Build the release version
    clean                   Delete all build artifacts
    cmake                   Use CMake to create a Makefile build system
    build                   Build the software based in BUILD_DIR and BUILD_TYPE variables
    debug                   Build with debugging symbols
    release                 Build the release version
    unittest                Build with debugging and run unit tests
    format-fix              Fix formatting issues in the current branch
    format-check            Check for formatting issues on the current branch
    header-fix              Fix license header issues in the current branch
    header-check            Check for license header issues on the current branch
    tidy-fix                Fix clang-tidy issues in the current branch
    tidy-check              Check clang-tidy issues in the current branch
    linux-container         Build the CircleCi linux container from scratch
    help                    Show the help messages
```

## CircleCi Continuous Integration

Details are in the [.circleci/REAME.md](.circleci)

## Code formatting, headers and clang-tidy

### Showing, Fixing and Passing Checks

Makefile targets exist for showing, fixing and checking formatting, license
headers and clang-tidy warnings.  These targets are shortcuts for calling
`./scripts/check.py`.  

CircleCi runs `make format-check`, `make header-check` and `make tidy-check` as
part of our continious integration.  Pull requests should pass format-check and
header-check without errors before being accepted.  

Formatting issues found on the changed lines in the current commit can be
displayed using `make format-show`.  These issues can be fixed by using `make
format-fix`.  This will apply formatting changes to changed lines in the
current commit.

Header issues found on the changed files in the current commit can be displayed
using `make header-show`.  These issues can be fixed by using `make
header-fix`.  This will apply license header updates to files in the current
commit.

Similar commands `make tidy-show`, `make-tidy-fix`, `make tidy-check` exist for
running clang-tidy, but these checks are currently advisory only.

### Importing code

Code imported from fbcode might pass `make format-check` as is and without
change.  We are using the .clang-format config file that is used in fbcode.

Use `make header-fix` to apply our open source license to imported code.  

An entire directory tree of files can be formatted and have license headers added 
using the `tree` variant of the format.sh commands:
```
    ./scripts/check.py format tree 
    ./scripts/check.py format tree --fix

    ./scripts/check.py header tree 
    ./scripts/check.py header tree --fix
```

All the available formatting commands can be displayed by using
`./scripts/check.py help`.

There is not currently a mechanism to *opt out* files or directories from the
checks.  When we need one it can be added.

## Development Env.

### Setting up on macOs

See `scripts/setup-macos.sh`

After running the setup script add the cmake-format bin to your $PATH, maybe
something like this in your ~/.profile:

```
export PATH=$HOME/bin:$HOME/Library/Python/3.7/bin:$PATH
```

### Setting up on Linux (CentOs 8 or later)

See `scripts/setup-linux.sh`
