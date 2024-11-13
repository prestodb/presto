# Breeze

Portable implementation of algorithms for data parallel processing.

The library implements a collection of portable block-wide device
functions and algorithms that can be used to accelerate SQL queries.

The directory contains:

* Functions: `functions/` contains the block-wide device functions
* Algorithms: `algorithms/` contains the algorithms
* Tests: `test/` contains unit tests for all of the above
* Performance tests: `perftest/` contains perf tests for all of the above
* Platforms: `platforms/` contains the platform specific code
* Utilities: `utils/` contains helpers and utility types

Terminology
-----------

Portable library code uses terminology that match CUDA when possible to
make it easy to read and understand the code by anyone already familiar with
this API.

Execution model equivalence:

| [CUDA][CU] | [OpenCL][CL] | [SYCL][SY] | [Metal][MT] |
|:----------:|:------------:|:----------:|:-----------:|
| thread     | work-item    | work-item  | thread      |
| warp       | sub-group    | sub-group  | SIMD-group  |
| block      | work-group   | work-group | threadgroup |

Memory model equivalence:

| CUDA          | OpenCL         | SYCL           | Metal              |
|:-------------:|:--------------:|:--------------:|:------------------:|
| register      | private memory | private memory | thread memory      |
| shared memory | shared memory  | shared memory  | threadgroup memory |
| global memory | global memory  | global memory  | device memory      |

[CU]: https://docs.nvidia.com/cuda/cuda-c-programming-guide/index.html
[CL]: https://registry.khronos.org/OpenCL/specs/3.0-unified/html/OpenCL_API.html
[SY]: https://registry.khronos.org/SYCL/specs/sycl-2020/html/sycl-2020.html
[MT]: https://developer.apple.com/metal/Metal-Shading-Language-Specification.pdf

Usage
-----

### Set up your cmake build directory

Install cmake if you don't already have it. You'll also need the
`ninja` build system (`ninja-build` in Ubuntu).

Set up a build directory with:

```sh
# Sets up a `build` directory. This is where all your build artifacts
# and intermediates go.
cmake -B build -GNinja -DBUILD_CUDA=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo
```

Once your build directory is set up, you can invoke `ninja` to perform
the build.

```sh
ninja -C build
```

* To run all tests
```
ctest --test-dir build
```

* To run all perf tests
```
ctest --test-dir build -R PerfTest
```

* Run specific test
```
./build/test/load_function_unittest-cuda --gtest_filter=FunctionTest/0.Load
```

### Building for HIP

HIP platform support can be enabled using the `BUILD_HIP` cmake option.
`HIP_PATH` can be used to specify where HIP developer tools are installed.

```sh
cmake -B build -GNinja -DBUILD_HIP=ON
```

### Building for OpenCL

OpenCL platform support can be enabled using the `BUILD_OPENCL` cmake option.
An offline kernel compiler program (`clcc`) is currently required.

```sh
cmake -B build -GNinja -DBUILD_OPENCL=ON
```

### Building for SYCL

SYCL platform support can be enabled using the `BUILD_SYCL` cmake option.
`HIPSYCL_PATH` can be used to specify where hipSYCL is installed.
`HIPSYCL_TARGETS` also needs to be specified and `HIPSYCL_CUDA_PATH` must
be set if cuda is one of the targets.

```sh
cmake -B build -GNinja -DBUILD_SYCL=ON -DHIPSYCL_TARGETS=cuda:sm_80 -DHIPSYCL_CUDA_PATH=/usr/local/cuda
```

### Building for Metal

Metal platform support can be enabled on MacOS using the `BUILD_METAL` cmake
option.

```sh
cmake -B build -GNinja -DBUILD_METAL=ON
```

### Building for OpenMP

OpenMP platform support can be enabled using the `BUILD_OPENMP` cmake option.
OpenMP platform requires a C++ compiler with OpenMP support and that OpenMP
libraries and development headers are installed.

```sh
cmake -B build -GNinja -DBUILD_OPENMP=ON
```

### Alternative generators

Multiple generators are supported but `ninja` is recommended.

#### Make

```sh
cmake -B build -DBUILD_CUDA=ON
```

Once your build directory is set up, you can invoke `make` to perform the
build.

```sh
make -C build
```

#### Xcode

```sh
cmake -B build -GXcode -DBUILD_METAL=ON
```

Open `build/breeze.xcodeproj` in Xcode to build and run tests, or invoke
`xcodebuild` to perform the build.

```sh
xcodebuild -project build/breeze.xcodeproj -target ALL_BUILD
```
