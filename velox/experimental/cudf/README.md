# Velox-cuDF

Velox-cuDF is a Velox extension module that uses the cuDF library to implement a GPU-accelerated backend for executing Velox plans. [cuDF](https://github.com/rapidsai/cudf) is an open source library for GPU data processing, and Velox-cuDF integrates with "[libcudf](https://github.com/rapidsai/cudf/tree/main/cpp)", the CUDA C++ core of cuDF. libcudf uses [Arrow](https://arrow.apache.org)-compatible data layouts and includes single-node, single-GPU algorithms for data processing.

## How Velox and cuDF work together

Velox-cuDF implements the Velox [DriverAdapter](https://github.com/facebookincubator/velox/blob/d9f953cd23880f29593534f1ba9031c6cea8ba06/velox/exec/Driver.h#L695) interface as [CudfDriverAdapter](https://github.com/facebookincubator/velox/blob/226b92cefedce4b8a484bfc351260edbd3d2e501/velox/experimental/cudf/exec/ToCudf.cpp#L301) to rewrite query plans for GPU execution. Generally the cuDF DriverAdapter replaces operators one-to-one. For end-to-end GPU execution where cuDF replaces all of the Velox CPU operators, cuDF relies on Velox's [pipeline-based execution model](https://facebookincubator.github.io/velox/develop/task.html) to separate stages of execution, partition the work across drivers, and schedule concurrent work on the GPU.

For more information please refer to our blog: "[Extending Velox - GPU Acceleration with cuDF](https://velox-lib.io/blog/extending-velox-with-cudf)."

## Getting started with Velox-cuDF

cuDF supports Linux and WSL2 but not Windows or MacOS. cuDF also has minimum CUDA version, NVIDIA driver and GPU architecture requirements which can be found in the [RAPIDS Installation Guide](https://docs.rapids.ai/install/). Please refer to cuDF's [readme](https://github.com/rapidsai/cudf) and [developer guide](https://github.com/rapidsai/cudf/blob/main/cpp/doxygen/developer_guide/DEVELOPER_GUIDE.md) for more information.

### Building Velox with cuDF

The cuDF backend is included in Velox builds when the [VELOX_ENABLE_CUDF](https://github.com/facebookincubator/velox/blob/43df50c4f24bcbfa96f5739c072ab0894d41cf4c/CMakeLists.txt#L455) CMake option is set. The `adapters-cuda` service in Velox's [docker-compose.yml](https://github.com/facebookincubator/velox/blob/43df50c4f24bcbfa96f5739c072ab0894d41cf4c/docker-compose.yml#L69) is an excellent starting point for Velox builds with cuDF.

1. Use `docker compose` to run an `adapters-cuda` image.
```
$ docker compose -f docker-compose.yml run -e NUM_THREADS=8 --rm -v "$(pwd):/velox" adapters-cuda /bin/bash
```
2. Once inside the image, build cuDF with the following flags:
```
$ CUDA_ARCHITECTURES="native" EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_ARROW=ON -DVELOX_ENABLE_PARQUET=ON -DVELOX_ENABLE_BENCHMARKS=ON -DVELOX_ENABLE_BENCHMARKS_BASIC=ON" make cudf
```
3. After cuDF is built, verify the build by running the unit tests.
```
$ cd _build/release
$ ctest -R cudf -V
```

Velox-cuDF builds are included in Velox CI as part of the [adapters build](https://github.com/facebookincubator/velox/blob/de31a3eb07b5ec3cbd1e6320a989fcb2ee1a95a7/.github/workflows/linux-build-base.yml#L85). The build step for cuDF does not require the worker to have a GPU, so adding a Velox-cuDF build step to Velox CI is compatible with the existing runners.

### Testing Velox with cuDF

Tests with Velox-cuDF can only be run on GPU-enabled hardware. The Velox-cuDF tests in [experimental/cudf/tests](https://github.com/facebookincubator/velox/blob/main/velox/experimental/cudf/tests) include several types of tests:
* operator tests
* function tests
* fuzz tests (not yet implemented)

The repo [rapidsai/velox-testing](https://github.com/rapidsai/velox-testing/) includes standard scripts for testing Velox-cuDF. Please refer to the [test_velox.sh](https://github.com/rapidsai/velox-testing/blob/main/velox/scripts/test_velox.sh) for running the Velox-cuDF unit tests. We plan to first develop GitHub Actions for GPU CI in [rapidsai/velox-testing](https://github.com/rapidsai/velox-testing/), and then later transition GPU-enabled GitHub Actions to Velox mainline.

#### Operator tests

Many of the tests for cuDF are "operator tests" which confirm correct execution of simple query plans. cuDF's operator tests use `CudfDriverAdapter` to modify the test plan with GPU operators before executing it. The operator tests for cuDF include both tests that assert successful GPU operator replacement, and tests that pass with CPU fallback.

#### Function tests

Velox-cuDF also includes "function tests" which cover the behavior of shared functions that could be called in multiple operators. Velox-cuDF function tests assess the correctness of functions using one or more cuDF API calls to provide the output. [SubfieldFilterAstTest](https://github.com/facebookincubator/velox/blob/99a04b94eed42d1c35ae99101da3bf77b31652e8/velox/experimental/cudf/tests/SubfieldFilterAstTest.cpp#L158) includes several examples of function tests. Please note that unit tests for cuDF APIs are included in [cudf/cpp/tests](https://github.com/rapidsai/cudf/tree/branch-25.10/cpp/tests) rather than Velox.

#### Fuzz tests

Velox includes components for "fuzz testing" to ensure robustness of Velox operators. For instance, the [Join Fuzzer](https://github.com/facebookincubator/velox/blob/99a04b94eed42d1c35ae99101da3bf77b31652e8/velox/docs/develop/testing/join-fuzzer.rst) executes a random join type with random inputs and compares the Velox results with a reference query engine. Fuzz testing tools have been used for cuDF operator development, but fuzz testing for cuDF is not yet integrated into Velox mainline.

### Benchmarking Velox with cuDF

Velox's [TpchBenchmark](https://github.com/facebookincubator/velox/blob/d9f953cd23880f29593534f1ba9031c6cea8ba06/velox/benchmarks/tpch/TpchBenchmark.cpp) is derived from [TPC-H](https://www.tpc.org/tpch/) and provides a convenient tool for  benchmarking Velox's performance with OLAP (Online Analytical Processing) workloads. Velox-cuDF includes GPU operators for the hand-built query plans located in [TpchQueryBuilder](https://github.com/facebookincubator/velox/blob/43df50c4f24bcbfa96f5739c072ab0894d41cf4c/velox/exec/tests/utils/TpchQueryBuilder.cpp). Velox [PR 13695](https://github.com/facebookincubator/velox/pull/13695) extends Velox's TpchBenchmark to the cuDF backend.

Please note that Velox's hand-built query plans require the data set to have floating-point types in place of the fixed-point types defined in the standard. Further development of Velox's TpchBenchmark could allow correct behavior with both fixed-point and floating-point types.

## Contributing

Velox-cuDF's development priorities are documented as Velox issues using the "[cuDF]" prefix. Please check out the [open issues](https://github.com/facebookincubator/velox/issues?q=is%3Aissue%20state%3Aopen%20%5BcuDF%5D) to learn more.

We would love to hear from you in Velox's Slack workspace, please see Velox discussion [11348](https://github.com/facebookincubator/velox/discussions/11348) for information on joining.
