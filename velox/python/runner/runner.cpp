/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "velox/python/init/PyInit.h"

#include "velox/python/runner/PyConnectors.h"
#include "velox/python/runner/PyLocalRunner.h"

namespace py = pybind11;

PYBIND11_MODULE(runner, m) {
  using namespace facebook;
  velox::py::initializeVeloxMemory();

  // The executor and root pool need to outlive all vectors returned by this
  // module, so we make them static so they only get destructed when the process
  // is about to exit.
  static auto rootPool = velox::memory::memoryManager()->addRootPool();
  static auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());

  // execute() returns an iterator to Vectors.
  py::module::import("pyvelox.vector");

  py::class_<velox::py::PyLocalRunner>(m, "LocalRunner")
      // Only expose the plan node through the Python API.
      .def(py::init([](const velox::py::PyPlanNode& planNode) {
        return velox::py::PyLocalRunner{planNode, rootPool, executor};
      }))
      .def(
          "execute",
          &velox::py::PyLocalRunner::execute,
          py::arg("max_drivers") = 1,
          py::doc(R"(
        Executes a given plan returning an iterator to the output produced
        by the root plan node.

        Args:
          max_drivers: Maximum number of drivers (threads) to use when
          executing the plan.
          )"))
      .def(
          "print_plan_with_stats",
          &velox::py::PyLocalRunner::printPlanWithStats,
          py::doc(R"(
        Prints a descriptive debug message containing plan and execution
        stats. If the task hasn't finished, will print the plan with the
        current stats.
          )"))
      .def(
          "add_file_split",
          &velox::py::PyLocalRunner::addFileSplit,
          py::arg("file"),
          py::arg("plan_id"),
          py::arg("connector_id") = "prism",
          py::doc(R"(
        Add a split to scan a file, and associate it to the plan node
        described by plan_id.

        Args:
          file: A file object describing the file path and format.
          plan_id: The plan node id of the scan to associate this
                   file/split with.
          connector_id: The id of the connector used by the scan.
          )"))
      .def(
          "add_query_config",
          &velox::py::PyLocalRunner::addQueryConfig,
          py::arg("config_name"),
          py::arg("config_value"),
          py::doc(R"(
        Add a query configuration parameter. These values are passed to the
        Velox Task through a query context object.

        Args:
          configName: The name (key) of the configuration parameter.
          configValue: The configuration value.
          )"));

  m.def(
       "register_hive",
       &velox::py::registerHive,
       pybind11::arg("connector_name") = "hive",
       pybind11::arg("configs") =
           std::unordered_map<std::string, std::string>{},
       py::doc(R"(
        "Initialize and register Hive connector.

        Args:
          connector_name: Name to use for the registered connector.
          configs: A dictionary containing connector configs.
      )"))
      .def(
          "register_tpch",
          &velox::py::registerTpch,
          pybind11::arg("connector_name") = "tpch",
          pybind11::arg("configs") =
              std::unordered_map<std::string, std::string>{},
          py::doc(R"(
        "Initialize and register TPC-H connector.

        Args:
          connector_name: Name to use for the registered connector.
          configs: A dictionary containing connector configs.
      )"))
      .def(
          "unregister",
          &velox::py::unregister,
          pybind11::arg("connector_name"),
          py::doc(R"(
        "Unregister connector.

        Args:
          connector_name: Name of the connector to unregister.",
      )"))
      .def("unregister_all", &velox::py::unregisterAll, py::doc(R"(
        "Unregister all registered connectors.)"));

  // When the module gets unloaded, first ensure all tasks created by this
  // module have finished, then unregister all connectors that have been
  // registered by this module. We need to explicity unregister them to prevent
  // the connectors and their nested structures from being destructed after
  // other global and static resources are destructed.
  m.add_object("_cleanup", py::capsule([]() {
                 velox::py::drainAllTasks();
                 velox::py::unregisterAll();
               }));
}
