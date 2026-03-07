#pragma once

#include <string>

namespace facebook::presto::functions {

void registerKllSketchFunctions(const std::string& prefix = "");

} // namespace facebook::presto::functions


namespace facebook::presto::functions::kll_sketch {

namespace {

inline void registerAllKllSketchFunctions(const std::string& prefix = "") {
  facebook::presto::functions::registerKllSketchFunctions(prefix);
}

} // namespace
} // namespace facebook::presto::functions::kll_sketch