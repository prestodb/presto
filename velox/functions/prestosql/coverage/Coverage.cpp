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

#include <gflags/gflags.h>
#include <fstream>
#include <iostream>
#include "velox/exec/Aggregate.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_bool(all, false, "Generate coverage map for all Presto functions");
DEFINE_bool(
    most_used,
    false,
    "Generate coverage map for a subset of most-used Presto functions");

using namespace facebook::velox;

namespace {
std::vector<std::string> readFunctionNamesFromFile(
    const std::string& filePath) {
  std::ifstream functions("data/" + filePath);

  std::vector<std::string> names;
  std::string name;
  while (getline(functions, name)) {
    names.emplace_back(name);
  }

  functions.close();
  return names;
}

std::string toFuncLink(const std::string& name) {
  return fmt::format(":func:`{}`", name);
}

int maxLength(const std::vector<std::string>& names) {
  int maxLength = 0;

  for (const auto& name : names) {
    auto len = name.size();
    if (len > maxLength) {
      maxLength = len;
    }
  }
  return maxLength;
}

class TablePrinter {
 public:
  TablePrinter(
      size_t numScalarColumns,
      size_t columnSize,
      std::string indent,
      std::ostream& out)
      : numScalarColumns_{numScalarColumns},
        columnSize_{columnSize},
        indent_{std::move(indent)},
        out_{out} {}

  void header() {
    std::string line(columnSize_, '=');
    out_ << indent_ << line;
    for (int i = 1; i < numScalarColumns_; i++) {
      out_ << "  " << line;
    }
    out_ << "  ==  " << line << std::endl;

    auto scalarFunctionsColumnWidth =
        columnSize_ * numScalarColumns_ + 2 * (numScalarColumns_ - 1);

    out_ << indent_ << std::left << std::setw(scalarFunctionsColumnWidth)
         << "Scalar Functions"
         << "      "
         << "Aggregate Functions" << std::endl;
    out_ << indent_ << std::string(scalarFunctionsColumnWidth, '=')
         << "  ==  " << line << std::endl;
  }

  void startRow() {
    out_ << indent_;
    currentColumn_ = 0;
  }

  void addColumn(const std::string& text) {
    addColumn(text, columnSize_);
  }

  void addEmptyColumn() {
    if (currentColumn_ == numScalarColumns_) {
      addColumn("", 2);
    } else {
      addColumn("", columnSize_);
    }
  }

  void endRow() {
    out_ << std::endl;
  }

  void footer() {
    std::string line(columnSize_, '=');
    out_ << indent_ << line;
    for (int i = 1; i < numScalarColumns_; i++) {
      out_ << "  " << line;
    }
    out_ << "  ==  " << line << std::endl;
  }

 private:
  void addColumn(const std::string& text, size_t columnSize) {
    if (currentColumn_) {
      out_ << "  ";
    }
    out_ << std::setw(columnSize) << text;
    ++currentColumn_;
  }

  const size_t numScalarColumns_;
  const size_t columnSize_;
  const std::string indent_;
  std::ostream& out_;

  size_t currentColumn_{0};
};

class TableCellTracker {
 public:
  // Takes zero-based row and column numbers.
  void add(int row, int column) {
    cells_.emplace_back(row, column);
  }

  const std::vector<std::pair<int, int>>& cells() const {
    return cells_;
  }

 private:
  std::vector<std::pair<int, int>> cells_;
};

/// Prints out CSS rules to
/// - add lightblue background to table header;
/// - add lightblue background to an empty column that separates scalar and
/// aggregate functions;
/// - highlight cells identified by TableCellTracker.
void printTableCss(
    size_t numScalarColumns,
    const TableCellTracker& cellTracker,
    std::ostream& out) {
  out << "    div.body {max-width: 1300px;}" << std::endl;
  out << "    table.coverage th {background-color: lightblue; text-align: center;}"
      << std::endl;
  out << "    table.coverage "
      << "td:nth-child(" << numScalarColumns + 1 << ") "
      << "{background-color: lightblue;}" << std::endl;

  for (const auto& entry : cellTracker.cells()) {
    out << "    table.coverage "
        << "tr:nth-child(" << entry.first + 1 << ") "
        << "td:nth-child(" << entry.second + 1 << ") "
        << "{background-color: #6BA81E;}" << std::endl;
  }
}

void printCoverageMap(
    const std::vector<std::string>& scalarNames,
    const std::vector<std::string>& aggNames,
    const std::unordered_set<std::string>& veloxNames,
    const std::unordered_set<std::string>& veloxAggNames) {
  const auto scalarCnt = scalarNames.size();
  const auto aggCnt = aggNames.size();

  // Make sure there is enough space for the longest function name + :func:
  // syntax that turns function name into a link to function's description.
  const int columnSize = std::max(maxLength(scalarNames), maxLength(aggNames)) +
      toFuncLink("").size();

  const std::string indent(4, ' ');

  const int numScalarColumns = 5;

  // Split scalar functions into 'numScalarColumns' columns. Put all aggregate
  // functions into one column.
  auto numRows =
      std::max((size_t)std::ceil((double)scalarCnt / numScalarColumns), aggCnt);

  // Keep track of cells which contain functions available in Velox. These cells
  // need to be highlighted using CSS rules.
  TableCellTracker veloxCellTracker;

  auto printName = [&](int row,
                       int column,
                       const std::string& name,
                       const std::unordered_set<std::string>& veloxNames) {
    if (veloxNames.count(name)) {
      veloxCellTracker.add(row, column);
      return toFuncLink(name);
    } else {
      return name;
    }
  };

  std::ostringstream out;
  TablePrinter printer(numScalarColumns, columnSize, indent, out);
  printer.header();
  for (int i = 0; i < numRows; i++) {
    printer.startRow();

    // N columns of scalar functions.
    for (int j = 0; j < numScalarColumns; j++) {
      auto n = i + numRows * j;
      n < scalarCnt
          ? printer.addColumn(printName(i, j, scalarNames[n], veloxNames))
          : printer.addEmptyColumn();
    }

    // 1 empty column.
    printer.addEmptyColumn();

    // 1 column of aggregate functions.
    i < aggCnt ? printer.addColumn(printName(
                     i, numScalarColumns + 1, aggNames[i], veloxAggNames))
               : printer.addEmptyColumn();

    printer.endRow();
  }
  printer.footer();

  std::cout << ".. raw:: html" << std::endl << std::endl;
  std::cout << "    <style>" << std::endl;
  printTableCss(numScalarColumns, veloxCellTracker, std::cout);
  std::cout << "    </style>" << std::endl << std::endl;

  std::cout << ".. table::" << std::endl;
  std::cout << "    :widths: auto" << std::endl;
  std::cout << "    :class: coverage" << std::endl << std::endl;
  std::cout << out.str() << std::endl;
}

/// Returns alphabetically sorted list of scalar functions available in Velox.
std::vector<std::string> getSortedScalarNames() {
  // Do not print "internal" functions.
  static const std::unordered_set<std::string> kBlockList = {"row_constructor"};

  auto functions = getFunctionSignatures();

  std::vector<std::string> names;
  names.reserve(functions.size());
  for (const auto& func : functions) {
    const auto& name = func.first;
    if (kBlockList.count(name) == 0) {
      names.emplace_back(name);
    }
  }
  std::sort(names.begin(), names.end());
  return names;
}

/// Returns alphabetically sorted list of aggregate functions available in
/// Velox.
std::vector<std::string> getSortedAggregateNames() {
  const auto& functions = exec::aggregateFunctions();

  std::vector<std::string> names;
  names.reserve(functions.size());
  for (const auto& entry : functions) {
    names.push_back(entry.first);
  }
  std::sort(names.begin(), names.end());
  return names;
}

void printVeloxFunctions() {
  // Do not add links for arithmetic operators.
  static const std::unordered_set<std::string> kLinkBlockList = {
      "checked_divide",
      "checked_minus",
      "checked_modulus",
      "checked_multiply",
      "checked_negate",
      "checked_plus",
      "in",
      "modulus",
      "not"};

  auto scalarNames = getSortedScalarNames();
  auto aggNames = getSortedAggregateNames();

  const int columnSize = std::max(maxLength(scalarNames), maxLength(aggNames)) +
      toFuncLink("").size();

  const std::string indent(4, ' ');

  auto scalarCnt = scalarNames.size();
  auto aggCnt = aggNames.size();
  auto numRows = std::max((size_t)std::ceil(scalarCnt / 3.0), aggCnt);

  auto printName = [&](const std::string& name) {
    return kLinkBlockList.count(name) == 0 ? toFuncLink(name) : name;
  };

  TablePrinter printer(3, columnSize, indent, std::cout);
  printer.header();
  for (int i = 0; i < numRows; i++) {
    printer.startRow();

    // 3 columns of scalar functions.
    for (int j = 0; j < 3; j++) {
      auto n = i + numRows * j;
      n < scalarCnt ? printer.addColumn(printName(scalarNames[n]))
                    : printer.addEmptyColumn();
    }

    // 1 empty column.
    printer.addEmptyColumn();

    // 1 column of aggregate functions.
    i < aggCnt ? printer.addColumn(printName(aggNames[i]))
               : printer.addEmptyColumn();

    printer.endRow();
  }
  printer.footer();
}

/// Takes a super-set of simple, vector and aggregate function names and prints
/// coverage map showing which of these functions are available in Velox.
void printCoverageMap(
    const std::vector<std::string>& scalarNames,
    const std::vector<std::string>& aggNames) {
  auto veloxFunctions = getFunctionSignatures();

  std::unordered_set<std::string> veloxNames;
  veloxNames.reserve(veloxFunctions.size());
  for (const auto& func : veloxFunctions) {
    veloxNames.emplace(func.first);
  }

  std::unordered_set<std::string> veloxAggNames;

  const auto& veloxAggregateFunctions = exec::aggregateFunctions();
  for (const auto& entry : veloxAggregateFunctions) {
    veloxAggNames.emplace(entry.first);
  }

  printCoverageMap(scalarNames, aggNames, veloxNames, veloxAggNames);
}

void printCoverageMapForAll() {
  auto scalarNames = readFunctionNamesFromFile("all_scalar_functions.txt");
  std::sort(scalarNames.begin(), scalarNames.end());

  auto aggNames = readFunctionNamesFromFile("all_aggregate_functions.txt");
  std::sort(aggNames.begin(), aggNames.end());

  printCoverageMap(scalarNames, aggNames);
}

void printCoverageMapForMostUsed() {
  auto scalarNameList = readFunctionNamesFromFile("all_scalar_functions.txt");
  std::unordered_set<std::string> scalarNames(
      scalarNameList.begin(), scalarNameList.end());

  auto aggNameList = readFunctionNamesFromFile("all_aggregate_functions.txt");
  std::unordered_set<std::string> aggNames(
      aggNameList.begin(), aggNameList.end());

  auto allMostUsed = readFunctionNamesFromFile("most_used_functions.txt");
  std::vector<std::string> scalarMostUsed;
  std::copy_if(
      allMostUsed.begin(),
      allMostUsed.end(),
      std::back_inserter(scalarMostUsed),
      [&](auto name) { return scalarNames.count(name) > 0; });

  std::vector<std::string> aggMostUsed;
  std::copy_if(
      allMostUsed.begin(),
      allMostUsed.end(),
      std::back_inserter(aggMostUsed),
      [&](auto name) { return aggNames.count(name) > 0; });

  printCoverageMap(scalarMostUsed, aggMostUsed);
}

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Register all simple and vector scalar functions.
  functions::prestosql::registerAllScalarFunctions();

  // Register Presto aggregate functions.
  aggregate::prestosql::registerAllAggregateFunctions();

  if (FLAGS_all) {
    printCoverageMapForAll();
  } else if (FLAGS_most_used) {
    printCoverageMapForMostUsed();
  } else {
    printVeloxFunctions();
  }

  return 0;
}
