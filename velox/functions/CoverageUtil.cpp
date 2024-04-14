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

#include "velox/functions/CoverageUtil.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::velox::functions {

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
    out_ << "  ==  " << line << "  ==  " << line << std::endl;

    auto scalarFunctionsColumnWidth =
        columnSize_ * numScalarColumns_ + 2 * (numScalarColumns_ - 1);

    out_ << indent_ << std::left << std::setw(scalarFunctionsColumnWidth)
         << "Scalar Functions" << "      " << std::setw(columnSize_)
         << "Aggregate Functions" << "      " << "Window Functions"
         << std::endl;
    out_ << indent_ << std::string(scalarFunctionsColumnWidth, '=')
         << "  ==  " << line << "  ==  " << line << std::endl;
  }

  void startRow() {
    out_ << indent_;
    currentColumn_ = 0;
  }

  void addColumn(const std::string& text) {
    addColumn(text, columnSize_);
  }

  void addEmptyColumn() {
    if (currentColumn_ == numScalarColumns_ ||
        currentColumn_ == numScalarColumns_ + 2) {
      // If the current column is after the Scalar Functions columns or
      // the column next to Aggregate Functions column.
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
    out_ << "  ==  " << line << "  ==  " << line << std::endl;
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

std::string toFuncLink(
    const std::string& name,
    const std::string& domain = "") {
  return fmt::format("{}:func:`{}`", domain, name);
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

/// Prints out CSS rules to
/// - add lightblue background to table header;
/// - add lightblue background to an empty column that separates scalar,
/// aggregate, and window functions;
/// - highlight cells identified by TableCellTracker.
void printTableCss(
    size_t numScalarColumns,
    const TableCellTracker& cellTracker,
    std::ostream& out) {
  out << "    div.body {max-width: 1300px;}" << std::endl;
  out << "    table.coverage th {background-color: lightblue; text-align: center;}"
      << std::endl;
  out << "    table.coverage " << "td:nth-child(" << numScalarColumns + 1
      << ") " << "{background-color: lightblue;}" << std::endl;
  out << "    table.coverage " << "td:nth-child(" << numScalarColumns + 3
      << ") " << "{background-color: lightblue;}" << std::endl;

  for (const auto& entry : cellTracker.cells()) {
    out << "    table.coverage " << "tr:nth-child(" << entry.first + 1 << ") "
        << "td:nth-child(" << entry.second + 1 << ") "
        << "{background-color: #6BA81E;}" << std::endl;
  }
}

void printCoverageMap(
    const std::vector<std::string>& scalarNames,
    const std::vector<std::string>& aggNames,
    const std::vector<std::string>& windowNames,
    const std::unordered_set<std::string>& veloxNames,
    const std::unordered_set<std::string>& veloxAggNames,
    const std::unordered_set<std::string>& veloxWindowNames,
    const std::string& domain) {
  const auto scalarCnt = scalarNames.size();
  const auto aggCnt = aggNames.size();
  const auto windowCnt = windowNames.size();

  // Make sure there is enough space for the longest function name + :func:
  // syntax that turns function name into a link to function's description.
  const int columnSize = std::max(
                             {maxLength(scalarNames),
                              maxLength(aggNames),
                              maxLength(windowNames)}) +
      toFuncLink("", domain).size();

  const std::string indent(4, ' ');

  const int numScalarColumns = 5;

  // Split scalar functions into 'numScalarColumns' columns. Put all aggregate
  // functions into one column.
  auto numRows = std::max(
      {(size_t)std::ceil((double)scalarCnt / numScalarColumns),
       aggCnt,
       windowCnt});

  // Keep track of cells which contain functions available in Velox. These cells
  // need to be highlighted using CSS rules.
  TableCellTracker veloxCellTracker;

  auto printName = [&](int row,
                       int column,
                       const std::string& name,
                       const std::unordered_set<std::string>& veloxNames) {
    if (veloxNames.count(name)) {
      veloxCellTracker.add(row, column);
      return toFuncLink(name, domain);
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

    // 1 empty column.
    printer.addEmptyColumn();

    // 1 column of window functions.
    i < windowCnt
        ? printer.addColumn(printName(
              i, numScalarColumns + 3, windowNames[i], veloxWindowNames))
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

// A function name is a companion function's if the name is an existing
// aggregation functio name followed by a specific suffixes.
bool isCompanionFunctionName(
    const std::string& name,
    const std::unordered_map<std::string, exec::AggregateFunctionEntry>&
        aggregateFunctions) {
  auto suffixOffset = name.rfind("_partial");
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_merge_extract");
  }
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_merge");
  }
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_extract");
  }
  if (suffixOffset == std::string::npos) {
    return false;
  }
  return aggregateFunctions.count(name.substr(0, suffixOffset)) > 0;
}

/// Returns alphabetically sorted list of scalar functions available in Velox,
/// excluding companion functions.
std::vector<std::string> getSortedScalarNames() {
  // Do not print "internal" functions.
  static const std::unordered_set<std::string> kBlockList = {"row_constructor"};

  auto functions = getFunctionSignatures();

  std::vector<std::string> names;
  names.reserve(functions.size());
  exec::aggregateFunctions().withRLock([&](const auto& aggregateFunctions) {
    for (const auto& func : functions) {
      const auto& name = func.first;
      if (!isCompanionFunctionName(name, aggregateFunctions) &&
          kBlockList.count(name) == 0) {
        names.emplace_back(name);
      }
    }
  });
  std::sort(names.begin(), names.end());
  return names;
}

/// Returns alphabetically sorted list of aggregate functions available in
/// Velox, excluding compaion functions.
std::vector<std::string> getSortedAggregateNames() {
  std::vector<std::string> names;
  exec::aggregateFunctions().withRLock([&](const auto& functions) {
    names.reserve(functions.size());
    for (const auto& entry : functions) {
      if (!isCompanionFunctionName(entry.first, functions)) {
        names.push_back(entry.first);
      }
    }
  });
  std::sort(names.begin(), names.end());
  return names;
}

/// Returns alphabetically sorted list of window functions available in Velox,
/// excluding companion functions.
std::vector<std::string> getSortedWindowNames() {
  const auto& functions = exec::windowFunctions();

  std::vector<std::string> names;
  names.reserve(functions.size());
  exec::aggregateFunctions().withRLock([&](const auto& aggregateFunctions) {
    for (const auto& entry : functions) {
      if (!isCompanionFunctionName(entry.first, aggregateFunctions) &&
          aggregateFunctions.count(entry.first) == 0) {
        names.emplace_back(entry.first);
      }
    }
  });
  std::sort(names.begin(), names.end());
  return names;
}

/// Takes a super-set of simple, vector and aggregate function names and prints
/// coverage map showing which of these functions are available in Velox.
/// Companion functions are excluded.
void printCoverageMap(
    const std::vector<std::string>& scalarNames,
    const std::vector<std::string>& aggNames,
    const std::vector<std::string>& windowNames,
    const std::string& domain = "") {
  auto veloxFunctions = getFunctionSignatures();

  std::unordered_set<std::string> veloxNames;
  veloxNames.reserve(veloxFunctions.size());
  for (const auto& func : veloxFunctions) {
    veloxNames.emplace(func.first);
  }

  std::unordered_set<std::string> veloxAggNames;
  std::unordered_set<std::string> veloxWindowNames;
  const auto& veloxWindowFunctions = exec::windowFunctions();

  exec::aggregateFunctions().withRLock(
      [&](const auto& veloxAggregateFunctions) {
        for (const auto& entry : veloxAggregateFunctions) {
          if (!isCompanionFunctionName(entry.first, veloxAggregateFunctions)) {
            veloxAggNames.emplace(entry.first);
          }
        }
        for (const auto& entry : veloxWindowFunctions) {
          if (!isCompanionFunctionName(entry.first, veloxAggregateFunctions) &&
              veloxAggregateFunctions.count(entry.first) == 0) {
            veloxWindowNames.emplace(entry.first);
          }
        }
      });

  printCoverageMap(
      scalarNames,
      aggNames,
      windowNames,
      veloxNames,
      veloxAggNames,
      veloxWindowNames,
      domain);
}

void printCoverageMapForAll(const std::string& domain) {
  auto scalarNames = readFunctionNamesFromFile("all_scalar_functions.txt");
  std::sort(scalarNames.begin(), scalarNames.end());

  auto aggNames = readFunctionNamesFromFile("all_aggregate_functions.txt");
  std::sort(aggNames.begin(), aggNames.end());

  auto windowNames = readFunctionNamesFromFile("all_window_functions.txt");
  std::sort(windowNames.begin(), windowNames.end());

  printCoverageMap(scalarNames, aggNames, windowNames, domain);
}

void printVeloxFunctions(
    const std::unordered_set<std::string>& linkBlockList,
    const std::string& domain) {
  auto scalarNames = getSortedScalarNames();
  auto aggNames = getSortedAggregateNames();
  auto windowNames = getSortedWindowNames();

  const int columnSize = std::max(
                             {maxLength(scalarNames),
                              maxLength(aggNames),
                              maxLength(windowNames)}) +
      toFuncLink("", domain).size();

  const std::string indent(4, ' ');

  auto scalarCnt = scalarNames.size();
  auto aggCnt = aggNames.size();
  auto windowCnt = windowNames.size();
  auto numRows =
      std::max({(size_t)std::ceil(scalarCnt / 3.0), aggCnt, windowCnt});

  auto printName = [&](const std::string& name) {
    return linkBlockList.count(name) == 0 ? toFuncLink(name, domain) : name;
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

    // 1 empty column.
    printer.addEmptyColumn();

    // 1 column of window functions.
    i < windowCnt ? printer.addColumn(printName(windowNames[i]))
                  : printer.addEmptyColumn();

    printer.endRow();
  }
  printer.footer();
}

void printCoverageMapForMostUsed(const std::string& domain) {
  auto scalarNameList = readFunctionNamesFromFile("all_scalar_functions.txt");
  std::unordered_set<std::string> scalarNames(
      scalarNameList.begin(), scalarNameList.end());

  auto aggNameList = readFunctionNamesFromFile("all_aggregate_functions.txt");
  std::unordered_set<std::string> aggNames(
      aggNameList.begin(), aggNameList.end());

  auto windowNameList = readFunctionNamesFromFile("all_window_functions.txt");
  std::unordered_set<std::string> windowNames(
      windowNameList.begin(), windowNameList.end());

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

  std::vector<std::string> windowMostUsed;
  std::copy_if(
      allMostUsed.begin(),
      allMostUsed.end(),
      std::back_inserter(windowMostUsed),
      [&](auto name) { return windowNames.count(name) > 0; });

  printCoverageMap(scalarMostUsed, aggMostUsed, windowMostUsed, domain);
}

} // namespace facebook::velox::functions
