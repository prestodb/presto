/*
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
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>
#include "velox/core/PlanNode.h"

/// This header introduces base/abstract classes for the plan node
/// transformation framework.

namespace facebook {
namespace velox {
namespace transform {

// Transformation Base class
struct PlanNodeTransform {
  virtual ~PlanNodeTransform() {}

  /// Main entry point to run a transformation
  /// \param plan
  /// \return A new transformed plan
  virtual std::shared_ptr<core::PlanNode> transform(
      const core::PlanNode& plan) = 0;
};

auto planNodeTransformCompare = [](const PlanNodeTransform& a,
                                   const PlanNodeTransform& b) {
  return &a < &b;
};

/// Base clases for all analysis
struct PlanNodeAnalysis {
  virtual ~PlanNodeAnalysis() {}

  // Run the analysis on the given plan
  virtual void run(const core::PlanNode& plan) = 0;
};

struct transformationOptions {
  std::map<std::string, std::shared_ptr<void>> options;
};

struct transformationContext {
  transformationOptions options;
};

// A transformation sequence is a linear sequence of transformation applied to a
// planNode.
struct PlanNodeTransformSequence : public PlanNodeTransform {
  ~PlanNodeTransformSequence() override {}

  std::vector<std::reference_wrapper<PlanNodeTransform>> transformations;
  std::map<
      std::reference_wrapper<PlanNodeTransform>,
      bool,
      decltype(planNodeTransformCompare)>
      shouldRun{planNodeTransformCompare};
  PlanNodeTransformSequence(
      const std::vector<std::reference_wrapper<PlanNodeTransform>>&
          transformations_)
      : transformations(transformations_) {
    for (const auto& transformation : transformations_) {
      shouldRun[transformation] = true;
    }
  };
};

} // namespace transform
} // namespace velox
} // namespace facebook
