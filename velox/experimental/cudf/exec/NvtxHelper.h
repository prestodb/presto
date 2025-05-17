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

#pragma once

#include <nvtx3/nvtx3.hpp>

#include <optional>

namespace facebook::velox::cudf_velox {

class NvtxHelper {
 public:
  NvtxHelper();
  NvtxHelper(
      nvtx3::color color,
      std::optional<int64_t> payload = std::nullopt,
      std::optional<std::string> extraInfo = std::nullopt)
      : color_(color), payload_(payload), extraInfo_(extraInfo) {}

  nvtx3::color color_{nvtx3::rgb{125, 125, 125}}; // Gray
  std::optional<int64_t> payload_{};
  std::optional<std::string> extraInfo_{};
};

/**
 * @brief Tag type for Velox's NVTX domain.
 */
struct VeloxDomain {
  static constexpr char const* name{"velox"};
};

using NvtxRegisteredStringT = nvtx3::registered_string_in<VeloxDomain>;

/**
 * @brief Extracts class and function name from a pretty function string.
 *
 * This function parses a string like:
 * "virtual facebook::velox::RowVectorPtr
 * facebook::velox::cudf_velox::CudfHashAggregation::getOutput()" and returns
 * "CudfHashAggregation::getOutput"
 *
 * @param prettyFunction The string from __PRETTY_FUNCTION__
 * @return A simplified string in the format "classname::function"
 */
constexpr std::string_view extractClassAndFunction(
    std::string_view prettyFunction) {
  // Find the last occurrence of "::" before the opening parenthesis
  auto parenPos = prettyFunction.find('(');
  if (parenPos == std::string_view::npos) {
    parenPos = prettyFunction.size();
  }

  auto lastColonPos = prettyFunction.rfind("::", parenPos);
  if (lastColonPos == std::string_view::npos) {
    return prettyFunction.substr(0, parenPos); // No class name found
  }

  // Find the previous "::" to get the start of the class name
  auto prevColonPos = prettyFunction.rfind("::", lastColonPos - 1);
  if (prevColonPos == std::string_view::npos) {
    return prettyFunction.substr(0, parenPos); // No namespace found
  }

  // Return the class and function name
  return prettyFunction.substr(prevColonPos + 2, parenPos - prevColonPos - 2);
}

#define VELOX_NVTX_OPERATOR_FUNC_RANGE()                                         \
  static_assert(                                                                 \
      std::is_base_of<NvtxHelper, std::remove_pointer<decltype(this)>::type>::   \
          value,                                                                 \
      "VELOX_NVTX_OPERATOR_FUNC_RANGE can only be used"                          \
      " in Operators derived from NvtxHelper");                                  \
  static std::string const nvtx3_func_name__{                                    \
      std::string(extractClassAndFunction(__PRETTY_FUNCTION__))};                \
  std::string const nvtx3_func_extra_info__{                                     \
      nvtx3_func_name__ + " " + this->extraInfo_.value_or("")};                  \
  ::nvtx3::event_attributes const nvtx3_func_attr__{                           \
      this->payload_.has_value() ?                                             \
          ::nvtx3::event_attributes{nvtx3_func_extra_info__, this->color_,     \
                                   nvtx3::payload{this->payload_.value()}} :   \
          ::nvtx3::event_attributes{nvtx3_func_extra_info__, this->color_}}; \
  ::nvtx3::scoped_range_in<VeloxDomain> const nvtx3_range__{nvtx3_func_attr__};

#define VELOX_NVTX_PRETTY_FUNC_RANGE()                                         \
  static NvtxRegisteredStringT const nvtx3_func_name__{                        \
      std::string(__func__) + " " + std::string(__PRETTY_FUNCTION__)};         \
  static ::nvtx3::event_attributes const nvtx3_func_attr__{nvtx3_func_name__}; \
  ::nvtx3::scoped_range_in<VeloxDomain> const nvtx3_range__{nvtx3_func_attr__};

#define VELOX_NVTX_FUNC_RANGE() NVTX3_FUNC_RANGE_IN(VeloxDomain)

} // namespace facebook::velox::cudf_velox
