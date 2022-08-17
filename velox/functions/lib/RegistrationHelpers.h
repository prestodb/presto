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

#include "velox/functions/Registerer.h"

namespace facebook::velox::functions {
namespace {

template <template <class> typename T>
void registerBinaryIntegral(const std::vector<std::string>& aliases) {
  registerFunction<T, int8_t, int8_t, int8_t>(aliases);
  registerFunction<T, int16_t, int16_t, int16_t>(aliases);
  registerFunction<T, int32_t, int32_t, int32_t>(aliases);
  registerFunction<T, int64_t, int64_t, int64_t>(aliases);
}

template <template <class> typename T>
void registerBinaryFloatingPoint(const std::vector<std::string>& aliases) {
  registerFunction<T, double, double, double>(aliases);
  registerFunction<T, float, float, float>(aliases);
}

template <template <class> typename T>
void registerBinaryNumeric(const std::vector<std::string>& aliases) {
  registerBinaryIntegral<T>(aliases);
  registerBinaryFloatingPoint<T>(aliases);
}

template <template <class> class T, typename TReturn>
void registerBinaryScalar(const std::vector<std::string>& aliases) {
  registerFunction<T, TReturn, int8_t, int8_t>(aliases);
  registerFunction<T, TReturn, int16_t, int16_t>(aliases);
  registerFunction<T, TReturn, int32_t, int32_t>(aliases);
  registerFunction<T, TReturn, int64_t, int64_t>(aliases);
  registerFunction<T, TReturn, double, double>(aliases);
  registerFunction<T, TReturn, float, float>(aliases);
  registerFunction<T, TReturn, Varchar, Varchar>(aliases);
  registerFunction<T, TReturn, Varbinary, Varbinary>(aliases);
  registerFunction<T, TReturn, bool, bool>(aliases);
  registerFunction<T, TReturn, Timestamp, Timestamp>(aliases);
  registerFunction<T, TReturn, Date, Date>(aliases);
}

template <template <class> class T, typename TReturn>
void registerNonSimdizableScalar(const std::vector<std::string>& aliases) {
  registerFunction<T, TReturn, Varchar, Varchar>(aliases);
  registerFunction<T, TReturn, Varbinary, Varbinary>(aliases);
  registerFunction<T, TReturn, bool, bool>(aliases);
  registerFunction<T, TReturn, Timestamp, Timestamp>(aliases);
  registerFunction<T, TReturn, Date, Date>(aliases);
}

template <template <class> class T>
void registerUnaryIntegral(const std::vector<std::string>& aliases) {
  registerFunction<T, int8_t, int8_t>(aliases);
  registerFunction<T, int16_t, int16_t>(aliases);
  registerFunction<T, int32_t, int32_t>(aliases);
  registerFunction<T, int64_t, int64_t>(aliases);
}

template <template <class> class T>
void registerUnaryFloatingPoint(const std::vector<std::string>& aliases) {
  registerFunction<T, double, double>(aliases);
  registerFunction<T, float, float>(aliases);
}

template <template <class> class T>
void registerUnaryNumeric(const std::vector<std::string>& aliases) {
  registerUnaryIntegral<T>(aliases);
  registerUnaryFloatingPoint<T>(aliases);
}

} // namespace

} // namespace facebook::velox::functions
