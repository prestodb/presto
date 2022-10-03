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

namespace facebook::velox {
/// Used to temporarily set the value of a variable till it goes out of scope.
template <typename T>
class ScopedVarSetter {
 public:
  ScopedVarSetter(T* place, T value) : place_(place), old_(*place) {
    *place = value;
  }

  // Updates "place" to "value" if condition is true. No-op otherwise.
  ScopedVarSetter(T* place, T value, bool condition)
      : place_(place), old_(*place) {
    if (condition) {
      *place = value;
    }
  }

  ~ScopedVarSetter() {
    *place_ = old_;
  }

 private:
  T* place_;
  T old_;
};

} // namespace facebook::velox
