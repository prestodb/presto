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

#include <string>

#include <re2/re2.h>

#include "folly/CPortability.h"

#include "velox/common/base/Exceptions.h"
#include "velox/functions/lib/Re2Functions.h"

namespace facebook::velox::functions {
template <typename T>
using Re2RegexpReplacePresto = Re2RegexpReplace<
    T,
    prepareRegexpReplacePattern,
    prepareRegexpReplaceReplacement>;

} // namespace facebook::velox::functions
