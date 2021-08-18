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

/*
 * This header is used to select between std::ranges and range-v3 depending on
 * the build env
 */
#pragma once

#if USE_STD_RANGE
#include <ranges> // @manual
using namespace ranges = std::ranges
#else
#include <range/v3/range.hpp>
#include <range/v3/view.hpp>
namespace views = ranges::views;
#endif
