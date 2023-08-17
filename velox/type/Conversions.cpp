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

#include "velox/type/Conversions.h"

DEFINE_bool(
    experimental_enable_legacy_cast,
    false,
    "Experimental feature flag for backward compatibility with previous output"
    " format of type conversions used for casting. This is a temporary solution"
    " that aims to facilitate a seamless transition for users who rely on the"
    " legacy behavior and hence can change in the future.");
