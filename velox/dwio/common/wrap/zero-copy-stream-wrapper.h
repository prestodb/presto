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

#include "velox/dwio/common/Adaptor.h"

DIAGNOSTIC_PUSH

DIAGNOSTIC_IGNORE("-Wdeprecated")
DIAGNOSTIC_IGNORE("-Wpadded")
DIAGNOSTIC_IGNORE("-Wunused-parameter")

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
#endif

#include <google/protobuf/io/zero_copy_stream.h>

DIAGNOSTIC_POP
