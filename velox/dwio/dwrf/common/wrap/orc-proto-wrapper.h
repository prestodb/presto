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
DIAGNOSTIC_IGNORE("-Wconversion")
DIAGNOSTIC_IGNORE("-Wdeprecated")
DIAGNOSTIC_IGNORE("-Wsign-conversion")
DIAGNOSTIC_IGNORE("-Wunused-parameter")

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wnested-anon-types")
DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
DIAGNOSTIC_IGNORE("-Wshorten-64-to-32")
DIAGNOSTIC_IGNORE("-Wweak-vtables")
#endif

#include "velox/dwio/dwrf/proto/orc_proto.pb.h"

DIAGNOSTIC_POP
