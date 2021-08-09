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

// Collection of preprocessor test

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-W#warnings"

#ifndef __cpp_concepts
#warning "Concept support is preferred"
#else
#define ENABLE_CONCEPT 1
#endif

#ifndef __cpp_lib_ranges
#warning "Range support is preferred"
#else
#define USE_STD_RANGE 1
#endif

#pragma clang diagnostic pop

#if __cplusplus < 201703L
#error "C++17 is required"
#endif
