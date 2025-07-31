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

#include "velox/functions/lib/UpperLower.h"

namespace facebook::velox::functions {

using PrestoUpperFunction = UpperLowerTemplateFunction<
    /*isLower=*/false,
    /*turkishCasing=*/false,
    /*greekFinalSigma=*/false>;
using PrestoLowerFunction = UpperLowerTemplateFunction<
    /*isLower=*/true,
    /*turkishCasing=*/false,
    /*greekFinalSigma=*/false>;

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_upper,
    PrestoUpperFunction::signatures(),
    (std::make_unique<PrestoUpperFunction>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_lower,
    PrestoLowerFunction::signatures(),
    (std::make_unique<PrestoLowerFunction>()));

} // namespace facebook::velox::functions
