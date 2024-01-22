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

#include "velox/expression/PrestoCastHooks.h"

namespace facebook::velox::exec {

Timestamp PrestoCastHooks::castStringToTimestamp(const StringView& view) const {
  return util::fromTimestampString(view.data(), view.size());
}

int32_t PrestoCastHooks::castStringToDate(const StringView& dateString) const {
  // Cast from string to date allows only ISO 8601 formatted strings:
  // [+-](YYYY-MM-DD).
  return util::castFromDateString(dateString, true /*isIso8601*/);
}

void PrestoCastHooks::castTimestampToString(
    const Timestamp& timestamp,
    StringWriter<false>& out) const {
  out.copy_from(
      legacyCast_
          ? util::Converter<TypeKind::VARCHAR, void, util::LegacyCastPolicy>::
                cast(timestamp)
          : util::Converter<TypeKind::VARCHAR, void, util::DefaultCastPolicy>::
                cast(timestamp));
  out.finalize();
}

bool PrestoCastHooks::legacy() const {
  return legacyCast_;
}

StringView PrestoCastHooks::removeWhiteSpaces(const StringView& view) const {
  return view;
}

bool PrestoCastHooks::truncate() const {
  return false;
}
} // namespace facebook::velox::exec
