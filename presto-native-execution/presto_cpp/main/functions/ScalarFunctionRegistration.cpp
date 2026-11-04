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
#include "presto_cpp/main/functions/ScalarFunctionRegistration.h"

#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/DateTimeCalendarFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::presto::functions {

void registerPrestoScalarFunctions(const std::string& prefix) {
  // The coordinator calls `at_timezone` whenever the AT TIME ZONE result type
  // is not TIMESTAMP, so keep the overloads that return TIMESTAMP WITH TIME
  // ZONE.
  velox::functions::prestosql::registerAllScalarFunctions(
      prefix, /*defaultOwner=*/{}, /*ignoreTimeZoneLabel=*/false);

  // With legacy_timestamp_with_timezone=false, the coordinator types AT TIME
  // ZONE on a TIMESTAMP WITH TIME ZONE value as TIMESTAMP and calls
  // `at_timezone_convert`. Velox fixes the `at_timezone` result type at
  // registration, while the session property can differ per query, so register
  // the TIMESTAMP-returning overload under the separate name.
  velox::registerFunction<
      velox::functions::AtTimezoneToTimestampFunction,
      velox::Timestamp,
      velox::TimestampWithTimezone,
      velox::Varchar>({prefix + "at_timezone_convert"}, {}, true);
}

} // namespace facebook::presto::functions
