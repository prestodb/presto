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
namespace facebook::presto::protocol {

void to_json(json& j, const HiveStorageFormat& p) {
  throw ParseError("Not implemented");
}

static const std::pair<HiveStorageFormat, json> HiveStorageFormat_enum_table[] =
    { // NOLINT: cert-err58-cpp
        {HiveStorageFormat::ORC, "ORC"},
        {HiveStorageFormat::DWRF, "DWRF"},
        {HiveStorageFormat::PARQUET, "PARQUET"},
        {HiveStorageFormat::AVRO, "AVRO"},
        {HiveStorageFormat::RCBINARY, "RCBINARY"},
        {HiveStorageFormat::RCTEXT, "RCTEXT"},
        {HiveStorageFormat::SEQUENCEFILE, "SEQUENCEFILE"},
        {HiveStorageFormat::JSON, "JSON"},
        {HiveStorageFormat::TEXTFILE, "TEXTFILE"},
        {HiveStorageFormat::CSV, "CSV"},
        {HiveStorageFormat::PAGEFILE, "PAGEFILE"}};

void from_json(const json& j, HiveStorageFormat& e) {
  static_assert(
      std::is_enum<HiveStorageFormat>::value,
      "HiveStorageFormat must be an enum!");
  const auto* it = std::find_if(
      std::begin(HiveStorageFormat_enum_table),
      std::end(HiveStorageFormat_enum_table),
      [&j](const std::pair<HiveStorageFormat, json>& ej_pair) -> bool {
        return ej_pair.second == j;
      });
  e = ((it != std::end(HiveStorageFormat_enum_table))
           ? it
           : std::begin(HiveStorageFormat_enum_table))
          ->first;
}
} // namespace facebook::presto::protocol
