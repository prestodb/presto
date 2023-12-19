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

#include "velox/dwio/dwrf/test/OrcFiles.h"

using namespace facebook::velox::type::fbhive;

///////////////////////////////////////////////////////////////////////////////
///////////////////////////// Simple struct file //////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/*
clang-format off

Fields:
Field 0, Name: root, Type: STRUCT
  Field 1, Column 0, Name: a, Type: INT
  Field 2, Column 1, Name: b, Type: STRUCT
    Field 3, Column 1, Name: a, Type: INT
    Field 4, Column 1, Name: b, Type: FLOAT
    Field 5, Column 1, Name: c, Type: STRING
  Field 6, Column 2, Name: c, Type: FLOAT

Stripes:
Stripe 0: number of rows: 1, offset: 3, length: 420
        index length: 216, data length: 33, footer length: 171
        raw data size: 19, checksum: -3525804471458565161, group size: 0

clang-format on
*/
namespace facebook::velox::dwrf {
const std::string& getStructFile() {
  static const std::string structFile_ = getExampleFilePath("struct.orc");
  return structFile_;
}

///////////////////////////////////////////////////////////////////////////////
////////////////// FM small file- flatmap encoded map fields //////////////////
///////////////////////////////////////////////////////////////////////////////

/*
clang-format off

Fields:
Field 0, Name: root, Type: STRUCT
  Field 1, Column 0, Name: id, Type: INT
  Field 2, Column 1, Name: map1, Type: MAP
    Field 3, Column 1, Name: key, Type: INT
    Field 4, Column 1, Name: value, Type: LIST
      Field 5, Column 1, Name: item, Type: FLOAT
  Field 6, Column 2, Name: map2, Type: MAP
    Field 7, Column 2, Name: key, Type: STRING
    Field 8, Column 2, Name: value, Type: MAP
      Field 9, Column 2, Name: key, Type: SHORT
      Field 10, Column 2, Name: value, Type: LONG
  Field 11, Column 3, Name: map3, Type: MAP
    Field 12, Column 3, Name: key, Type: INT
    Field 13, Column 3, Name: value, Type: INT
  Field 14, Column 4, Name: map4, Type: MAP
    Field 15, Column 4, Name: key, Type: INT
    Field 16, Column 4, Name: value, Type: STRUCT
      Field 17, Column 4, Name: field1, Type: INT
      Field 18, Column 4, Name: field2, Type: FLOAT
      Field 19, Column 4, Name: field3, Type: STRING
  Field 20, Column 5, Name: memo, Type: STRING


Stripes:
Stripe 0: number of rows: 300, offset: 3, length: 94,783
        index length: 7,221, data length: 84,909, footer length: 2,653
        raw data size: 179,993, checksum: 5901778347563170914, group size: 84,909
Stripe 1: number of rows: 300, offset: 94786, length: 101,452
        index length: 7,226, data length: 91,561, footer length: 2,665
        raw data size: 189,079, checksum: 3404351558338649814, group size: 91,561
Stripe 2: number of rows: 300, offset: 196238, length: 103,043
        index length: 7,231, data length: 93,159, footer length: 2,653
        raw data size: 193,250, checksum: -3135465507877383748, group size: 93,159
Stripe 3: number of rows: 100, offset: 299281, length: 47,427
        index length: 7,013, data length: 37,835, footer length: 2,579
        raw data size: 64,643, checksum: -8658965514269281311, group size: 37,835

clang-format on
*/
const std::string& getFMSmallFile() {
  static const std::string fmSmallFile_ = getExampleFilePath("fm_small.orc");
  return fmSmallFile_;
}

///////////////////////////////////////////////////////////////////////////////
// FM large file- same schema as FM small, but contains more rows per stripe //
///////////////////////////////////////////////////////////////////////////////

/*
clang-format off

Fields:
Field 0, Name: root, Type: STRUCT
  Field 1, Column 0, Name: id, Type: INT
  Field 2, Column 1, Name: map1, Type: MAP
    Field 3, Column 1, Name: key, Type: INT
    Field 4, Column 1, Name: value, Type: LIST
      Field 5, Column 1, Name: item, Type: FLOAT
  Field 6, Column 2, Name: map2, Type: MAP
    Field 7, Column 2, Name: key, Type: STRING
    Field 8, Column 2, Name: value, Type: MAP
      Field 9, Column 2, Name: key, Type: SHORT
      Field 10, Column 2, Name: value, Type: LONG
  Field 11, Column 3, Name: map3, Type: MAP
    Field 12, Column 3, Name: key, Type: INT
    Field 13, Column 3, Name: value, Type: INT
  Field 14, Column 4, Name: map4, Type: MAP
    Field 15, Column 4, Name: key, Type: INT
    Field 16, Column 4, Name: value, Type: STRUCT
      Field 17, Column 4, Name: field1, Type: INT
      Field 18, Column 4, Name: field2, Type: FLOAT
      Field 19, Column 4, Name: field3, Type: STRING
  Field 20, Column 5, Name: memo, Type: STRING

Stripes:
Stripe 0: number of rows: 3,000, offset: 3, length: 874,811
        index length: 20,564, data length: 851,376, footer length: 2,871
        raw data size: 1,866,915, checksum: 5060529436512968808, group size: 851,376
Stripe 1: number of rows: 3,000, offset: 874814, length: 884,182
        index length: 20,734, data length: 860,538, footer length: 2,910
        raw data size: 1,879,912, checksum: 2163495802508399877, group size: 860,538
Stripe 2: number of rows: 3,000, offset: 1758996, length: 883,123
        index length: 20,723, data length: 859,469, footer length: 2,931
        raw data size: 1,880,439, checksum: 1864059226478411868, group size: 859,469
Stripe 3: number of rows: 1,000, offset: 2642119, length: 294,399
        index length: 7,198, data length: 284,523, footer length: 2,678
        raw data size: 619,154, checksum: -1554821492627335802, group size: 284,523

clang-format on
*/

const std::string& getFMLargeFile() {
  static const std::string fmLargeFile_ = getExampleFilePath("fm_large.orc");
  return fmLargeFile_;
}

// RowType for FM small and FM large files
const std::shared_ptr<const RowType>& getFlatmapSchema() {
  static const std::shared_ptr<const RowType> schema_ =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
         id:int,\
     map1:map<int, array<float>>,\
     map2:map<string, map<smallint,bigint>>,\
     map3:map<int,int>,\
     map4:map<int,struct<field1:int,field2:float,field3:string>>,\
     memo:string>"));
  return schema_;
}

} // namespace facebook::velox::dwrf
