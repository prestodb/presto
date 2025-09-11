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
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under tpcds/gen/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#ifndef R_APPEND_H
#define R_APPEND_H

#include <stdbool.h>
#include <stdlib.h>

#include "velox/tpcds/gen/dsdgen/include/decimal.h"

typedef void* append_info;

append_info* append_info_get(void* info_list, int table_id);

void append_row_start(append_info info);
void append_row_end(append_info info);

void append_varchar(
    int32_t column,
    append_info info,
    const char* value,
    bool fillEmptyStringAsNull = true);
void append_varchar(
    int32_t column,
    append_info info,
    std::string value,
    bool fillEmptyStringAsNull = true);
void append_key(int32_t column, append_info info, int64_t value);
void append_date(int32_t column, append_info info, int64_t value);
void append_integer(int32_t column, append_info info, int32_t value);
void append_decimal(int32_t column, append_info info, decimal_t* val);
void append_boolean(int32_t column, append_info info, int32_t val);
void append_integer_decimal(int32_t column, append_info info, int32_t val);

#endif
