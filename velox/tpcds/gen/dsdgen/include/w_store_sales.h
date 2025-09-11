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

#ifndef W_STORE_SALES_H
#define W_STORE_SALES_H

#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/pricing.h"

int mk_w_store_sales(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);
int vld_w_store_sales(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext);
#endif
