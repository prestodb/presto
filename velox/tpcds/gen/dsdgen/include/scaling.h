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

#include "velox/tpcds/gen/dsdgen/include/dist.h"

#ifndef SCALING_H
#define SCALING_H

ds_key_t get_rowcount(int table, DSDGenContext& dsdGenContext);
ds_key_t getIDCount(int nTable, DSDGenContext& dsdGenContext);
int getUpdateID(ds_key_t* pDest, int nTable, int nColumn);
int getScaleSlot(int nTargetGB, DSDGenContext& dsdGenContext);
int getSkewedJulianDate(int nWeight, int nColumn, DSDGenContext& dsdGenContext);
ds_key_t dateScaling(int nColumn, ds_key_t jDate, DSDGenContext& dsdGenContext);
int getUpdateDate(int nTable, ds_key_t kRowcount, DSDGenContext& dsdGenContext);
void setUpdateDates(DSDGenContext& dsdGenContext);
void setUpdateScaling(int nTable, DSDGenContext& dsdGenContext);
ds_key_t getUpdateBase(int nTable, DSDGenContext& dsdGenContext);

#endif
