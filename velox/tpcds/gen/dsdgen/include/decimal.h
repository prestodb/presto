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

#ifndef R_DECIMAL_H
#define R_DECIMAL_H
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/mathops.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"

#define FL_INIT 0x0004

int itodec(decimal_t* dest, int i);
int strtodec(decimal_t* d, const char* src);

#define dectoi(d) atoi(d->number)

int decimal_t_op(decimal_t* dest, int o, decimal_t* d1, decimal_t* d2);
void set_precision(decimal_t* d, int sie, int precision);
#define NegateDecimal(d) (d)->number *= -1
#endif /* R_DECIMAL_H */
