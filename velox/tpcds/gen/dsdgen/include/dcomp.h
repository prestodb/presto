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

#ifndef DCOMP_H
#define DCOMP_H

#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/grammar.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"

/*
 * query template grammar definition
 */
#define TKN_UNKNOWN 0
#define TKN_CREATE 1
#define TKN_WEIGHTS 2
#define TKN_TYPES 3
#define TKN_INCLUDE 4
#define TKN_SET 5
#define TKN_VARCHAR 6
#define TKN_INT 7
#define TKN_ADD 8
#define TKN_DATE 9
#define TKN_DECIMAL 10
#define TKN_NAMES 11
#define MAX_TOKEN 11

int ProcessDistribution(char* s, token_t* t);
int ProcessTypes(char* s, token_t* t);
int ProcessInclude(char* s, token_t* t);
int ProcessSet(char* s, token_t* t);
int ProcessAdd(char* s, token_t* t);

#ifdef DECLARER
token_t dcomp_tokens[MAX_TOKEN + 2] = {
    {TKN_UNKNOWN, "", NULL},
    {TKN_CREATE, "create", ProcessDistribution},
    {TKN_WEIGHTS, "weights", NULL},
    {TKN_TYPES, "types", NULL},
    {TKN_INCLUDE, "#include", ProcessInclude},
    {TKN_SET, "set", ProcessSet},
    {TKN_VARCHAR, "varchar", NULL},
    {TKN_INT, "int", NULL},
    {TKN_ADD, "add", ProcessAdd},
    {TKN_DATE, "date", NULL},
    {TKN_DECIMAL, "decimal", NULL},
    {TKN_NAMES, "names", NULL},
    {-1, "", NULL}};
#else
extern token_t tokens[];
#endif

#endif /* DCOMP_H */
