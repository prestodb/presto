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

#ifndef TEMPLATE_H
#define TEMPLATE_H
#include "velox/tpcds/gen/dsdgen/include/StringBuffer.h"
#include "velox/tpcds/gen/dsdgen/include/expr.h" // @manual
#include "velox/tpcds/gen/dsdgen/include/list.h" // @manual
#include "velox/tpcds/gen/dsdgen/include/substitution.h" // @manual

/*  Replacement flags */
#define REPL_FL_NONE 0x0001 /*  no effect on result set size */
#define REPL_FL_MORE 0x0002 /*  likely to increase result set size */
#define REPL_FL_LESS 0x0004 /*  likely to decrease result set size */

typedef struct TEMPLATE_T {
  char* name;
  int index;
  int flags;
  list_t* SubstitutionList;
  list_t* SegmentList;
  list_t* DistList;
} template_t;
#define QT_INIT 0x0001

extern template_t* pCurrentQuery;

void PrintQuery(FILE* fp, template_t* t);
int AddQuerySegment(template_t* pQuery, char* szSQL);
int AddQuerySubstitution(
    template_t* Query,
    char* szSubName,
    int nUse,
    int nSubPart);
int AddSubstitution(template_t* t, char* s, expr_t* pExpr);
int SetSegmentFlag(template_t* Query, int nSegmentNumber, int nFlag);
substitution_t* FindSubstitution(template_t* t, char* stmt, int* nUse);
#endif
