/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under external/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#ifndef TEMPLATE_H
#define TEMPLATE_H
#include "StringBuffer.h"
#include "expr.h"
#include "list.h"
#include "substitution.h"

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
