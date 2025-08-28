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

#ifndef GRAMMAR_SUPPORT_H
#define GRAMMAR_SUPPORT_H
/*
 * entry in the file stack used to manage multiple input file and include files
 */
typedef struct FILE_REF_T {
  FILE* file;
  char* name;
  int line_number;
  void* pContext;
  struct FILE_REF_T* pNext;
#if defined(MKS) || defined(FLEX)
  void* pLexState;
#endif
} file_ref_t;

extern file_ref_t* pCurrentFile;

int yywarn(char* str);
void yyerror(char* msg, ...);
int setup(void);
int include_file(char* fn, void* pContext);
void GetErrorCounts(int* nError, int* nWarning);
#endif
