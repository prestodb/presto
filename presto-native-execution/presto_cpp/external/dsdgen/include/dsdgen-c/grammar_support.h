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
