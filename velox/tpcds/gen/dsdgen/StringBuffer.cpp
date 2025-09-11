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

#include <assert.h>
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include "velox/tpcds/gen/dsdgen/include/StringBuffer.h"

/*
 * Routine: AddBuffer
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int AddBuffer(StringBuffer_t* pBuf, char* pStr) {
  int nRemaining = pBuf->nBytesAllocated - pBuf->nBytesUsed,
      nRequested = strlen(pStr);

  if (!nRequested)
    return (0);

  while (nRequested >= nRemaining) {
    pBuf->pText = static_cast<char*>(realloc(
        static_cast<void*>(pBuf->pText),
        pBuf->nBytesAllocated + pBuf->nIncrement));
    if (!pBuf->pText)
      return (-1);
    pBuf->nBytesAllocated += pBuf->nIncrement;
    nRemaining += pBuf->nIncrement;
  }

  strncat(pBuf->pText, pStr, pBuf->nBytesAllocated);
  if (pBuf->nBytesUsed == 0) /* first string adds a terminator */
    pBuf->nBytesUsed = 1;
  pBuf->nBytesUsed += nRequested;

  return (0);
}

/*
 * Routine: ResetStringBuffer
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int ResetBuffer(StringBuffer_t* pBuf) {
  pBuf->nBytesUsed = 0;
  if (pBuf->nBytesAllocated)
    pBuf->pText[0] = '\0';

  return (0);
}

/*
 * Routine: GetBuffer
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
char* GetBuffer(StringBuffer_t* pBuf) {
  return (pBuf->pText);
}

/*
 * Routine: FreeBuffer
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void FreeBuffer(StringBuffer_t* pBuf) {
  if (!pBuf)
    return;
  if (pBuf->pText)
    free(static_cast<void*>(pBuf->pText));
  free(static_cast<void*>(pBuf));

  return;
}
