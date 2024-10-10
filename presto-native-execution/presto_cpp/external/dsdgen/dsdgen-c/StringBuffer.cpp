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

#include <assert.h>
#include <stdio.h>
#include "config.h"
#include "porting.h"
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include "StringBuffer.h"

/*
 * Routine: InitBuffer
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
StringBuffer_t* InitBuffer(int nSize, int nIncrement) {
  StringBuffer_t* pBuf;

  pBuf = (StringBuffer_t*)malloc(sizeof(struct STRING_BUFFER_T));
  MALLOC_CHECK(pBuf);
  if (pBuf == NULL)
    return (NULL);
  memset((void*)pBuf, 0, sizeof(struct STRING_BUFFER_T));

  pBuf->pText = (char*)malloc(sizeof(char) * nSize);
  MALLOC_CHECK(pBuf->pText);
  if (pBuf->pText == NULL)
    return (NULL);
  memset((void*)pBuf->pText, 0, sizeof(char) * nSize);

  pBuf->nIncrement = nIncrement;
  pBuf->nBytesAllocated = nSize;
  pBuf->nFlags = SB_INIT;

  return (pBuf);
}

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
    pBuf->pText = (char*)realloc(
        (void*)pBuf->pText, pBuf->nBytesAllocated + pBuf->nIncrement);
    if (!pBuf->pText)
      return (-1);
    pBuf->nBytesAllocated += pBuf->nIncrement;
    nRemaining += pBuf->nIncrement;
  }

  strcat(pBuf->pText, pStr);
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
    free((void*)pBuf->pText);
  free((void*)pBuf);

  return;
}
