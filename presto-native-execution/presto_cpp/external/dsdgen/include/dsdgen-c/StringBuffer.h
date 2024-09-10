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

#ifndef STRING_BUFFER_H
#define STRING_BUFFER_H

#define SB_INIT 0x01

typedef struct STRING_BUFFER_T {
  int nFlags;
  int nBytesAllocated;
  int nBytesUsed;
  int nIncrement;
  char* pText;
} StringBuffer_t;

StringBuffer_t* InitBuffer(int nSize, int nIncrement);
int AddBuffer(StringBuffer_t* pBuf, char* pStr);
int ResetBuffer(StringBuffer_t* pBuf);
char* GetBuffer(StringBuffer_t* pBuf);
void FreeBuffer(StringBuffer_t* pBuf);
#endif
