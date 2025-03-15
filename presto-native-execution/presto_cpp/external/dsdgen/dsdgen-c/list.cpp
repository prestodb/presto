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

#include "list.h"
#include <assert.h>
#include <stdio.h>
#include <cstdint>
#include "config.h"
#include "error_msg.h"
#include "porting.h"

list_t* makeList(int nFlags, int (*SortFunc)(const void* d1, const void* d2)) {
  auto pRes = static_cast<list_t*>(malloc(sizeof(list_t)));
  MALLOC_CHECK(pRes);
  if (pRes == nullptr)
    ReportError(QERR_NO_MEMORY, "client list", 1);
  memset(pRes, 0, sizeof(list_t));
  pRes->nFlags = nFlags;
  pRes->pSortFunc = SortFunc;

  return (pRes);
}

list_t* addList(list_t* pList, void* pData) {
  node_t* pInsertPoint;
  unsigned int bMoveForward =
      (static_cast<uint32_t>(pList->nFlags) & L_FL_HEAD);

  auto pNode = static_cast<node_t*>(malloc(sizeof(node_t)));
  MALLOC_CHECK(pNode);
  if (!pNode)
    ReportErrorNoLine(QERR_NO_MEMORY, "client node", 1);
  memset(pNode, 0, sizeof(node_t));
  pNode->pData = pData;

  if (pList->nMembers == 0) /* first node */
  {
    pList->head = pNode;
    pList->tail = pNode;
    pList->nMembers = 1;
    return (pList);
  }

  if (static_cast<uint32_t>(pList->nFlags) & L_FL_SORT) {
    if (pList->pSortFunc(pData, pList->head->pData) <= 0) {
      /* new node become list head */
      pNode->pNext = pList->head;
      pList->head->pPrev = pNode;
      pList->head = pNode;
      pList->nMembers += 1;
      return (pList);
    }
    pInsertPoint = pList->head;

    /* find the correct point to insert new node */
    while (pInsertPoint) {
      if (pList->pSortFunc(pInsertPoint->pData, pData) < 0)
        break;
      pInsertPoint = (bMoveForward) ? pInsertPoint->pNext : pInsertPoint->pPrev;
    }
    if (pInsertPoint) /* mid-list insert */
    {
      pNode->pNext = pInsertPoint->pNext;
      pNode->pPrev = pInsertPoint;
      pInsertPoint->pNext = pNode;
    } else {
      if (bMoveForward) {
        /* new node becomes list tail */
        pNode->pPrev = pList->tail;
        pList->tail->pNext = pNode;
        pList->tail = pNode;
      } else {
        /* new node become list head */
        pNode->pNext = pList->head;
        pList->head->pPrev = pNode;
        pList->head = pNode;
      }
    }

    pList->nMembers += 1;

    return (pList);
  }

  if (static_cast<uint32_t>(pList->nFlags) & L_FL_HEAD) {
    pNode->pNext = pList->head;
    pList->head->pPrev = pNode;
    pList->head = pNode;
    pList->nMembers += 1;
  } else {
    pNode->pPrev = pList->tail;
    pList->tail->pNext = pNode;
    pList->tail = pNode;
    pList->nMembers += 1;
  }

  return (pList);
}

/*
 * Routine:
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
void* removeItem(list_t* pList, int bHead) {
  void* pResult;

  if (pList->nMembers == 0)
    return (NULL);

  if (!bHead) {
    pResult = pList->tail->pData;
    pList->tail = pList->tail->pPrev;
    pList->tail->pNext = NULL;
  } else {
    pResult = pList->head->pData;
    pList->head = pList->head->pNext;
    pList->head->pPrev = NULL;
  }

  pList->nMembers -= 1;

  return (pResult);
}

/*
 * Routine:
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
void* getHead(list_t* pList) {
  assert(pList);

  if (!pList->head)
    return (NULL);

  pList->pCurrent = pList->head;

  return (pList->pCurrent->pData);
}

/*
 * Routine:
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
void* getTail(list_t* pList) {
  assert(pList);

  if (!pList->tail)
    return (NULL);

  pList->pCurrent = pList->tail;

  return (pList->pCurrent->pData);
}

/*
 * Routine:
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
void* getNext(list_t* pList) {
  assert(pList);

  if (!pList->pCurrent->pNext)
    return (NULL);

  pList->pCurrent = pList->pCurrent->pNext;

  return (pList->pCurrent->pData);
}

/*
 * Routine:
 * Purpose: findList(list_t *pList, void *pData)
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
void* findList(list_t* pList, void* pData) {
  void* pNode;
  struct LIST_NODE_T* pOldCurrent = pList->pCurrent;

  for (pNode = getHead(pList); pNode; pNode = getNext(pList))
    if (pList->pSortFunc(pNode, pData) == 0) {
      pList->pCurrent = pOldCurrent;
      return (pNode);
    }

  pList->pCurrent = pOldCurrent;
  return (NULL);
}

/*
 * Routine:
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
void* getItem(list_t* pList, int nIndex) {
  void* pResult;
  struct LIST_NODE_T* pOldCurrent = pList->pCurrent;

  if (nIndex > length(pList))
    return (NULL);

  for (pResult = getHead(pList); --nIndex; pResult = getNext(pList))
    ;

  pList->pCurrent = pOldCurrent;
  return (pResult);
}
