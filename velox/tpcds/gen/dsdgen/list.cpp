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

#include "velox/tpcds/gen/dsdgen/include/list.h"
#include <assert.h>
#include <stdio.h>
#include <cstdint>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/error_msg.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"

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
