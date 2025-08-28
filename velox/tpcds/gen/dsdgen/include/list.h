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

#ifndef LIST_H
#define LIST_H
typedef struct LIST_NODE_T {
  struct LIST_NODE_T* pNext;
  struct LIST_NODE_T* pPrev;
  void* pData;
} node_t;

typedef struct LIST_T {
  struct LIST_NODE_T* head;
  struct LIST_NODE_T* tail;
  struct LIST_NODE_T* pCurrent;
  int (*pSortFunc)(const void* pD1, const void* pD2);
  int nMembers;
  int nFlags;
} list_t;

/* list_t flags */
#define L_FL_HEAD 0x01 /* add at head */
#define L_FL_TAIL 0x02 /* add at tail */
#define L_FL_SORT 0x04 /* create sorted list */

#define length(list) list->nMembers

list_t* makeList(
    int nFlags,
    int (*pSortFunc)(const void* pD1, const void* pD2));
list_t* addList(list_t* pList, void* pData);
void* findList(list_t* pList, void* pData);
void* removeItem(list_t* pList, int bFromHead);
void* getHead(list_t* pList);
void* getTail(list_t* pList);
void* getNext(list_t* pList);
void* getItem(list_t* pList, int nIndex);
#endif
