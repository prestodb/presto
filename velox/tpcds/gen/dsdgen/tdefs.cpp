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

#include "velox/tpcds/gen/dsdgen/include/tdefs.h"
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/r_params.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdef_functions.h"

/*
 * Routine: get_rowcount(int table)
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
ds_key_t GetRowcountByName(char* szName, DSDGenContext& dsdGenContext) {
  int nTable = -1;

  nTable = GetTableNumber(szName, dsdGenContext);
  if (nTable >= 0)
    return (get_rowcount(nTable - 1, dsdGenContext));

  nTable = distsize(szName, dsdGenContext);
  return (nTable);
}

/*
 * Routine: GetTableNumber(char *szName, DSDGenContext& dsdGenContext)
 * Purpose: Return size of table, pseudo table or distribution
 * Algorithm: Need to use rowcount distribution, since argument could be a
 * pseudo table Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int GetTableNumber(char* szName, DSDGenContext& dsdGenContext) {
  int i;

  for (i = 1; i <= distsize("rowcounts", dsdGenContext); i++) {
    char* szTable = new char[100];
    dist_member(szTable, "rowcounts", i, 1, dsdGenContext);
    if (strcasecmp(szTable, szName) == 0)
      return (i - 1);
  }

  return (-1);
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
/*
tdef *
getTdefsByNumber(int nTable)
{
   if (is_set("UPDATE"))
   {
      if (s_tdefs[nTable].flags & FL_PASSTHRU)
      {
         switch(nTable + S_BRAND)
         {
         case S_CATALOG_PAGE: nTable = CATALOG_PAGE; break;
         case S_CUSTOMER_ADDRESS: nTable = CUSTOMER_ADDRESS; break;
         case S_PROMOTION: nTable = PROMOTION; break;
         }
         return(&w_tdefs[nTable]);
      }
      else
         return(&s_tdefs[nTable]);
   }
    else
        return(&w_tdefs[nTable]);
}
*/
tdef* getSimpleTdefsByNumber(int nTable, DSDGenContext& dsdGenContext) {
  if (nTable >= S_BRAND)
    return (&dsdGenContext.s_tdefs[nTable - S_BRAND]);
  return (&dsdGenContext.w_tdefs[nTable]);
}

tdef* getTdefsByNumber(int nTable, DSDGenContext& dsdGenContext) {
  if (is_set("UPDATE", dsdGenContext) && is_set("VALIDATE", dsdGenContext)) {
    if (static_cast<unsigned int>(dsdGenContext.s_tdefs[nTable].flags) &
        FL_PASSTHRU) {
      switch (nTable + S_BRAND) {
        case S_CATALOG_PAGE:
          nTable = CATALOG_PAGE;
          break;
        case S_CUSTOMER_ADDRESS:
          nTable = CUSTOMER_ADDRESS;
          break;
        case S_PROMOTION:
          nTable = PROMOTION;
          break;
      }
      return (&dsdGenContext.w_tdefs[nTable]);
    } else
      return (&dsdGenContext.s_tdefs[nTable]);
  }

  return (getSimpleTdefsByNumber(nTable, dsdGenContext));
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
const char* getTableNameByID(int i, DSDGenContext& dsdGenContext) {
  tdef* pT = getSimpleTdefsByNumber(i, dsdGenContext);

  return (pT->name);
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
int getTableFromColumn(int nColumn, DSDGenContext& dsdGenContext) {
  int i;
  tdef* pT;

  for (i = 0; i <= MAX_TABLE; i++) {
    pT = getSimpleTdefsByNumber(i, dsdGenContext);
    if ((nColumn >= pT->nFirstColumn) && (nColumn <= pT->nLastColumn))
      return (i);
  }
  return (-1);
}
