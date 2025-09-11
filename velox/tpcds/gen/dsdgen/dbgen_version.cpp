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

#include "velox/tpcds/gen/dsdgen/include/dbgen_version.h"
#include <stdio.h>
#include <time.h>
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/misc.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"

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
int mk_dbgen_version(
    void* pDest,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext) {
  time_t ltime = time_t();
  struct tm* pTimeStamp = nullptr;
  struct DBGEN_VERSION_TBL* r;

  if (pDest == NULL)
    r = &dsdGenContext.g_dbgen_version;
  else
    r = static_cast<DBGEN_VERSION_TBL*>(pDest);

  if (!dsdGenContext.mk_dbgen_version_init) {
    memset(&dsdGenContext.g_dbgen_version, 0, sizeof(struct DBGEN_VERSION_TBL));
    dsdGenContext.mk_dbgen_version_init = 1;
  }

  time(&ltime); /* Get time in seconds */
  pTimeStamp = localtime(&ltime); /* Convert time to struct */

  auto result = snprintf(
      r->szDate,
      sizeof(r->szDate),
      "%4d-%02d-%02d",
      pTimeStamp->tm_year + 1900,
      pTimeStamp->tm_mon + 1,
      pTimeStamp->tm_mday);
  if (result < 0)
    perror("sprintf failed");
  result = snprintf(
      r->szTime,
      sizeof(r->szTime),
      "%02d:%02d:%02d",
      pTimeStamp->tm_hour,
      pTimeStamp->tm_min,
      pTimeStamp->tm_sec);
  if (result < 0)
    perror("sprintf failed");
  result = snprintf(
      r->szVersion,
      sizeof(r->szVersion),
      "%d.%d.%d%s",
      VERSION,
      RELEASE,
      MODIFICATION,
      PATCH);
  if (result < 0)
    perror("sprintf failed");
  strcpy(r->szCmdLineArgs, "--this_table_is_rather_pointless");

  return (0);
}
//
///*
// * Routine:
// * Purpose:
// * Algorithm:
// * Data Structures:
// *
// * Params:
// * Returns:
// * Called By:
// * Calls:
// * Assumptions:
// * Side Effects:
// * TODO: None
// */
// int pr_dbgen_version(void *pSrc) {
//	struct DBGEN_VERSION_TBL *r;
//
//	if (pSrc == NULL)
//		r = &g_dbgen_version;
//	else
//		r = pSrc;
//
//	print_start(DBGEN_VERSION);
//	print_varchar(DV_VERSION, r->szVersion, 1);
//	print_varchar(DV_CREATE_DATE, r->szDate, 1);
//	print_varchar(DV_CREATE_TIME, r->szTime, 1);
//	print_varchar(DV_CMDLINE_ARGS, r->szCmdLineArgs, 0);
//	print_end(DBGEN_VERSION);
//
//	return (0);
//}
