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

#include "dbgen_version.h"
#include <stdio.h>
#include <time.h>
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "dist.h"
#include "misc.h"
#include "porting.h"
#include "tables.h"

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

  auto result = sprintf(
      r->szDate,
      "%4d-%02d-%02d",
      pTimeStamp->tm_year + 1900,
      pTimeStamp->tm_mon + 1,
      pTimeStamp->tm_mday);
  if (result < 0)
    perror("sprintf failed");
  result = sprintf(
      r->szTime,
      "%02d:%02d:%02d",
      pTimeStamp->tm_hour,
      pTimeStamp->tm_min,
      pTimeStamp->tm_sec);
  if (result < 0)
    perror("sprintf failed");
  result = sprintf(
      r->szVersion, "%d.%d.%d%s", VERSION, RELEASE, MODIFICATION, PATCH);
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
