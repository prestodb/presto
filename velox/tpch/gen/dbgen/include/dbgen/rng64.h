/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under extension/tpch/dbgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */
#pragma once

#include "dbgen/config.h" // @manual
#include "dbgen/dss.h" // @manual

DSS_HUGE AdvanceRand64(DSS_HUGE nSeed, DSS_HUGE nCount);
void dss_random64(DSS_HUGE* tgt, DSS_HUGE nLow, DSS_HUGE nHigh, seed_t* seed);
DSS_HUGE NextRand64(DSS_HUGE nSeed);
