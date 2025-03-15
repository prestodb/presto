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

#ifndef PARALLEL_H
#define PARALLEL_H

int split_work(
    int nTable,
    ds_key_t* pkFirstRow,
    ds_key_t* pkRowCount,
    DSDGenContext& dsdGenContext);
int row_stop(int tbl, DSDGenContext& dsdGenContext);
int row_skip(int tbl, ds_key_t count, DSDGenContext& dsdGenContext);

#endif /* PARALLEL_H */
