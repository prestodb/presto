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

#ifndef DS_ADDRESS_H
#define DS_ADDRESS_H

#include "constants.h"
#include "dist.h"

#define DS_ADDR_SUITE_NUM 0
#define DS_ADDR_STREET_NUM 1
#define DS_ADDR_STREET_NAME1 2
#define DS_ADDR_STREET_NAME2 3
#define DS_ADDR_STREET_TYPE 4
#define DS_ADDR_CITY 5
#define DS_ADDR_COUNTY 6
#define DS_ADDR_STATE 7
#define DS_ADDR_COUNTRY 8
#define DS_ADDR_ZIP 9
#define DS_ADDR_PLUS4 10
#define DS_ADDR_GMT_OFFSET 11

int mk_address(ds_addr_t* pDest, int nColumn, DSDGenContext& dsdGenContext);
int mk_streetnumber(int nTable, int* dest, DSDGenContext& dsdGenContext);
int mk_suitenumber(int nTable, char* dest, DSDGenContext& dsdGenContext);
int mk_streetname(int nTable, char* dest);
int mk_city(int nTable, char** dest, DSDGenContext& dsdGenContext);
int city_hash(int nTable, char* name);
int mk_zipcode(
    int nTable,
    char* dest,
    int nRegion,
    char* city,
    DSDGenContext& dsdGenContext);
// void printAddressPart(FILE *fp, ds_addr_t *pAddr, int nAddressPart);
void resetCountCount(void);

#endif
