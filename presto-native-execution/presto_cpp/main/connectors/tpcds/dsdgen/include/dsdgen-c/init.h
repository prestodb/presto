/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors:
 * Gradient Systems
 */

#ifndef DS_INIT_H
#define DS_INIT_H

struct InitConstants {
  int init_rand_init;
  int mk_address_init;
  int setUpdateDateRange_init;
  int mk_dbgen_version_init;
  int getCatalogNumberFromPage_init;
  int checkSeeds_init;
  int dateScaling_init;
  int mk_w_call_center_init;
  int mk_w_catalog_page_init;
  int mk_master_catalog_sales_init;
  int dectostr_init;
  int date_join_init;
  int setSCDKeys_init;
  int scd_join_init;
  int matchSCDSK_init;
  int skipDays_init;
  int mk_w_catalog_returns_init;
  int mk_detail_catalog_sales_init;
  int mk_w_customer_init;
  int mk_w_date_init;
  int mk_w_inventory_init;
  int mk_w_item_init;
  int mk_w_promotion_init;
  int mk_w_reason_init;
  int mk_w_ship_mode_init;
  int mk_w_store_returns_init;
  int mk_master_store_sales_init;
  int mk_w_store_init;
  int mk_w_web_page_init;
  int mk_w_web_returns_init;
  int mk_master_init;
  int mk_detail_init;
  int mk_w_web_site_init;
  int mk_cust_init;
  int mk_order_init;
  int mk_part_init;
  int mk_supp_init;
  int dbg_text_init;
  int find_dist_init;
  int cp_join_init;
  int web_join_init;
  int set_pricing_init;
  int init_params_init;
  int get_rowcount_init;

  void Reset();
};

#endif
