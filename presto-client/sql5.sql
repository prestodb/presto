-- icm_pch_financials_status
    SELECT M.finance_site
         , M.finance_ds
         , MI.project_group                                                                                                                         min_etc_project_group
         , M.finance_min_etc_savings
         , MA.project_group                                                                                                                         max_etc_project_group
         , M.finance_max_etc_savings
         , IF(((M.finance_max_etc_savings > 1.2E-1) AND (M.finance_min_etc_savings >= 2E-2)), MA.project_group, MI.project_group)                   "finance_etc_project_group"
         , IF(((M.finance_max_etc_savings > 1.2E-1) AND (M.finance_min_etc_savings >= 2E-2)), M.finance_max_etc_savings, M.finance_min_etc_savings) "finance_etc_savings"
         , "concat"('https://tableau.thefacebook.com/#/views/ProjectFinancialHealthDashboard/FinancialHealthLock?site=', M.finance_site)            "finance_url"
         , 'Lock'                                                                                                                                   finance_locktype
    FROM (((
        SELECT site               finance_site
             , ds                 finance_ds
             , "min"(etc_savings) "finance_min_etc_savings"
             , "max"(etc_savings) "finance_max_etc_savings"
        FROM (
                 SELECT *
                      , (budget - (eac - contingency))                                                        "finance_potential_savings"
                      , CAST(((budget - (eac - contingency)) / NULLIF((eac - actuals), 0)) AS decimal(10, 5)) "etc_savings"
                 FROM (
                          SELECT p.site
                               , p.project_group
                               , p.ds
                               , "sum"(bi.currentbudget)                                                      "budget"
                               , "sum"(bi.estimateatcompletion)                                               "eac"
                               , "sum"(IF(("WBSLevelOne" = '900 - Contingency'), bi.estimateatcompletion, 0)) "contingency"
                               , "sum"(bi.actualsreceived)                                                    "actuals"
                          FROM (v_idc_eb_projects_fb p
                                   INNER JOIN (
                              SELECT *
                                   , (CASE
                                          WHEN (bi.segment1value = '100') THEN '100 - Real Estate'
                                          WHEN (bi.segment1value = '200') THEN '200 - Soft Costs'
                                          WHEN (bi.segment1value = '300') THEN '300 - Hard Costs'
                                          WHEN (bi.segment1value = '350') THEN '350 - Substation'
                                          WHEN (bi.segment1value = '360') THEN '360 - DCC'
                                          WHEN (bi.segment1value = '400') THEN '400 - OFE'
                                          WHEN (bi.segment1value = '900') THEN '900 - Contingency'
                                          ELSE 'Other' END) "WBSLevelOne"
                              FROM idc_eb_budget_items_fb bi
                              WHERE (ds = (SELECT "max"(forecast_date)
                                           FROM icm_cost_ctrl_cost_tblu
                                           WHERE (ds = (SELECT "max"(ds)
                                                        FROM icm_cost_ctrl_cost_tblu
                                           ))
                              ))
                          ) bi ON (p.project_id = bi.projectid))
                          WHERE ((((p.ds = (SELECT "max"(forecast_date)
                                            FROM icm_cost_ctrl_cost_tblu
                                            WHERE (ds = (SELECT "max"(ds)
                                                         FROM icm_cost_ctrl_cost_tblu
                                            ))
                          )) AND (p.status IN ('Active'))) AND (p.project_owner = 'New Builds')) AND ("WBSLevelOne" <> 'Unknown'))
                          GROUP BY p.site, p.project_group, p.ds
                          ORDER BY p.site ASC, p.project_group ASC, p.ds ASC
                      )
             )
        GROUP BY site, ds
        ORDER BY site ASC, ds ASC
    ) M
        LEFT JOIN (
            SELECT site               finance_site
                 , project_group
                 , ds                 finance_ds
                 , "min"(etc_savings) "finance_min_etc_savings"
            FROM (
                     SELECT *
                          , (budget - (eac - contingency))                                                        "finance_potential_savings"
                          , CAST(((budget - (eac - contingency)) / NULLIF((eac - actuals), 0)) AS decimal(10, 5)) "etc_savings"
                     FROM (
                              SELECT p.site
                                   , p.project_group
                                   , p.ds
                                   , "sum"(bi.currentbudget)                                                      "budget"
                                   , "sum"(bi.estimateatcompletion)                                               "eac"
                                   , "sum"(IF(("WBSLevelOne" = '900 - Contingency'), bi.estimateatcompletion, 0)) "contingency"
                                   , "sum"(bi.actualsreceived)                                                    "actuals"
                              FROM (v_idc_eb_projects_fb p
                                       INNER JOIN (
                                  SELECT *
                                       , (CASE
                                              WHEN (bi.segment1value = '100') THEN '100 - Real Estate'
                                              WHEN (bi.segment1value = '200') THEN '200 - Soft Costs'
                                              WHEN (bi.segment1value = '300') THEN '300 - Hard Costs'
                                              WHEN (bi.segment1value = '350') THEN '350 - Substation'
                                              WHEN (bi.segment1value = '360') THEN '360 - DCC'
                                              WHEN (bi.segment1value = '400') THEN '400 - OFE'
                                              WHEN (bi.segment1value = '900') THEN '900 - Contingency'
                                              ELSE 'Other' END) "WBSLevelOne"
                                  FROM idc_eb_budget_items_fb bi
                                  WHERE (ds = (SELECT "max"(forecast_date)
                                               FROM icm_cost_ctrl_cost_tblu
                                               WHERE (ds = (SELECT "max"(ds)
                                                            FROM icm_cost_ctrl_cost_tblu
                                               ))
                                  ))
                              ) bi ON (p.project_id = bi.projectid))
                              WHERE ((((p.ds = (SELECT "max"(forecast_date)
                                                FROM icm_cost_ctrl_cost_tblu
                                                WHERE (ds = (SELECT "max"(ds)
                                                             FROM icm_cost_ctrl_cost_tblu
                                                ))
                              )) AND (p.status IN ('Active'))) AND (p.project_owner = 'New Builds')) AND ("WBSLevelOne" <> 'Unknown'))
                              GROUP BY p.site, p.project_group, p.ds
                              ORDER BY p.site ASC, p.project_group ASC, p.ds ASC
                          )
                 )
            GROUP BY site, project_group, ds
            ORDER BY site ASC, project_group ASC, ds ASC
        ) MI ON ((M.finance_site = MI.finance_site) AND (M.finance_min_etc_savings = MI.finance_min_etc_savings)))
             LEFT JOIN (
        SELECT site               finance_site
             , project_group
             , ds                 finance_ds
             , "max"(etc_savings) "finance_max_etc_savings"
        FROM (
                 SELECT *
                      , (budget - (eac - contingency))                                                        "finance_potential_savings"
                      , CAST(((budget - (eac - contingency)) / NULLIF((eac - actuals), 0)) AS decimal(10, 5)) "etc_savings"
                 FROM (
                          SELECT p.site
                               , p.project_group
                               , p.ds
                               , "sum"(bi.currentbudget)                                                      "budget"
                               , "sum"(bi.estimateatcompletion)                                               "eac"
                               , "sum"(IF(("WBSLevelOne" = '900 - Contingency'), bi.estimateatcompletion, 0)) "contingency"
                               , "sum"(bi.actualsreceived)                                                    "actuals"
                          FROM (v_idc_eb_projects_fb p
                                   INNER JOIN (
                              SELECT *
                                   , (CASE
                                          WHEN (bi.segment1value = '100') THEN '100 - Real Estate'
                                          WHEN (bi.segment1value = '200') THEN '200 - Soft Costs'
                                          WHEN (bi.segment1value = '300') THEN '300 - Hard Costs'
                                          WHEN (bi.segment1value = '350') THEN '350 - Substation'
                                          WHEN (bi.segment1value = '360') THEN '360 - DCC'
                                          WHEN (bi.segment1value = '400') THEN '400 - OFE'
                                          WHEN (bi.segment1value = '900') THEN '900 - Contingency'
                                          ELSE 'Other' END) "WBSLevelOne"
                              FROM idc_eb_budget_items_fb bi
                              WHERE (ds = (SELECT "max"(forecast_date)
                                           FROM icm_cost_ctrl_cost_tblu
                                           WHERE (ds = (SELECT "max"(ds)
                                                        FROM icm_cost_ctrl_cost_tblu
                                           ))
                              ))
                          ) bi ON (p.project_id = bi.projectid))
                          WHERE ((((p.ds = (SELECT "max"(forecast_date)
                                            FROM icm_cost_ctrl_cost_tblu
                                            WHERE (ds = (SELECT "max"(ds)
                                                         FROM icm_cost_ctrl_cost_tblu
                                            ))
                          )) AND (p.status IN ('Active'))) AND (p.project_owner = 'New Builds')) AND ("WBSLevelOne" <> 'Unknown'))
                          GROUP BY p.site, p.project_group, p.ds
                          ORDER BY p.site ASC, p.project_group ASC, p.ds ASC
                      )
             )
        GROUP BY site, project_group, ds
        ORDER BY site ASC, project_group ASC, ds ASC
    ) MA ON ((M.finance_site = MA.finance_site) AND (M.finance_max_etc_savings = MA.finance_max_etc_savings)))
    UNION ALL
    SELECT M.finance_site
         , M.finance_ds
         , MI.project_group                                                                                                                         min_etc_project_group
         , M.finance_min_etc_savings
         , MA.project_group                                                                                                                         max_etc_project_group
         , M.finance_max_etc_savings
         , IF(((M.finance_max_etc_savings > 1.2E-1) AND (M.finance_min_etc_savings >= 2E-2)), MA.project_group, MI.project_group)                   "finance_etc_project_group"
         , IF(((M.finance_max_etc_savings > 1.2E-1) AND (M.finance_min_etc_savings >= 2E-2)), M.finance_max_etc_savings, M.finance_min_etc_savings) "finance_etc_savings"
         , "concat"('https://tableau.thefacebook.com/#/views/ProjectFinancialHealthDashboardLive/FinancialHealthLive?site=', M.finance_site)        "finance_url"
         , 'Live'                                                                                                                                   finance_locktype
    FROM (((
        SELECT site               finance_site
             , ds                 finance_ds
             , "min"(etc_savings) "finance_min_etc_savings"
             , "max"(etc_savings) "finance_max_etc_savings"
        FROM (
                 SELECT *
                      , (budget - (eac - contingency))                                                        "finance_potential_savings"
                      , CAST(((budget - (eac - contingency)) / NULLIF((eac - actuals), 0)) AS decimal(10, 5)) "etc_savings"
                 FROM (
                          SELECT p.site
                               , p.project_group
                               , p.ds
                               , "sum"(bi.currentbudget)                                                      "budget"
                               , "sum"(bi.estimateatcompletion)                                               "eac"
                               , "sum"(IF(("WBSLevelOne" = '900 - Contingency'), bi.estimateatcompletion, 0)) "contingency"
                               , "sum"(bi.actualsreceived)                                                    "actuals"
                          FROM (v_idc_eb_projects_fb p
                                   INNER JOIN (
                              SELECT *
                                   , (CASE
                                          WHEN (bi.segment1value = '100') THEN '100 - Real Estate'
                                          WHEN (bi.segment1value = '200') THEN '200 - Soft Costs'
                                          WHEN (bi.segment1value = '300') THEN '300 - Hard Costs'
                                          WHEN (bi.segment1value = '350') THEN '350 - Substation'
                                          WHEN (bi.segment1value = '360') THEN '360 - DCC'
                                          WHEN (bi.segment1value = '400') THEN '400 - OFE'
                                          WHEN (bi.segment1value = '900') THEN '900 - Contingency'
                                          ELSE 'Other' END) "WBSLevelOne"
                              FROM idc_eb_budget_items_fb bi
                              WHERE (ds = (SELECT "max"(ds)
                                           FROM idc_eb_budget_items_fb
                              ))
                          ) bi ON (p.project_id = bi.projectid))
                          WHERE ((((p.ds = (SELECT "max"(ds)
                                            FROM idc_eb_budget_items_fb
                          )) AND (p.status IN ('Active'))) AND (p.project_owner = 'New Builds')) AND ("WBSLevelOne" <> 'Unknown'))
                          GROUP BY p.site, p.project_group, p.ds
                          ORDER BY p.site ASC, p.project_group ASC, p.ds ASC
                      )
             )
        GROUP BY site, ds
        ORDER BY site ASC, ds ASC
    ) M
        LEFT JOIN (
            SELECT site               finance_site
                 , project_group
                 , ds                 finance_ds
                 , "min"(etc_savings) "finance_min_etc_savings"
            FROM (
                     SELECT *
                          , (budget - (eac - contingency))                                                        "finance_potential_savings"
                          , CAST(((budget - (eac - contingency)) / NULLIF((eac - actuals), 0)) AS decimal(10, 5)) "etc_savings"
                     FROM (
                              SELECT p.site
                                   , p.project_group
                                   , p.ds
                                   , "sum"(bi.currentbudget)                                                      "budget"
                                   , "sum"(bi.estimateatcompletion)                                               "eac"
                                   , "sum"(IF(("WBSLevelOne" = '900 - Contingency'), bi.estimateatcompletion, 0)) "contingency"
                                   , "sum"(bi.actualsreceived)                                                    "actuals"
                              FROM (v_idc_eb_projects_fb p
                                       INNER JOIN (
                                  SELECT *
                                       , (CASE
                                              WHEN (bi.segment1value = '100') THEN '100 - Real Estate'
                                              WHEN (bi.segment1value = '200') THEN '200 - Soft Costs'
                                              WHEN (bi.segment1value = '300') THEN '300 - Hard Costs'
                                              WHEN (bi.segment1value = '350') THEN '350 - Substation'
                                              WHEN (bi.segment1value = '360') THEN '360 - DCC'
                                              WHEN (bi.segment1value = '400') THEN '400 - OFE'
                                              WHEN (bi.segment1value = '900') THEN '900 - Contingency'
                                              ELSE 'Other' END) "WBSLevelOne"
                                  FROM idc_eb_budget_items_fb bi
                                  WHERE (ds = (SELECT "max"(ds)
                                               FROM idc_eb_budget_items_fb
                                  ))
                              ) bi ON (p.project_id = bi.projectid))
                              WHERE ((((p.ds = (SELECT "max"(ds)
                                                FROM idc_eb_budget_items_fb
                              )) AND (p.status IN ('Active'))) AND (p.project_owner = 'New Builds')) AND ("WBSLevelOne" <> 'Unknown'))
                              GROUP BY p.site, p.project_group, p.ds
                              ORDER BY p.site ASC, p.project_group ASC, p.ds ASC
                          )
                 )
            GROUP BY site, project_group, ds
            ORDER BY site ASC, project_group ASC, ds ASC
        ) MI ON ((M.finance_site = MI.finance_site) AND (M.finance_min_etc_savings = MI.finance_min_etc_savings)))
             LEFT JOIN (
        SELECT site               finance_site
             , project_group
             , ds                 finance_ds
             , "max"(etc_savings) "finance_max_etc_savings"
        FROM (
                 SELECT *
                      , (budget - (eac - contingency))                                                        "finance_potential_savings"
                      , CAST(((budget - (eac - contingency)) / NULLIF((eac - actuals), 0)) AS decimal(10, 5)) "etc_savings"
                 FROM (
                          SELECT p.site
                               , p.project_group
                               , p.ds
                               , "sum"(bi.currentbudget)                                                      "budget"
                               , "sum"(bi.estimateatcompletion)                                               "eac"
                               , "sum"(IF(("WBSLevelOne" = '900 - Contingency'), bi.estimateatcompletion, 0)) "contingency"
                               , "sum"(bi.actualsreceived)                                                    "actuals"
                          FROM (v_idc_eb_projects_fb p
                                   INNER JOIN (
                              SELECT *
                                   , (CASE
                                          WHEN (bi.segment1value = '100') THEN '100 - Real Estate'
                                          WHEN (bi.segment1value = '200') THEN '200 - Soft Costs'
                                          WHEN (bi.segment1value = '300') THEN '300 - Hard Costs'
                                          WHEN (bi.segment1value = '350') THEN '350 - Substation'
                                          WHEN (bi.segment1value = '360') THEN '360 - DCC'
                                          WHEN (bi.segment1value = '400') THEN '400 - OFE'
                                          WHEN (bi.segment1value = '900') THEN '900 - Contingency'
                                          ELSE 'Other' END) "WBSLevelOne"
                              FROM idc_eb_budget_items_fb bi
                              WHERE (ds = (SELECT "max"(ds)
                                           FROM idc_eb_budget_items_fb
                              ))
                          ) bi ON (p.project_id = bi.projectid))
                          WHERE ((((p.ds = (SELECT "max"(ds)
                                            FROM idc_eb_budget_items_fb
                          )) AND (p.status IN ('Active'))) AND (p.project_owner = 'New Builds')) AND ("WBSLevelOne" <> 'Unknown'))
                          GROUP BY p.site, p.project_group, p.ds
                          ORDER BY p.site ASC, p.project_group ASC, p.ds ASC
                      )
             )
        GROUP BY site, project_group, ds
        ORDER BY site ASC, project_group ASC, ds ASC
    ) MA ON ((M.finance_site = MA.finance_site) AND (M.finance_max_etc_savings = MA.finance_max_etc_savings)))