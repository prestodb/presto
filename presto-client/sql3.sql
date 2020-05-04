-- icm_pch_qaqc_status
    SELECT Site                                                                                                            qaqc_site
         , ("avg"(Project_Score) * 100)                                                                                    Score
         , (CASE
                WHEN (("avg"(Project_Score) * 100) > 80) THEN 1
                WHEN (("avg"(Project_Score) * 100) >= 65) THEN 2
                WHEN (("avg"(Project_Score) * 100) < 65) THEN 3
                ELSE 4 END)                                                                                                qaqc_status
         , "concat"("concat"("concat"('https://tableau.thefacebook.com/#/views/ICM-QAQCDashboard_v2/ProgramKPI?:iid=5&Project%20Name=', Site),
                             '&Status=active&Status=In-Flight&Select%20KPI%20Score=Latest%20Monthly%20KPI&month='), month) qaqc_url
         , 'Live'                                                                                                          qaqc_locktype
         , month
    FROM (
             SELECT project_name
                  , ("sum"(monthly_score_final) / "count"(project_name)) Project_Score
                  , month
                  , "count"(project_name)
                  , project_status
                  , (CASE
                         WHEN (project_name LIKE 'ATN%') THEN 'ATN'
                         WHEN (project_name LIKE 'LLA%') THEN 'LLA'
                         WHEN (project_name LIKE 'RVA%') THEN 'RVA'
                         WHEN (project_name LIKE 'ICM-RVA%') THEN 'RVA'
                         WHEN (project_name LIKE 'CCO%') THEN 'CCO'
                         WHEN (project_name LIKE 'PRN%') THEN 'PRN'
                         WHEN (project_name LIKE 'EAG%') THEN 'EAG'
                         WHEN (project_name LIKE 'VLL%') THEN 'VLL'
                         WHEN (project_name LIKE 'FTW%') THEN 'FTW'
                         WHEN (project_name LIKE 'PNB%') THEN 'PNB'
                         WHEN (project_name LIKE 'ICM-PNB-%') THEN 'PNB'
                         WHEN (project_name LIKE 'ICM-PCI%') THEN 'PCI'
                         WHEN (project_name LIKE 'PCI%') THEN 'PCI'
                         WHEN (project_name LIKE 'ATN%') THEN 'ATN'
                         WHEN (project_name LIKE 'NHA%') THEN 'NHA'
                         WHEN (project_name LIKE 'NCG%') THEN 'NCG'
                         WHEN (project_name LIKE 'NAO%') THEN 'NAO'
                         WHEN (project_name LIKE 'ICM-NAO-%') THEN 'NAO'
                         WHEN (project_name LIKE 'FRC%') THEN 'FRC'
                         WHEN (project_name LIKE 'RVA%') THEN 'RVA'
                         WHEN (project_name LIKE 'CLN%') THEN 'CLN'
                         WHEN (project_name LIKE 'ICM-CLN%') THEN 'CLN'
                         WHEN (project_name LIKE 'ODN%') THEN 'ODN'
                         WHEN (project_name LIKE 'SGA%') THEN 'SGA'
                         ELSE 'Others' END)                              site
             FROM "stg_tm_idc_bim_issue_monthly_project_qaqc:infrastructure"
             WHERE (("date"(month) = (SELECT "max"("date"(month))
                                      FROM "stg_tm_idc_bim_issue_monthly_project_qaqc:infrastructure"
             )) AND (ds = (SELECT "max"(ds)
                           FROM (
                                    SELECT ds
                                         , "count"(DISTINCT continent) count
                                    FROM "stg_tm_idc_bim_issue_monthly_project_qaqc:infrastructure"
                                    GROUP BY ds
                                )
                           WHERE (count = 2)
             )))
             GROUP BY project_name, month, project_status
         )
    WHERE ((((Project_Score IS NOT NULL) AND (NOT (project_name LIKE 'SP%'))) AND (NOT (project_name LIKE '%Substation'))) AND
           ((project_status = 'active') OR (project_status = 'In-Flight')))
    GROUP BY Site, 'Live', month
    UNION
    SELECT Site                                                                                                            qaqc_site
         , ("avg"(Project_Score) * 100)                                                                                    Score
         , (CASE
                WHEN (("avg"(Project_Score) * 100) > 80) THEN 1
                WHEN (("avg"(Project_Score) * 100) >= 65) THEN 2
                WHEN (("avg"(Project_Score) * 100) < 65) THEN 3
                ELSE 4 END)                                                                                                qaqc_status
         , "concat"("concat"("concat"('https://tableau.thefacebook.com/#/views/ICM-QAQCDashboard_v2/ProgramKPI?:iid=5&Project%20Name=', Site),
                             '&Status=active&Status=In-Flight&Select%20KPI%20Score=Latest%20Monthly%20KPI&month='), month) qaqc_url
         , 'Lock'                                                                                                          qaqc_locktype
         , month
    FROM (
             SELECT project_name
                  , ("sum"(monthly_score_final) / "count"(project_name)) Project_Score
                  , month
                  , "count"(project_name)
                  , project_status
                  , ds
                  , (CASE
                         WHEN (project_name LIKE 'ATN%') THEN 'ATN'
                         WHEN (project_name LIKE 'LLA%') THEN 'LLA'
                         WHEN (project_name LIKE 'RVA%') THEN 'RVA'
                         WHEN (project_name LIKE 'ICM-RVA%') THEN 'RVA'
                         WHEN (project_name LIKE 'CCO%') THEN 'CCO'
                         WHEN (project_name LIKE 'PRN%') THEN 'PRN'
                         WHEN (project_name LIKE 'EAG%') THEN 'EAG'
                         WHEN (project_name LIKE 'VLL%') THEN 'VLL'
                         WHEN (project_name LIKE 'FTW%') THEN 'FTW'
                         WHEN (project_name LIKE 'PNB%') THEN 'PNB'
                         WHEN (project_name LIKE 'ICM-PNB-%') THEN 'PNB'
                         WHEN (project_name LIKE 'ICM-PCI%') THEN 'PCI'
                         WHEN (project_name LIKE 'PCI%') THEN 'PCI'
                         WHEN (project_name LIKE 'ATN%') THEN 'ATN'
                         WHEN (project_name LIKE 'NHA%') THEN 'NHA'
                         WHEN (project_name LIKE 'NCG%') THEN 'NCG'
                         WHEN (project_name LIKE 'NAO%') THEN 'NAO'
                         WHEN (project_name LIKE 'ICM-NAO-%') THEN 'NAO'
                         WHEN (project_name LIKE 'FRC%') THEN 'FRC'
                         WHEN (project_name LIKE 'RVA%') THEN 'RVA'
                         WHEN (project_name LIKE 'CLN%') THEN 'CLN'
                         WHEN (project_name LIKE 'ICM-CLN%') THEN 'CLN'
                         WHEN (project_name LIKE 'ODN%') THEN 'ODN'
                         WHEN (project_name LIKE 'SGA%') THEN 'SGA'
                         ELSE 'Others' END)                              site
             FROM "stg_tm_idc_bim_issue_monthly_project_qaqc:infrastructure"
             WHERE (("date"(month) = ((SELECT "max"("date"(month))
                                       FROM "stg_tm_idc_bim_issue_monthly_project_qaqc:infrastructure"
                                      ) - INTERVAL '1' MONTH)) AND (ds = (SELECT "max"(ds)
                                                                          FROM (
                                                                                   SELECT ds
                                                                                        , "count"(DISTINCT continent) count
                                                                                   FROM "stg_tm_idc_bim_issue_monthly_project_qaqc:infrastructure"
                                                                                   GROUP BY ds
                                                                               )
                                                                          WHERE (count = 2)
             )))
             GROUP BY project_name, month, project_status, ds
         )
    WHERE (((((Project_Score IS NOT NULL) AND (NOT (project_name LIKE 'SP%'))) AND (NOT (project_name LIKE '%Substation'))) AND
            ((project_status = 'active') OR (project_status = 'In-Flight'))) AND ("date"(ds) >= (SELECT "max"("date"(month))
                                                                                                 FROM "stg_tm_idc_bim_issue_monthly_project_qaqc:infrastructure"
    )))
    GROUP BY Site, 'Lock', month


