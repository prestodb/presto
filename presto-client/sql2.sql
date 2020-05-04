WITH base AS (
    SELECT
        CAST(
          YEAR(
            DATE(agc.ds)
              ) AS VARCHAR
            ) AS year,
        CAST(
          DATE_TRUNC(
            'quarter',
            DATE(agc.ds)
              ) AS VARCHAR
            ) AS quarter_id,
        CONCAT(
          SUBSTR(agc.ds, 1, 7),
          '-01'
            ) AS month,
        agc.ultimate_planning_agency_id AS agency_ultimate_org_id,
        -- This is Facebook Org Id / CRM Id
        agc.planning_agency_name AS agency,
        agc.ultimate_planning_agency_name AS agency_ultimate,
        agc.planning_agency_operating_company AS operating_co,
        agc.planning_agency_id AS agency_org_id,
        -- This is Facebook Org Id / CRM Id
        agc.planning_agency_billing_country AS agency_country,
        adv.end_advertiser_vertical AS advertiser_vertical,
        adv.ultimate_end_advertiser_name AS advertiser_ultimate,
        adv.end_advertiser_name AS advertiser,
        adv.end_advertiser_channel AS sales_channel,
        ROUND(
          SUM(agc.legal_rev_usd_daily_rate),
          2
            ) AS total_legal_revenue,
        ROUND(
          SUM(
            agc.legal_rev_instagram_placement_usd_daily_rate
              ),
          2
            ) AS instagram_revenue,
        ROUND(
          SUM(
            agc.legal_rev_facebook_placement_usd_daily_rate
              ),
          2
            ) AS facebook_revenue,
        ROUND(
          SUM(
            agc.legal_rev_instagram_stories_placement_usd_daily_rate
              ),
          2
            ) AS instagram_stories_revenue,
        ROUND(
          SUM(
            agc.legal_rev_instream_video_placement_usd_daily_rate
              ),
          2
            ) AS instream_video_revenue,
        ROUND(
          SUM(
            agc.legal_rev_mob_instream_video_placement_usd_daily_rate
              ),
          2
            ) AS mobile_instream_video_revenue,
        ROUND(
          SUM(
            agc.legal_rev_web_instream_video_placement_usd_daily_rate
              ),
          2
            ) AS web_instream_video_revenue,
        ROUND(
          SUM(
            agc.legal_rev_messenger_placement_usd_daily_rate
              ),
          2
            ) AS messenger_revenue,
        ROUND(
          SUM(
            agc.legal_rev_custom_audience_usd_daily_rate
              ),
          2
            ) AS ca_revenue,
        ROUND(
          SUM(
            agc.legal_rev_video_usd_daily_rate
              ),
          2
            ) AS video_revenue,
        ROUND(
          SUM(
            agc.legal_rev_lt_15s_video_usd_daily_rate
              ),
          2
            ) AS fifteen_sec_vid_revenue,
        ROUND(
          SUM(
            agc.legal_rev_vertical_video_usd_daily_rate
              ),
          2
            ) AS vertical_vid_revenue,
        ROUND(
          SUM(
            agc.legal_rev_rf_usd_daily_rate
              ),
          2
            ) AS rf_revenue,
        ROUND(
          SUM(
            agc.legal_rev_dpa_usd_daily_rate
              ),
          2
            ) AS dpa_revenue,
        ROUND(
          SUM(
            agc.legal_rev_fb_pixel_usd_daily_rate
              ),
          2
            ) AS fb_pxl_revenue,
        ROUND(
          SUM(
            agc.legal_rev_audience_network_placement_usd_daily_rate
              ),
          2
            ) AS audience_network_revenue,
        ROUND(
          SUM(
            agc.legal_rev_liquidity_usd_daily_rate
              ),
          2
            ) AS platform_optimal_revenue,
        -- gms_ind_7
        ROUND(
          SUM(
            agc.legal_rev_optimal_usd_daily_rate
              ),
          2
            ) AS h_optimal_dr_revn,
        -- gms_ind 1
        ROUND(
          SUM(
            agc.legal_rev_mobile_first_usd_daily_rate
              ),
          2
            ) AS mobile_first_video_revenue,
        ROUND(
          SUM(
            agc.meaningful_business_outcome_revenue_usd_daily
              ),
          2
            ) AS mbo_revenue,
        ROUND(
          SUM(
            agc.meaningful_business_outcome_eligible_revenue_usd_daily
              ),
          2
            ) AS mbo_eligible_revenue,
        ROUND(
          SUM(
            agc.legal_rev_automatic_placement_usd_daily_rate
              ),
          2
            ) AS automatic_placements_revenue,
        ROUND(
          SUM(
            agc.legal_rev_campaign_budget_optimization_usd_daily_rate
              ),
          2
            ) cbo_revenue,
        ROUND(
          SUM(
            agc.legal_rev_consideration_funnel_usd_daily_rate
              ),
          2
            ) AS consideration_revenue,
        ROUND(
          SUM(
            agc.legal_rev_conversion_funnel_usd_daily_rate
              ),
          2
            ) AS conversion_revenue,
        ROUND(
          SUM(
            agc.legal_rev_awareness_funnel_usd_daily_rate
              ),
          2
            ) AS awareness_revenue
    FROM
        (
            SELECT
                planning_agency_id,
                account_id,
                admarket_account_currency,
                admarket_account_id,
                planning_agency_billing_country,
                planning_agency_channel,
                planning_agency_export_advertiser_country,
                planning_agency_is_gpa,
                planning_agency_is_reseller,
                planning_agency_is_ultimate_parent,
                planning_agency_legal_entity_name,
                planning_agency_name,
                planning_agency_operating_company,
                planning_agency_parent_vertical,
                planning_agency_parent_vertical_id,
                planning_agency_partnership_tier,
                planning_agency_shipping_country,
                planning_agency_telephone,
                planning_agency_tier,
                planning_agency_vertical,
                planning_agency_vertical_id,
                planning_agency_website_url,
                timezone_id,
                ultimate_planning_agency_id,
                ultimate_planning_agency_name,
                legal_rev_ad_objective_to_optimization_goal_map,
                legal_rev_advanced_matching_usd_daily,
                legal_rev_audience_network_placement_usd_daily_rate,
                legal_rev_automatic_adv_matching_usd_daily,
                legal_rev_automatic_placement_usd_daily_rate,
                legal_rev_awareness_funnel_usd_daily_rate,
                legal_rev_campaign_budget_optimization_usd_daily_rate,
                legal_rev_consideration_funnel_usd_daily_rate,
                legal_rev_conversion_funnel_usd_daily_rate,
                legal_rev_custom_audience_usd_daily_rate,
                legal_rev_dpa_usd_daily_rate,
                legal_rev_dynamic_ads_broad_audience_usd_daily,
                legal_rev_dynamic_ads_retargeting_usd_daily,
                legal_rev_facebook_placement_usd_daily_rate,
                legal_rev_fb_pixel_usd_daily_rate,
                legal_rev_feed_usd_daily_rate,
                legal_rev_instagram_placement_usd_daily_rate,
                legal_rev_instagram_stories_placement_usd_daily_rate,
                legal_rev_instream_video_placement_usd_daily_rate,
                legal_rev_lead_ad_usd_daily_rate,
                legal_rev_liquidity_usd_daily_rate,
                legal_rev_lt_15s_video_usd_daily_rate,
                legal_rev_manual_adv_matching_usd_daily,
                legal_rev_may_benefit_matching_usd_daily,
                legal_rev_messenger_placement_usd_daily_rate,
                legal_rev_mob_instream_video_placement_usd_daily_rate,
                legal_rev_mobile_first_usd_daily_rate,
                legal_rev_optimal_usd_daily_rate,
                legal_rev_optimization_goal_usd_daily_rate_map,
                legal_rev_rf_usd_daily_rate,
                legal_rev_stories_placement_usd_daily_rate,
                legal_rev_usd_daily_rate,
                legal_rev_vertical_video_usd_daily_rate,
                legal_rev_video_usd_daily_rate,
                legal_rev_web_instream_video_placement_usd_daily_rate,
                meaningful_business_outcome_eligible_revenue_usd_daily,
                meaningful_business_outcome_revenue_usd_daily,
                placement_optimal_legal_revenue_usd_daily,
                ds
            FROM
                acdp_fct_l4_account_planning_agency_planning_agency_daily_v0_immutable
            WHERE
                ds BETWEEN '2018-10-01'
                    AND '2019-11-01'
        ) agc
            LEFT OUTER JOIN (
            SELECT
                ds,
                account_id,
                end_advertiser_vertical,
                ultimate_end_advertiser_name,
                end_advertiser_name,
                end_advertiser_channel
            FROM
                (
                    SELECT
                        account_id,
                        end_advertiser_id,
                        admarket_account_currency,
                        admarket_account_id,
                        end_advertiser_billing_country,
                        end_advertiser_channel,
                        end_advertiser_is_gpa,
                        end_advertiser_is_reseller,
                        end_advertiser_legal_entity_name,
                        end_advertiser_name,
                        end_advertiser_parent_vertical,
                        end_advertiser_parent_vertical_id,
                        end_advertiser_partnership_tier,
                        end_advertiser_shipping_country,
                        end_advertiser_telephone,
                        end_advertiser_tier,
                        end_advertiser_vertical,
                        end_advertiser_vertical_id,
                        end_advertiser_website_url,
                        timezone_id,
                        ultimate_end_advertiser_id,
                        ultimate_end_advertiser_name,
                        legal_rev_usd_daily_rate,
                        ds
                    FROM
                        acdp_fct_l4_account_end_advertiser_advertiser_daily_v0_immutable
                    WHERE
                        ds BETWEEN '2018-10-01'
                            AND '2019-11-01'
                )
            WHERE
                    1 = 1
              AND (
                        end_advertiser_channel IN ('', 'SMB', 'Global Sales', 'GPA')
                    OR end_advertiser_is_gpa = '1'
                )
        ) adv ON agc.account_id = adv.account_id
            AND agc.ds = adv.ds -- joining on ds to reflect as-was tagging
    WHERE
            1 = 1
      AND agc.planning_agency_id IN (380876375818016)
      AND agc.planning_agency_is_reseller = '1'
      AND agc.planning_agency_billing_country IN (
                                                  'US',
                                                  'CA',
                                                  'PR',
                                                  'AG',
                                                  'AI',
                                                  'AN',
                                                  'AR',
                                                  'AW',
                                                  'BB',
                                                  'BM',
                                                  'BO',
                                                  'BR',
                                                  'BS',
                                                  'BZ',
                                                  'CL',
                                                  'CO',
                                                  'CR',
                                                  'CU',
                                                  'DM',
                                                  'DO',
                                                  'EC',
                                                  'FK',
                                                  'GD',
                                                  'GF',
                                                  'GP',
                                                  'GT',
                                                  'GY',
                                                  'HN',
                                                  'HT',
                                                  'JM',
                                                  'KN',
                                                  'KY',
                                                  'LC',
                                                  'MQ',
                                                  'MS',
                                                  'MX',
                                                  'NI',
                                                  'PA',
                                                  'PE',
                                                  'PY',
                                                  'SR',
                                                  'SV',
                                                  'TC',
                                                  'TT',
                                                  'UY',
                                                  'VC',
                                                  'VE',
                                                  'VG',
                                                  'VI',
                                                  'AD',
                                                  'AL',
                                                  'AM',
                                                  'AZ',
                                                  'BA',
                                                  'BG',
                                                  'BY',
                                                  'CY',
                                                  'EE',
                                                  'GE',
                                                  'HR',
                                                  'LT',
                                                  'LV',
                                                  'MD',
                                                  'ME',
                                                  'MK',
                                                  'RO',
                                                  'RS',
                                                  'SI',
                                                  'SK',
                                                  'UA',
                                                  'PL',
                                                  'CZ',
                                                  'GR',
                                                  'HU',
                                                  'RU',
                                                  'TR',
                                                  'AX',
                                                  'EU',
                                                  'GG',
                                                  'IM',
                                                  'JE',
                                                  'PM',
                                                  'SH',
                                                  'AO',
                                                  'BF',
                                                  'BI',
                                                  'BJ',
                                                  'BW',
                                                  'CD',
                                                  'CF',
                                                  'CG',
                                                  'CI',
                                                  'CM',
                                                  'CV',
                                                  'DJ',
                                                  'EH',
                                                  'ER',
                                                  'ET',
                                                  'FO',
                                                  'GA',
                                                  'GH',
                                                  'GI',
                                                  'GL',
                                                  'GM',
                                                  'GN',
                                                  'GQ',
                                                  'GW',
                                                  'IO',
                                                  'IS',
                                                  'KE',
                                                  'KM',
                                                  'LI',
                                                  'LR',
                                                  'LS',
                                                  'LU',
                                                  'MC',
                                                  'MG',
                                                  'ML',
                                                  'MR',
                                                  'MT',
                                                  'MU',
                                                  'MW',
                                                  'MZ',
                                                  'NA',
                                                  'NE',
                                                  'NG',
                                                  'RE',
                                                  'RW',
                                                  'SC',
                                                  'SD',
                                                  'SJ',
                                                  'SL',
                                                  'SM',
                                                  'SN',
                                                  'SO',
                                                  'ST',
                                                  'SZ',
                                                  'TD',
                                                  'TG',
                                                  'TZ',
                                                  'UG',
                                                  'VA',
                                                  'YT',
                                                  'ZM',
                                                  'ZW',
                                                  'FI',
                                                  'GB',
                                                  'IE',
                                                  'PT',
                                                  'AE',
                                                  'AT',
                                                  'BE',
                                                  'BH',
                                                  'CH',
                                                  'DE',
                                                  'DK',
                                                  'DZ',
                                                  'EG',
                                                  'ES',
                                                  'FR',
                                                  'IL',
                                                  'IQ',
                                                  'IR',
                                                  'IT',
                                                  'JO',
                                                  'KW',
                                                  'LB',
                                                  'LY',
                                                  'MA',
                                                  'NL',
                                                  'NO',
                                                  'OM',
                                                  'PS',
                                                  'QA',
                                                  'SA',
                                                  'SE',
                                                  'SY',
                                                  'TN',
                                                  'YE',
                                                  'ZA',
                                                  'AS',
                                                  'AU',
                                                  'BD',
                                                  'BN',
                                                  'BT',
                                                  'CK',
                                                  'CN',
                                                  'CX',
                                                  'FJ',
                                                  'FM',
                                                  'GU',
                                                  'HK',
                                                  'ID',
                                                  'IN',
                                                  'JP',
                                                  'KH',
                                                  'KI',
                                                  'KP',
                                                  'KR',
                                                  'LA',
                                                  'LK',
                                                  'MH',
                                                  'MM',
                                                  'MN',
                                                  'MO',
                                                  'MP',
                                                  'MV',
                                                  'MY',
                                                  'NC',
                                                  'NF',
                                                  'NP',
                                                  'NR',
                                                  'NU',
                                                  'NZ',
                                                  'PF',
                                                  'PG',
                                                  'PH',
                                                  'PN',
                                                  'PW',
                                                  'SB',
                                                  'SG',
                                                  'TH',
                                                  'TK',
                                                  'TO',
                                                  'TV',
                                                  'TW',
                                                  'UM',
                                                  'VN',
                                                  'VU',
                                                  'WF',
                                                  'WS',
                                                  'AF',
                                                  'KG',
                                                  'KZ',
                                                  'PK',
                                                  'TJ',
                                                  'TM',
                                                  'UZ',
                                                  'YD',
                                                  'XK',
                                                  'CC',
                                                  'TP',
                                                  'NT',
                                                  'BL',
                                                  'CS',
                                                  'HM',
                                                  'AQ',
                                                  'GS',
                                                  'MF',
                                                  'XB',
                                                  'CW',
                                                  'SS',
                                                  'TL',
                                                  'TF'
        )
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13
    UNION ALL
    SELECT
        CAST(
          YEAR(
            DATE(agc.ds)
              ) AS VARCHAR
            ) AS year,
        CAST(
          DATE_TRUNC(
            'quarter',
            DATE(agc.ds)
              ) AS VARCHAR
            ) AS quarter_id,
        CONCAT(
          SUBSTR(agc.ds, 1, 7),
          '-01'
            ) AS month,
        agc.ultimate_sold_to_id AS agency_ultimate_org_id,
        -- This is Facebook Org Id / CRM Id
        agc.sold_to_name AS agency,
        agc.sold_to_name AS agency_ultimate,
        agc.sold_to_agency_operating_company AS operating_co,
        agc.sold_to_id AS agency_org_id,
        -- This is Facebook Org Id / CRM Id
        agc.sold_to_billing_country AS agency_country,
        adv.end_advertiser_vertical AS advertiser_vertical,
        adv.ultimate_end_advertiser_name AS advertiser_ultimate,
        adv.end_advertiser_name AS advertiser,
        adv.end_advertiser_channel AS sales_channel,
        ROUND(
          SUM(agc.legal_rev_usd_daily_rate),
          2
            ) AS total_legal_revenue,
        ROUND(
          SUM(
            agc.legal_rev_instagram_placement_usd_daily_rate
              ),
          2
            ) AS instagram_revenue,
        ROUND(
          SUM(
            agc.legal_rev_facebook_placement_usd_daily_rate
              ),
          2
            ) AS facebook_revenue,
        ROUND(
          SUM(
            agc.legal_rev_instagram_stories_placement_usd_daily_rate
              ),
          2
            ) AS instagram_stories_revenue,
        ROUND(
          SUM(
            agc.legal_rev_instream_video_placement_usd_daily_rate
              ),
          2
            ) AS instream_video_revenue,
        ROUND(
          SUM(
            agc.legal_rev_mob_instream_video_placement_usd_daily_rate
              ),
          2
            ) AS mobile_instream_video_revenue,
        ROUND(
          SUM(
            agc.legal_rev_web_instream_video_placement_usd_daily_rate
              ),
          2
            ) AS web_instream_video_revenue,
        ROUND(
          SUM(
            agc.legal_rev_messenger_placement_usd_daily_rate
              ),
          2
            ) AS messenger_revenue,
        ROUND(
          SUM(
            agc.legal_rev_custom_audience_usd_daily_rate
              ),
          2
            ) AS ca_revenue,
        ROUND(
          SUM(
            agc.legal_rev_video_usd_daily_rate
              ),
          2
            ) AS video_revenue,
        ROUND(
          SUM(
            agc.legal_rev_lt_15s_video_usd_daily_rate
              ),
          2
            ) AS fifteen_sec_vid_revenue,
        ROUND(
          SUM(
            agc.legal_rev_vertical_video_usd_daily_rate
              ),
          2
            ) AS vertical_vid_revenue,
        ROUND(
          SUM(
            agc.legal_rev_rf_usd_daily_rate
              ),
          2
            ) AS rf_revenue,
        ROUND(
          SUM(
            agc.legal_rev_dpa_usd_daily_rate
              ),
          2
            ) AS dpa_revenue,
        ROUND(
          SUM(
            agc.legal_rev_fb_pixel_usd_daily_rate
              ),
          2
            ) AS fb_pxl_revenue,
        ROUND(
          SUM(
            agc.legal_rev_audience_network_placement_usd_daily_rate
              ),
          2
            ) AS audience_network_revenue,
        ROUND(
          SUM(
            agc.legal_rev_liquidity_usd_daily_rate
              ),
          2
            ) AS platform_optimal_revenue,
        -- gms_ind_7
        ROUND(
          SUM(
            agc.legal_rev_optimal_usd_daily_rate
              ),
          2
            ) AS h_optimal_dr_revn,
        -- gms_ind 1
        ROUND(
          SUM(
            agc.legal_rev_mobile_first_usd_daily_rate
              ),
          2
            ) AS mobile_first_video_revenue,
        ROUND(
          SUM(
            agc.meaningful_business_outcome_revenue_usd_daily
              ),
          2
            ) AS mbo_revenue,
        ROUND(
          SUM(
            agc.meaningful_business_outcome_eligible_revenue_usd_daily
              ),
          2
            ) AS mbo_eligible_revenue,
        ROUND(
          SUM(
            agc.legal_rev_automatic_placement_usd_daily_rate
              ),
          2
            ) AS automatic_placements_revenue,
        ROUND(
          SUM(
            agc.legal_rev_campaign_budget_optimization_usd_daily_rate
              ),
          2
            ) cbo_revenue,
        ROUND(
          SUM(
            agc.legal_rev_consideration_funnel_usd_daily_rate
              ),
          2
            ) AS consideration_revenue,
        ROUND(
          SUM(
            agc.legal_rev_conversion_funnel_usd_daily_rate
              ),
          2
            ) AS conversion_revenue,
        ROUND(
              SUM(agc.legal_rev_usd_daily_rate) - SUM(
            agc.legal_rev_consideration_funnel_usd_daily_rate
              ) - SUM(
                agc.legal_rev_conversion_funnel_usd_daily_rate
                  ),
              2
            ) AS awareness_revenue
    FROM
        (
            SELECT
                sold_to_id,
                account_id,
                admarket_account_currency,
                admarket_account_id,
                sold_to_agency_operating_company,
                sold_to_billing_country,
                sold_to_channel,
                sold_to_export_advertiser_country,
                sold_to_is_gpa,
                sold_to_is_reseller,
                sold_to_is_ultimate_parent,
                sold_to_legal_entity_name,
                sold_to_name,
                sold_to_partnership_tier,
                sold_to_shipping_country,
                sold_to_telephone,
                sold_to_tier,
                sold_to_website_url,
                timezone_id,
                ultimate_sold_to_id,
                ultimate_sold_to_name,
                legal_rev_audience_network_placement_usd_daily_rate,
                legal_rev_automatic_placement_usd_daily_rate,
                legal_rev_campaign_budget_optimization_usd_daily_rate,
                legal_rev_consideration_funnel_usd_daily_rate,
                legal_rev_conversion_funnel_usd_daily_rate,
                legal_rev_custom_audience_usd_daily_rate,
                legal_rev_dpa_usd_daily_rate,
                legal_rev_facebook_placement_usd_daily_rate,
                legal_rev_fb_pixel_usd_daily_rate,
                legal_rev_feed_usd_daily_rate,
                legal_rev_instagram_placement_usd_daily_rate,
                legal_rev_instagram_stories_placement_usd_daily_rate,
                legal_rev_instream_video_placement_usd_daily_rate,
                legal_rev_lead_ad_usd_daily_rate,
                legal_rev_liquidity_usd_daily_rate,
                legal_rev_lt_15s_video_usd_daily_rate,
                legal_rev_messenger_placement_usd_daily_rate,
                legal_rev_mob_instream_video_placement_usd_daily_rate,
                legal_rev_mobile_first_usd_daily_rate,
                legal_rev_optimal_usd_daily_rate,
                legal_rev_optimization_goal_usd_daily_rate_map,
                legal_rev_rf_usd_daily_rate,
                legal_rev_stories_placement_usd_daily_rate,
                legal_rev_usd_daily_rate,
                legal_rev_vertical_video_usd_daily_rate,
                legal_rev_video_usd_daily_rate,
                legal_rev_web_instream_video_placement_usd_daily_rate,
                meaningful_business_outcome_eligible_revenue_usd_daily,
                meaningful_business_outcome_revenue_usd_daily,
                placement_optimal_legal_revenue_usd_daily,
                ds
            FROM
                dev_acdp_fct_l4_sold_to_account_sold_to_daily_v1_immutable
            WHERE
                ds BETWEEN '2018-10-01'
                    AND '2019-11-01'
        ) agc
            JOIN (
            SELECT
                account_id,
                end_advertiser_id,
                admarket_account_currency,
                admarket_account_id,
                end_advertiser_billing_country,
                end_advertiser_channel,
                end_advertiser_is_gpa,
                end_advertiser_is_reseller,
                end_advertiser_legal_entity_name,
                end_advertiser_name,
                end_advertiser_parent_vertical,
                end_advertiser_parent_vertical_id,
                end_advertiser_partnership_tier,
                end_advertiser_shipping_country,
                end_advertiser_telephone,
                end_advertiser_tier,
                end_advertiser_vertical,
                end_advertiser_vertical_id,
                end_advertiser_website_url,
                timezone_id,
                ultimate_end_advertiser_id,
                ultimate_end_advertiser_name,
                legal_rev_usd_daily_rate,
                ds
            FROM
                acdp_fct_l4_account_end_advertiser_advertiser_daily_v0_immutable
            WHERE
                ds BETWEEN '2018-10-01'
                    AND '2019-11-01'
        ) adv ON agc.account_id = adv.account_id
            AND agc.ds = adv.ds -- joining on ds to reflect as-was tagging
    WHERE
            1 = 1
      AND agc.sold_to_id IN (380876375818016)
      AND agc.sold_to_is_reseller = '1'
      AND agc.sold_to_billing_country IN (
                                          'US',
                                          'CA',
                                          'PR',
                                          'AG',
                                          'AI',
                                          'AN',
                                          'AR',
                                          'AW',
                                          'BB',
                                          'BM',
                                          'BO',
                                          'BR',
                                          'BS',
                                          'BZ',
                                          'CL',
                                          'CO',
                                          'CR',
                                          'CU',
                                          'DM',
                                          'DO',
                                          'EC',
                                          'FK',
                                          'GD',
                                          'GF',
                                          'GP',
                                          'GT',
                                          'GY',
                                          'HN',
                                          'HT',
                                          'JM',
                                          'KN',
                                          'KY',
                                          'LC',
                                          'MQ',
                                          'MS',
                                          'MX',
                                          'NI',
                                          'PA',
                                          'PE',
                                          'PY',
                                          'SR',
                                          'SV',
                                          'TC',
                                          'TT',
                                          'UY',
                                          'VC',
                                          'VE',
                                          'VG',
                                          'VI',
                                          'AD',
                                          'AL',
                                          'AM',
                                          'AZ',
                                          'BA',
                                          'BG',
                                          'BY',
                                          'CY',
                                          'EE',
                                          'GE',
                                          'HR',
                                          'LT',
                                          'LV',
                                          'MD',
                                          'ME',
                                          'MK',
                                          'RO',
                                          'RS',
                                          'SI',
                                          'SK',
                                          'UA',
                                          'PL',
                                          'CZ',
                                          'GR',
                                          'HU',
                                          'RU',
                                          'TR',
                                          'AX',
                                          'EU',
                                          'GG',
                                          'IM',
                                          'JE',
                                          'PM',
                                          'SH',
                                          'AO',
                                          'BF',
                                          'BI',
                                          'BJ',
                                          'BW',
                                          'CD',
                                          'CF',
                                          'CG',
                                          'CI',
                                          'CM',
                                          'CV',
                                          'DJ',
                                          'EH',
                                          'ER',
                                          'ET',
                                          'FO',
                                          'GA',
                                          'GH',
                                          'GI',
                                          'GL',
                                          'GM',
                                          'GN',
                                          'GQ',
                                          'GW',
                                          'IO',
                                          'IS',
                                          'KE',
                                          'KM',
                                          'LI',
                                          'LR',
                                          'LS',
                                          'LU',
                                          'MC',
                                          'MG',
                                          'ML',
                                          'MR',
                                          'MT',
                                          'MU',
                                          'MW',
                                          'MZ',
                                          'NA',
                                          'NE',
                                          'NG',
                                          'RE',
                                          'RW',
                                          'SC',
                                          'SD',
                                          'SJ',
                                          'SL',
                                          'SM',
                                          'SN',
                                          'SO',
                                          'ST',
                                          'SZ',
                                          'TD',
                                          'TG',
                                          'TZ',
                                          'UG',
                                          'VA',
                                          'YT',
                                          'ZM',
                                          'ZW',
                                          'FI',
                                          'GB',
                                          'IE',
                                          'PT',
                                          'AE',
                                          'AT',
                                          'BE',
                                          'BH',
                                          'CH',
                                          'DE',
                                          'DK',
                                          'DZ',
                                          'EG',
                                          'ES',
                                          'FR',
                                          'IL',
                                          'IQ',
                                          'IR',
                                          'IT',
                                          'JO',
                                          'KW',
                                          'LB',
                                          'LY',
                                          'MA',
                                          'NL',
                                          'NO',
                                          'OM',
                                          'PS',
                                          'QA',
                                          'SA',
                                          'SE',
                                          'SY',
                                          'TN',
                                          'YE',
                                          'ZA',
                                          'AS',
                                          'AU',
                                          'BD',
                                          'BN',
                                          'BT',
                                          'CK',
                                          'CN',
                                          'CX',
                                          'FJ',
                                          'FM',
                                          'GU',
                                          'HK',
                                          'ID',
                                          'IN',
                                          'JP',
                                          'KH',
                                          'KI',
                                          'KP',
                                          'KR',
                                          'LA',
                                          'LK',
                                          'MH',
                                          'MM',
                                          'MN',
                                          'MO',
                                          'MP',
                                          'MV',
                                          'MY',
                                          'NC',
                                          'NF',
                                          'NP',
                                          'NR',
                                          'NU',
                                          'NZ',
                                          'PF',
                                          'PG',
                                          'PH',
                                          'PN',
                                          'PW',
                                          'SB',
                                          'SG',
                                          'TH',
                                          'TK',
                                          'TO',
                                          'TV',
                                          'TW',
                                          'UM',
                                          'VN',
                                          'VU',
                                          'WF',
                                          'WS',
                                          'AF',
                                          'KG',
                                          'KZ',
                                          'PK',
                                          'TJ',
                                          'TM',
                                          'UZ',
                                          'YD',
                                          'XK',
                                          'CC',
                                          'TP',
                                          'NT',
                                          'BL',
                                          'CS',
                                          'HM',
                                          'AQ',
                                          'GS',
                                          'MF',
                                          'XB',
                                          'CW',
                                          'SS',
                                          'TL',
                                          'TF'
        )
      AND (
                end_advertiser_channel IN ('', 'SMB', 'Global Sales', 'GPA')
            OR end_advertiser_is_gpa = '1'
        )
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13
)
SELECT
    year,
    quarter_id,
    month,
    agency_ultimate_org_id,
    agency,
    agency_ultimate,
    operating_co,
    agency_org_id,
    agency_country,
    advertiser_vertical,
    advertiser_ultimate,
    advertiser,
    sales_channel,
    ROUND(
      SUM(total_legal_revenue),
      2
        ) AS total_legal_revenue,
    ROUND(
      SUM(instagram_revenue),
      2
        ) AS instagram_revenue,
    ROUND(
      SUM(facebook_revenue),
      2
        ) AS facebook_revenue,
    ROUND(
      SUM(instagram_stories_revenue),
      2
        ) AS instagram_stories_revenue,
    ROUND(
      SUM(instream_video_revenue),
      2
        ) AS instream_video_revenue,
    ROUND(
      SUM(mobile_instream_video_revenue),
      2
        ) AS mobile_instream_video_revenue,
    ROUND(
      SUM(web_instream_video_revenue),
      2
        ) AS web_instream_video_revenue,
    ROUND(
      SUM(messenger_revenue),
      2
        ) AS messenger_revenue,
    ROUND(
      SUM(ca_revenue),
      2
        ) AS ca_revenue,
    ROUND(
      SUM(video_revenue),
      2
        ) AS video_revenue,
    ROUND(
      SUM(fifteen_sec_vid_revenue),
      2
        ) AS fifteen_sec_vid_revenue,
    ROUND(
      SUM(vertical_vid_revenue),
      2
        ) AS vertical_vid_revenue,
    ROUND(
      SUM(rf_revenue),
      2
        ) AS rf_revenue,
    ROUND(
      SUM(dpa_revenue),
      2
        ) AS dpa_revenue,
    ROUND(
      SUM(fb_pxl_revenue),
      2
        ) AS fb_pxl_revenue,
    ROUND(
      SUM(audience_network_revenue),
      2
        ) AS audience_network_revenue,
    ROUND(
      SUM(platform_optimal_revenue),
      2
        ) AS platform_optimal_revenue,
    -- gms_ind_7
    ROUND(
      SUM(h_optimal_dr_revn),
      2
        ) AS h_optimal_dr_revn,
    -- gms_ind 1
    ROUND(
      SUM(mobile_first_video_revenue),
      2
        ) AS mobile_first_video_revenue,
    ROUND(
      SUM(mbo_revenue),
      2
        ) AS mbo_revenue,
    ROUND(
      SUM(mbo_eligible_revenue),
      2
        ) AS mbo_eligible_revenue,
    ROUND(
      SUM(automatic_placements_revenue),
      2
        ) AS automatic_placements_revenue,
    ROUND(
      SUM(cbo_revenue),
      2
        ) cbo_revenue,
    ROUND(
      SUM(consideration_revenue),
      2
        ) AS consideration_revenue,
    ROUND(
      SUM(conversion_revenue),
      2
        ) AS conversion_revenue,
    ROUND(
      SUM(awareness_revenue),
      2
        ) AS awareness_revenue
FROM
    base
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13