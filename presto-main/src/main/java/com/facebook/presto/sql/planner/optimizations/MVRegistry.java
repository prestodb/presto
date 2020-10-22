/*
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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.QualifiedObjectName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MVRegistry
{
    private final Map<QualifiedObjectName, List<MVInfo>> registry;

    public MVRegistry()
    {
        this.registry = new HashMap<>();
        init();
    }

    public void init()
    {
        addSimpleMVTable();
        addAdMetricsTable();
    }

    private void addAdMetricsTable()
    {
        QualifiedObjectName baseTable = new QualifiedObjectName("prism", "nrt", "admetrics_output_nrt");
        QualifiedObjectName mvTable = new QualifiedObjectName("prism", "nrt", "rj_mv_admetrics_output_nrt_day");

        Map<String, String> expressionToMVColumnName = new HashMap<>();
        expressionToMVColumnName.put("MULTIPLY(orderkey, custkey)", "_orderkey_mult_custkey_");
        expressionToMVColumnName.put("MULTIPLY(ads_conversions_down_funnel, weight)", "_ads_conversions_down_funnel_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(ads_conversions_in_qrt, weight)", "_ads_conversions_in_qrt_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(ads_xouts, weight)", "_ads_xouts_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(price_to_value_ratio, CAST(weight))", "_price_to_value_ratio_mult_weight_");

        expressionToMVColumnName.put("MULTIPLY(instream_ads_host_video_asset_id, weight)", "_instream_ads_host_video_asset_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(targeting_delivery_signature, weight)", "_targeting_delivery_signature_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(is_instream_ads_skippable_eligible, weight)", "_is_instream_ads_skippable_eligible_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(fb_story_ads_number_of_video_to_carousel_cards, weight)", "_fb_story_ads_number_of_video_to_carousel_cards_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(clk_ecvr, weight)", "_clk_ecvr_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(subsidy_coefficient, weight)", "_subsidy_coefficient_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(price_floor, weight)", "_price_floor_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(product_raw_pre_cali_enfbr_pos0, weight)", "_product_raw_pre_cali_enfbr_pos0_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(continuity_flags, weight)", "_continuity_flags_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(user_tail_load_udv_percentile, weight)", "_user_tail_load_udv_percentile_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(dco_asset_customization_rule_id, weight)", "_dco_asset_customization_rule_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(adfinder_eroh_online_calibration_multiplier, weight)", "_adfinder_eroh_online_calibration_multiplier_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(conversion_event_type, weight)", "_conversion_event_type_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(udv_bucket_for_resource_allocation_per_country, weight)", "_udv_bucket_for_resource_allocation_per_country_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(user_head_load_udv_percentile, weight)", "_user_head_load_udv_percentile_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(freedom_cali_au_multiplier, CAST(weight))", "_freedom_cali_au_multiplier_mult_weight_");

        expressionToMVColumnName.put("MULTIPLY(app_id, weight)", "_app_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(lte_value, CAST(weight))", "_lte_value_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(dco_cta_count, weight)", "_dco_cta_count_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(internal_delivery_type, weight)", "_internal_delivery_type_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(user_head_load_udv_bucket_id, weight)", "_user_head_load_udv_bucket_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(ad_type, weight)", "_ad_type_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(count_of_this_story_type, weight)", "_count_of_this_story_type_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(product_raw_pre_cali_ecvr_pos0, CAST(weight))", "_product_raw_pre_cali_ecvr_pos0_mult_weight_");

        expressionToMVColumnName.put("MULTIPLY(ads_good_click, weight)", "_ads_good_click_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(advertiser_value_reserve_price, CAST(weight))", "_advertiser_value_reserve_price_mult_weight_");

        expressionToMVColumnName.put("MULTIPLY(dco_video_count, weight)", "_dco_video_count_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(report_over_hide_prediction_raw, CAST(weight))", "_report_over_hide_prediction_raw_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(is_instream_ad_pod_nonlive, weight)", "_is_instream_ad_pod_nonlive_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(converted_product_position, weight)", "_converted_product_position_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(qrt_budget_universe_udv_bucket_id, weight)", "_qrt_budget_universe_udv_bucket_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(repetition_penalty, CAST(weight))", "_repetition_penalty_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(iab_bounce_conv, weight)", "_iab_bounce_conv_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(adfinder_enfbr_online_calibration_multiplier, weight)", "_adfinder_enfbr_online_calibration_multiplier_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(is_dco_dof_ad, weight)", "_is_dco_dof_ad_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(dco_image_count, weight)", "_dco_image_count_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(bi_oops_score, CAST(weight))", "_bi_oops_score_mult_weight_");

        expressionToMVColumnName.put("MULTIPLY(paced_bid, CAST(weight))", "_paced_bid_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(instagram_sponsor_id, weight)", "_instagram_sponsor_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(browser_type, weight)", "_browser_type_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(app_sl_gt_qrt_version, weight)", "_app_sl_gt_qrt_version_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(external_legal_budget_usd, CAST(weight))", "_external_legal_budget_usd_mult_weight_");

        expressionToMVColumnName.put("MULTIPLY(freedom_cali_multiplier, CAST(weight))", "_freedom_cali_multiplier_mult_weight_");

        expressionToMVColumnName.put("MULTIPLY(bid, weight)", "_bid_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(qrt_quality_universe_udv_bucket_id, weight)", "_qrt_quality_universe_udv_bucket_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(primary_ad_id, weight)", "_primary_ad_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(user_dal_udv_bucket_id, weight)", "_user_dal_udv_bucket_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(udv_bucket_for_resource_allocation, weight)", "_udv_bucket_for_resource_allocation_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(feature_story_carousel_opt_in_position, weight)", "_feature_story_carousel_opt_in_position_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(device_type, weight)", "_device_type_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(ig_acqs_survey_response, weight)", "_ig_acqs_survey_response_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(gap_to_previous_ad, weight)", "_gap_to_previous_ad_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(is_related_ads_eligible, weight)", "_is_related_ads_eligible_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(product_item_id, weight)", "_product_item_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(user_tail_load_udv_bucket_id, weight)", "_user_tail_load_udv_bucket_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(freedom_exp_version, weight)", "_freedom_exp_version_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(is_targeting_b_delivery, weight)", "_is_targeting_b_delivery_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(fb_story_ads_number_of_carousel_cards_before_opt_in, weight)", "_fb_story_ads_number_of_carousel_cards_before_opt_in_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(user_dal_udv_percentile, weight)", "_user_dal_udv_percentile_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(adfinder_enfbr, weight)", "_adfinder_enfbr_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(rack_qrt_segment_id, weight)", "_rack_qrt_segment_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(is_instream_ads_skippable, weight)", "_is_instream_ads_skippable_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(aco_substituted_video_id, weight)", "_aco_substituted_video_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(unified_pixel_id, weight)", "_unified_pixel_id_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(post_iab_engagement_conv, weight)", "_post_iab_engagement_conv_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(is_from_related_ads, weight)", "_is_from_related_ads_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(max_bau_lookalike_score, CAST(weight))", "_max_bau_lookalike_score_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(carousel_smart_opt_in_prediction, CAST(weight))", "_carousel_smart_opt_in_prediction_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(multifeed_ectr, CAST(weight))", "_multifeed_ectr_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(product_raw_pre_cali_ectr_pos0, CAST(weight))", "_product_raw_pre_cali_ectr_pos0_mult_weight_");

        MVInfo simpleMVInfo = new MVInfo();
        simpleMVInfo.setBaseObject(baseTable);
        simpleMVInfo.setMvObject(mvTable);
        simpleMVInfo.setExpressionToMVColumnName(expressionToMVColumnName);

        this.registry.putIfAbsent(baseTable, new ArrayList<>());
        this.registry.get(baseTable).add(simpleMVInfo);
    }

    public List<MVInfo> getRegistry(QualifiedObjectName qualifiedObjectName)
    {
        return registry.get(qualifiedObjectName);
    }

    private void addSimpleMVTable()
    {
        QualifiedObjectName simpleBaseTable = new QualifiedObjectName("hive", "tpch", "simple_base_table");
        QualifiedObjectName simpleMVTable = new QualifiedObjectName("hive", "tpch", "simple_mv_table");

        Map<String, String> expressionToMVColumnName = new HashMap<>();
        expressionToMVColumnName.put("MULTIPLY(ads_conversions_down_funnel, weight)", "_ads_conversions_down_funnel_mult_weight_");

        MVInfo simpleMVInfo = new MVInfo();
        simpleMVInfo.setBaseObject(simpleBaseTable);
        simpleMVInfo.setMvObject(simpleMVTable);
        simpleMVInfo.setExpressionToMVColumnName(expressionToMVColumnName);

        this.registry.putIfAbsent(simpleBaseTable, new ArrayList<>());
        this.registry.get(simpleBaseTable).add(simpleMVInfo);
    }
}
