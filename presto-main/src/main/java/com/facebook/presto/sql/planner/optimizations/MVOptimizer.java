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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.SystemSessionProperties.MV_OPTIMIZATION_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MVOptimizer
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(MVOptimizer.class);
    private static final HashMap<String, MVInfo> mvTableNames = new HashMap<>();
    private static final HashMap<String, String> expressionToMVColumnName = new HashMap<>();
    private static final HashSet<String> dimensions = new HashSet<>();
    private final Metadata metadata;
    private final String baseTableName;

    public MVOptimizer(Metadata metadata)
    {
        //TODO create hierarchy under the qualified name.
        log.info("Initiating MVOptimizer!");
        this.metadata = requireNonNull(metadata, "metadata is null");

        MVInfo rjTableMVInfo = new MVInfo(new QualifiedObjectName("hive", "tpch", "rj_mv_base_table_simple"),
                new QualifiedObjectName("hive", "tpch", "rj_mv_mv_table_simple"));

        MVInfo simpleTableMVInfo = new MVInfo(new QualifiedObjectName("hive", "tpch", "simple_base_table"),
                new QualifiedObjectName("hive", "tpch", "simple_mv_table"));

        MVInfo adMetricsMVInfo = new MVInfo(new QualifiedObjectName("prism", "nrt", "admetrics_output_nrt"),
                new QualifiedObjectName("prism", "nrt", "rj_mv_admetrics_output_nrt_multi_expr"));

        mvTableNames.put("rj_mv_base_table_simple", rjTableMVInfo);
        expressionToMVColumnName.put("MULTIPLY(id1, id6)", "_id1_mult_id6_");
        expressionToMVColumnName.put("MULTIPLY(id2, id6)", "_id2_mult_id6_");
        expressionToMVColumnName.put("MULTIPLY(id3, id6)", "_id3_mult_id6_");

        mvTableNames.put("simple_base_table", simpleTableMVInfo);
        expressionToMVColumnName.put("MULTIPLY(id1, id2)", "_id1_mult_id2_");
        expressionToMVColumnName.put("MULTIPLY(id3, CAST(id2))", "_id3_mult_id2_");

        mvTableNames.put("admetrics_output_nrt", adMetricsMVInfo);

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


        dimensions.add("ds");
        dimensions.add("time");
        dimensions.add("conv_purpose_flag_ads_score");
        dimensions.add("is_ranking_legal");
        dimensions.add("category");
        dimensions.add("page_type_int");
        dimensions.add("realized_adevent_trait_type");
        dimensions.add("country");
        dimensions.add("ad_event_primary_type");
        dimensions.add("is_lift");
        dimensions.add("ad_attribution_event");
        dimensions.add("conversion_type");
        dimensions.add("page_tab");
        dimensions.add("ad_objective");
        dimensions.add("account_id");
        dimensions.add("is_legal");
        dimensions.add("advertiser_bqrt_version");
        dimensions.add("ad_pivot_type");
        dimensions.add("business_id");
        dimensions.add("mobile_os");
        dimensions.add("ad_optimization_goal");
        dimensions.add("adfinder_region");
        dimensions.add("optimized_ad_primary_event_types");
        dimensions.add("qrt_quality_emerging_surface_ccd_version");
        dimensions.add("campaign_id");
        dimensions.add("qrt_feed_ads_quality_2_version");
        dimensions.add("qrt_fb_story_ads_optimization_2_version");
        dimensions.add("qrt_feed_ads_quality_ccd_version");
        dimensions.add("page_id");
        dimensions.add("account_type");
        dimensions.add("ad_event_sub_type");

        baseTableName = "simple_base_table";
    }

    class MVInfo
    {
        QualifiedObjectName mvObject;
        QualifiedObjectName baseObject;

        public MVInfo(QualifiedObjectName baseObject, QualifiedObjectName mvObject)
        {
            this.mvObject = mvObject;
            this.baseObject = baseObject;
        }
    }

    @Override
    public PlanNode optimize(
            PlanNode planNode,
            Session session,
            TypeProvider types,
            PlanVariableAllocator planVariableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        log.info("MVOptimizer optimize is called!");
        if (!isMVEnabled(session)) {
            log.info("MVOptimizer is not enabled, returning the rootnode.");
            return planNode;
        }

        MVInfo mvInfo = mvTableNames.get(baseTableName);
        QualifiedObjectName tableQualifiedName = mvInfo.baseObject;
        QualifiedObjectName mvQualifiedObjectName = mvInfo.mvObject;

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableQualifiedName);
        Optional<TableHandle> mvTableHandle = metadata.getTableHandle(session, mvQualifiedObjectName);

        log.info("Checking it base table is present.");
        checkState(tableHandle.isPresent(), String.format("Base Table [%s] is not present", tableHandle));

        //TODO: Use ODS counter.
        log.info("Checking it MV table is present.");
        if (!mvTableHandle.isPresent()) {
            log.info("MV table is not present, returning the root node");
            log.error("MV Table handle is not present. MV Table name: %s", mvQualifiedObjectName);
            return planNode;
        }

        TableMetadata tableMetadata = metadata.getTableMetadata(session, mvTableHandle.get());
        Context mvOptimizerContext = new Context(session, metadata, mvTableHandle.get(), tableHandle.get(), planVariableAllocator);

        if (!isMVCompatible(mvOptimizerContext, planNode)) {
            log.error("The provided plan node is not compatible with supported materialized views.");
            return planNode;
        }

        //TODO:
        //Get filter predicate, if ds filter is present, gets its min and max values.
        //Get the latest partition landed on the MV table.
        //Check if the MV has all the partitions landed?
        //Update the table for update MV
        //If yes, use the MV otherwise ignore it.
//        getFilterPredicates(planNode);

        //TODO: Scan last week query and build the dimension and metrics.
        //build the column mapping
        //Run the shadow in the T10 cluster.

        //TODO build object to get the table name.

        log.info("Going to rewrite the plan");
        return SimplePlanRewriter.rewriteWith(new MVOptimizer.Rewriter(session, metadata, idAllocator), planNode, mvOptimizerContext);
    }

    private boolean isMVCompatible(Context mvOptimizerContext, PlanNode planNode)
    {
        return isPlanCompatible(planNode) && isProjectionCompatible(planNode)
                && isFilterCompatible(planNode); // && isPartitionsLanded(mvOptimizerContext, planNode);
    }

    private boolean isFilterCompatible(PlanNode node)
    {
        FilterNode filterNode = getFilterNode(node);

        List<RowExpression> predicates = LogicalRowExpressions.extractPredicates(filterNode.getPredicate());
        for (RowExpression predicate : predicates) {
            if (predicate instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) predicate;
                List<RowExpression> arguments = callExpression.getArguments();
                for (RowExpression expression : arguments) {
                    if (expression instanceof VariableReferenceExpression) {
                        if (!dimensions.contains(((VariableReferenceExpression) expression).getName())) {
                            log.error(String.format("Failed to find the expression in the dimension![%s], Skipping the MV optimization", expression.toString()));
                            return false;
                        }
                    }
                }
            }
            else if (predicate instanceof SpecialFormExpression) {
                SpecialFormExpression specialFormExpression = (SpecialFormExpression) predicate;
                List<RowExpression> arguments = specialFormExpression.getArguments();
                for (RowExpression expression : arguments) {
                    if (expression instanceof VariableReferenceExpression) {
                        if (!dimensions.contains(((VariableReferenceExpression) expression).getName())) {
                            log.error(String.format("Failed to find the expression in the dimension![%s], Skipping the MV optimization", expression.toString()));
                            return false;
                        }
                    }
                }
            }
            else {
                log.error("Unsupported predicate expression [{}]", predicate);
                return false;
            }
        }

        return true;
    }

    private boolean isPartitionsLanded(Context context, PlanNode planNode)
    {
/*
        ColumnHandle dsColumnHandle = context.getMVColumnHandles().get("ds");
        ColumnMetadata columnMetadata = metadata.getColumnMetadata(context.getSession(), context.getMVTableHandle(), dsColumnHandle);
        List<ColumnHandle> columnHandles = new ArrayList<>();
        columnHandles.add(dsColumnHandle);
*/

        //TODO: get it from MVInfo
        Optional<List<String>> partitionNames = metadata.getPartitionNames(context.getSession(), "tpch", context.getMVTableHandle());

        String partitionName = "ds";

        FilterNode filterNode = getFilterNode(planNode);
        if (filterNode == null) {
            log.error("Filter node not found!! Skipping partition check");
            return true;
        }

        AtomicBoolean partitionLanded = new AtomicBoolean(false);

        List<RowExpression> predicates = LogicalRowExpressions.extractPredicates(filterNode.getPredicate());
        predicates.stream().filter(this::isPartitionPredicate).forEach(predicate -> {
            if (predicate instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) predicate;
                List<RowExpression> arguments = callExpression.getArguments();
                FunctionHandle functionHandle = callExpression.getFunctionHandle();
                List<TypeSignatureProvider> typeProviderList = fromTypes(callExpression.getArguments().stream().map(RowExpression::getType).collect(toImmutableList()));
                System.out.println(functionHandle);
                //one of these
            }
            else {
                partitionLanded.set(false);
            }
        });

        return partitionLanded.get();
    }

    private boolean isPartitionPredicate(RowExpression expression)
    {
        //TODO: get partition name from context. first partition from the list.
        String partitionName = "ds";
        //TODO: Add SpecialFormExpression ?
        if (expression instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) expression;
            List<RowExpression> arguments = callExpression.getArguments();
            for (RowExpression arg : arguments) {
                if (arg instanceof VariableReferenceExpression && arg.toString().equals(partitionName)) {
                    return true;
                }
            }
        }

        return false;
    }

    private FilterNode getFilterNode(PlanNode planNode)
    {
        if (planNode == null) {
            return null;
        }

        if (planNode instanceof OutputNode) {
            return getFilterNode(((OutputNode) planNode).getSources().get(0));
        }
        if (planNode instanceof AggregationNode) {
            return getFilterNode(((AggregationNode) planNode).getSources().get(0));
        }
        if (planNode instanceof ProjectNode) {
            return getFilterNode(((ProjectNode) planNode).getSources().get(0));
        }
        if (planNode instanceof FilterNode) {
            return (FilterNode) planNode;
        }

        return null;
    }

    private boolean isProjectionCompatible(PlanNode planNode)
    {
        if (planNode instanceof ProjectNode) {
            ProjectNode node = (ProjectNode) planNode;
            Map<VariableReferenceExpression, RowExpression> nodeAssignments = node.getAssignments().getMap();
            Set<VariableReferenceExpression> variableReferenceExpressions = nodeAssignments.keySet();
            for (VariableReferenceExpression variableReferenceExpression : variableReferenceExpressions) {
                RowExpression rowExpression = nodeAssignments.get(variableReferenceExpression);
                String key = rowExpression.toString();
                if (rowExpression instanceof CallExpression) {
                    if (!expressionToMVColumnName.containsKey(key)) {
                        log.info("Expression [%s] is not registered for MV optimization. ", key);
                        return false;
                    }
                }
                else if (!(rowExpression instanceof VariableReferenceExpression)) {
                    //TODO for variable reference check if the column is present in the MV
                    log.info("Unsupported [%s] expression for MV optimization. ", key);
                    return false;
                }
            }
        }
        else {
            return planNode.getSources().size() == 1 && isProjectionCompatible(planNode.getSources().get(0));
        }

        return true;
    }

    private boolean isPlanCompatible(PlanNode node)
    {
        if (!(node instanceof OutputNode)) {
            return false;
        }
        node = ((OutputNode) node).getSource();
        if (!(node instanceof AggregationNode)) {
            return false;
        }
        node = ((AggregationNode) node).getSource();
        if (!(node instanceof ProjectNode)) {
            return false;
        }
        node = ((ProjectNode) node).getSource();
        if (node instanceof TableScanNode) {
            return true;
        }
        if (!(node instanceof FilterNode)) {
            return false;
        }
        node = ((FilterNode) node).getSource();
        return node instanceof TableScanNode;
    }

    private static class Context
    {
        private final Session session;
        private final TableHandle tableHandle;
        private final TableHandle mvTableHandle;
        private final Map<String, ColumnHandle> tableColumnHandles;
        private final Map<String, ColumnHandle> mvColumnHandles;
        private final PlanVariableAllocator planVariableAllocator;
        private final Map<String, VariableReferenceExpression> mvColumnNameToVariable;
        private final Set<VariableReferenceExpression> mvCarryVariables;

        private Context(Session session, Metadata metadata, TableHandle mvTableHandle, TableHandle tableHandle, PlanVariableAllocator planVariableAllocator)
        {
            this.session = session;
            this.mvTableHandle = mvTableHandle;
            this.tableHandle = tableHandle;
            this.tableColumnHandles = metadata.getColumnHandles(session, tableHandle);
            this.mvColumnHandles = metadata.getColumnHandles(session, mvTableHandle);
            this.planVariableAllocator = planVariableAllocator;
            this.mvColumnNameToVariable = new HashMap<>();
            this.mvCarryVariables = new HashSet<>();
        }

        public Session getSession()
        {
            return session;
        }

        public TableHandle getTableHandle()
        {
            return tableHandle;
        }

        public TableHandle getMVTableHandle()
        {
            return mvTableHandle;
        }

        public PlanVariableAllocator getPlanVariableAllocator()
        {
            return planVariableAllocator;
        }

        public Map<String, ColumnHandle> getTableColumnHandles()
        {
            return tableColumnHandles;
        }

        public Map<String, ColumnHandle> getMVColumnHandles()
        {
            return mvColumnHandles;
        }

        public Map<String, VariableReferenceExpression> getMVColumnNameToVariable()
        {
            return mvColumnNameToVariable;
        }

        public Set<VariableReferenceExpression> getMVCarryVariables()
        {
            return mvCarryVariables;
        }
    }

    private boolean isMVEnabled(Session session)
    {
        return session.getSystemProperty(MV_OPTIMIZATION_ENABLED, Boolean.class);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Context>
    {
        private final Session session;
        private final Metadata metadata;
        private final StandardFunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;

        public Rewriter(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionResolution = new FunctionResolution(metadata.getFunctionManager());
            this.variableAllocator = new PlanVariableAllocator();
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            Assignments.Builder assignments = Assignments.builder();
            PlanVariableAllocator planVariableAllocator = context.get().getPlanVariableAllocator();
            Map<String, VariableReferenceExpression> mvColumnNameToVariable = context.get().getMVColumnNameToVariable();
            Set<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();

            try {
                node.getAssignments().forEach((variable, rowExpression) -> {
                    String key = rowExpression.toString();
                    if (rowExpression instanceof CallExpression) {
                        if (expressionToMVColumnName.containsKey(key)) {
                            String mvColumnName = expressionToMVColumnName.get(key);
                            if (!mvColumnNameToVariable.containsKey(mvColumnName)) {
                                VariableReferenceExpression newMVColumnVariable = planVariableAllocator.newVariable(mvColumnName, BIGINT);
                                mvColumnNameToVariable.put(mvColumnName, newMVColumnVariable);
                            }
                            assignments.put(variable, mvColumnNameToVariable.get(mvColumnName));
                        }
                        else {
                            //TODO: move this logic to validation.
                            log.info("Expression [{}] is not registered for MV optimization. ", key);
                            checkState(false);
                        }
                    }
                    else if (rowExpression instanceof VariableReferenceExpression) {
                        mvCarryVariables.add((VariableReferenceExpression) rowExpression);
                        assignments.put(variable, rowExpression);
                    }
                    else {
                        log.info("Unsupported [{}] expression for MV optimization. ", key);
                        checkState(false);
                    }
                });
            }
            catch (IllegalStateException ex) {
                return node;
            }

            ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), node.getSource(), assignments.build(), node.getLocality());
            PlanNode planNode = super.visitProject(projectNode, context);
            return planNode;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Context> context)
        {
            Set<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();
            List<RowExpression> predicates = LogicalRowExpressions.extractPredicates(node.getPredicate());
            predicates.forEach(predicate -> {
                if (predicate instanceof CallExpression) {
                    CallExpression callExpression = (CallExpression) predicate;
                    List<RowExpression> arguments = callExpression.getArguments();
                    arguments.stream().filter(expression -> expression instanceof VariableReferenceExpression).forEach(expression -> mvCarryVariables.add((VariableReferenceExpression) expression));
                }
                else if (predicate instanceof SpecialFormExpression) {
                    SpecialFormExpression specialFormExpression = (SpecialFormExpression) predicate;
                    List<RowExpression> arguments = specialFormExpression.getArguments();
                    arguments.stream().filter(expression -> expression instanceof VariableReferenceExpression).forEach(expression -> mvCarryVariables.add((VariableReferenceExpression) expression));
                }
                else {
                    log.error("Unsupported predicate expression [{}]", predicate);
                }
            });

            return super.visitFilter(node, context);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            TableHandle newMVTableHandle = context.get().getMVTableHandle();
            Map<String, ColumnHandle> mvColumnHandles = context.get().getMVColumnHandles();
            Map<String, ColumnHandle> tableColumnHandles = context.get().getTableColumnHandles();
            Set<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();

            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> mvAssignment = ImmutableMap.builder();
            List<VariableReferenceExpression> mvOutputVariables = new ArrayList<>();

            Map<String, VariableReferenceExpression> mvColumnNameToVariable = context.get().getMVColumnNameToVariable();

            mvCarryVariables.forEach(variable -> {
                String columnName = tableColumnHandles.get(variable.getName()).getName();
                ColumnHandle mvColumnHandle = mvColumnHandles.get(columnName);
                mvAssignment.put(variable, mvColumnHandle);
                mvOutputVariables.add(variable);
            });

            mvColumnNameToVariable.forEach((columnName, variable) -> {
                ColumnHandle mvColumnHandle = mvColumnHandles.get(columnName);
                mvAssignment.put(variable, mvColumnHandle);
                mvOutputVariables.add(variable);
            });

            //TODO: Use real constraints
            return new TableScanNode(
                    node.getId(),
                    newMVTableHandle,
                    mvOutputVariables,
                    mvAssignment.build(),
                    TupleDomain.all(),
                    TupleDomain.all());
        }
    }
}
