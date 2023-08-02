#include <algorithm>
#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeJoinOrder(const AbstractPlanNodeRef &plan) -> std::optional<size_t> {
  auto plan_type = plan->GetType();
  if (plan_type == PlanType::HashJoin || plan_type == PlanType::NestedLoopJoin) {
    auto left_cardinality = OptimizeJoinOrder(plan->children_[0]);
    auto right_cardinality = OptimizeJoinOrder(plan->children_[1]);
    if (left_cardinality && right_cardinality) {
      if (*left_cardinality > *right_cardinality) {
        // swap children to make left child smaller
        auto optimized_plan = plan->CloneWithChildren(plan->children_);
        std::swap(optimized_plan->children_[0], optimized_plan->children_[1]);
      }
      return std::make_optional<>(*left_cardinality + *right_cardinality);
    }
  }
  if (plan_type == PlanType::SeqScan) {
    auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode *>(plan.get());
    return EstimatedCardinality(seq_scan_plan->table_name_);
  }
  if (plan_type == PlanType::MockScan) {
    auto mock_scan_plan = dynamic_cast<const MockScanPlanNode *>(plan.get());
    return EstimatedCardinality(mock_scan_plan->GetTable());
  }
  if (plan_type == PlanType::Aggregation || plan_type == PlanType::Projection || plan_type == PlanType::Sort ||
      plan_type == PlanType::Filter) {
    return OptimizeJoinOrder(plan->GetChildAt(0));
  }
  if (plan_type == PlanType::TopN) {
    auto topn_plan = dynamic_cast<const TopNPlanNode *>(plan.get());
    return std::make_optional<>(topn_plan->GetN());
  }
  if (plan_type == PlanType::Limit) {
    auto limit_plan = dynamic_cast<const LimitPlanNode *>(plan.get());
    return std::make_optional<>(limit_plan->GetLimit());
  }
  if (plan_type == PlanType::Values) {
    auto values_plan = dynamic_cast<const ValuesPlanNode *>(plan.get());
    return std::make_optional<>(values_plan->GetValues().size());
  }

  return {};
}

auto Optimizer::RewriteExpressionForPushDown(const std::vector<AbstractExpressionRef> &exprs) -> AbstractExpressionRef {
  if (exprs.size() == 1) {
    return exprs[0];
  }
  // init
  auto logic_expr = std::make_unique<LogicExpression>(exprs[0], exprs[1], LogicType::And);
  for (size_t i = 2; i < exprs.size(); i++) {
    logic_expr = std::make_unique<LogicExpression>(std::move(logic_expr), exprs[i], LogicType::And);
  }
  return logic_expr;
}

auto Optimizer::OptimizePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto optimized_plan = plan->CloneWithChildren(plan->GetChildren());
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    auto nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);

    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    std::vector<AbstractExpressionRef> cur_exprs;

    std::queue<AbstractExpressionRef> expr_queue;
    expr_queue.push(nlj_plan.Predicate());
    while (!expr_queue.empty()) {
      auto expr = expr_queue.front();
      expr_queue.pop();
      if (auto cmp_expr = dynamic_cast<ComparisonExpression *>(expr.get()); cmp_expr != nullptr) {
        auto left_col = dynamic_cast<ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
        auto right_col = dynamic_cast<ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());
        if (left_col != nullptr && right_col != nullptr) {
          cur_exprs.push_back(expr);
        } else if (left_col != nullptr) {
          auto left_tuple_idx = left_col->GetTupleIdx();
          if (left_tuple_idx == 0) {
            left_exprs.push_back(expr);
          } else {
            right_exprs.push_back(expr);
          }
        } else {
          auto right_tuple_idx = right_col->GetTupleIdx();
          if (right_tuple_idx == 0) {
            left_exprs.push_back(expr);
          } else {
            right_exprs.push_back(expr);
          }
        }
      }
      for (auto const &child : expr->GetChildren()) {
        expr_queue.push(child);
      }
    }
    // push down filter
    auto tmp = plan->CloneWithChildren(plan->GetChildren());
    if (!cur_exprs.empty()) {
      nlj_plan.predicate_ = RewriteExpressionForPushDown(cur_exprs);
    }
    auto left_plan = nlj_plan.GetLeftPlan();
    auto right_plan = nlj_plan.GetRightPlan();
    if (!left_exprs.empty()) {
      if (left_plan->GetType() == PlanType::SeqScan || left_plan->GetType() == PlanType::MockScan) {
        nlj_plan.children_[0] = std::make_shared<FilterPlanNode>(left_plan->output_schema_,
                                                                 RewriteExpressionForPushDown(left_exprs), left_plan);
      } else if (left_plan->GetType() == PlanType::NestedLoopJoin) {
        auto tmp = left_plan->CloneWithChildren(left_plan->GetChildren());
        auto tmp_nlj = dynamic_cast<NestedLoopJoinPlanNode *>(tmp.get());
        left_exprs.push_back(tmp_nlj->Predicate());
        nlj_plan.children_[0] = std::make_shared<NestedLoopJoinPlanNode>(
            left_plan->output_schema_, left_plan->children_[0], left_plan->children_[1],
            RewriteExpressionForPushDown(left_exprs), tmp_nlj->GetJoinType());
      }
    }
    if (!right_exprs.empty()) {
      if (right_plan->GetType() == PlanType::SeqScan || right_plan->GetType() == PlanType::MockScan) {
        nlj_plan.children_[1] = std::make_shared<FilterPlanNode>(right_plan->output_schema_,
                                                                 RewriteExpressionForPushDown(right_exprs), right_plan);
      } else if (right_plan->GetType() == PlanType::NestedLoopJoin) {
        auto tmp = right_plan->CloneWithChildren(right_plan->GetChildren());
        auto tmp_nlj = dynamic_cast<NestedLoopJoinPlanNode *>(tmp.get());
        right_exprs.push_back(tmp_nlj->Predicate());
        nlj_plan.children_[1] = std::make_shared<NestedLoopJoinPlanNode>(
            right_plan->output_schema_, right_plan->children_[0], right_plan->children_[1],
            RewriteExpressionForPushDown(right_exprs), tmp_nlj->GetJoinType());
      }
    }
    optimized_plan = nlj_plan.CloneWithChildren(nlj_plan.GetChildren());
  }

  // recursively optimize childrens' plans
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizePushDown(child));
  }
  return optimized_plan->CloneWithChildren(std::move(children));
}

}  // namespace bustub
