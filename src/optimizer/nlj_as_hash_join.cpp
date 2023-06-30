#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // recursively optimize childrens' plans
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  // optimize current nested loop join plan
  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  // Has exactly two child
  BUSTUB_ENSURE(optimized_plan->children_.size() == 2, "Join must have two children");
  //      cmp1          cmp2          cmp3    ...
  // t1.a     t2.a  t1.b    t2.b  t3.a    t3.b
  std::vector<AbstractExpressionRef> left_exprs;
  std::vector<AbstractExpressionRef> right_exprs;
  auto key_expressions = nlj_plan.Predicate()->GetChildren();

  if (key_expressions.empty()) {
    return optimized_plan;
  }

  if (key_expressions[0]->children_.empty()) {  // t1.a = t2.a
    for (auto const &expr : key_expressions) {
      if (dynamic_cast<ColumnValueExpression *>(expr.get())->GetTupleIdx() == 0) {
        left_exprs.emplace_back(expr);
      } else {
        right_exprs.emplace_back(expr);
      }
    }
  } else {  // t1.a = t2.a and t1.b = t2.b and ...
    for (auto const &exprs : key_expressions) {
      for (auto const &expr : exprs->GetChildren()) {
        if (dynamic_cast<ColumnValueExpression *>(expr.get())->GetTupleIdx() == 0) {
          left_exprs.emplace_back(expr);
        } else {
          right_exprs.emplace_back(expr);
        }
      }
    }
  }
  return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, optimized_plan->GetChildAt(0),
                                            optimized_plan->GetChildAt(1), left_exprs, right_exprs,
                                            nlj_plan.GetJoinType());
}
}  // namespace bustub
