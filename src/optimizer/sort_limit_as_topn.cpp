#include "include/execution/plans/limit_plan.h"
#include "include/execution/plans/sort_plan.h"
#include "include/execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // recursively optimize childrens' plans
  if (plan->GetChildren().empty()) {
    return plan;
  }
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  auto child_plan = optimized_plan->GetChildAt(0);
  if (optimized_plan->GetType() != PlanType::Limit || child_plan->GetType() != PlanType::Sort) {
    return optimized_plan;
  }

  // merge limit and sort plan node as topn plan node
  const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
  const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
  BUSTUB_ENSURE(limit_plan.children_.size() == 1 && sort_plan.children_.size() == 1,
                "limit and sort must have only one child");
  auto output = std::make_shared<Schema>(optimized_plan->OutputSchema());
  auto child = sort_plan.GetChildPlan();
  auto &order_bys = sort_plan.GetOrderBy();
  auto n = limit_plan.GetLimit();

  return std::make_shared<TopNPlanNode>(output, child, order_bys, n);
}

}  // namespace bustub
