//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  struct Cmp {
    explicit Cmp(std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by,
                 std::unique_ptr<AbstractExecutor> &child_executor)
        : order_by_(std::move(order_by)), schema_(child_executor->GetOutputSchema()) {}

    auto operator()(Tuple &a, Tuple &b) -> bool {
      for (auto [ob_type, expr] : order_by_) {
        auto value_a = expr->Evaluate(&a, schema_);
        auto value_b = expr->Evaluate(&b, schema_);
        if (value_a.CompareEquals(value_b) == CmpBool::CmpTrue) {
          continue;
        }
        switch (ob_type) {
          case OrderByType::ASC:
          case OrderByType::DEFAULT:
            return value_a.CompareLessThan(value_b) == CmpBool::CmpTrue;
          case OrderByType::DESC:
            return value_b.CompareLessThan(value_a) == CmpBool::CmpTrue;
          default:
            BUSTUB_ENSURE(false, "invalid order by");
        }
      }
      return true;
    }

    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by_;
    Schema schema_;
  };

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> results_;
  std::vector<Tuple>::const_iterator iter_;
  Cmp cmp_;
};
}  // namespace bustub
