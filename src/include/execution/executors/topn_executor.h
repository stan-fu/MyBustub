//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <utility>
#include <vector>
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  struct Cmp {
    explicit Cmp(std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by,
                 std::unique_ptr<AbstractExecutor> &child_executor)
        : order_by_(std::move(order_by)) {
      if (child_executor != nullptr) {
        schema_ = std::make_shared<Schema>(child_executor->GetOutputSchema());
      }
    }
    auto operator()(const Tuple &a, const Tuple &b) -> bool {
      for (auto [ob_type, expr] : order_by_) {
        auto value_a = expr->Evaluate(&a, *schema_);
        auto value_b = expr->Evaluate(&b, *schema_);
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

    void swap(Cmp &that) {  // NOLINT
      order_by_.swap(that.order_by_);
      schema_.swap(that.schema_);
    }

    void SetSchema(const Schema &schema) {
      if (schema_ == nullptr) {
        schema_ = std::make_shared<Schema>(schema);
      }
    }

    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by_;
    std::shared_ptr<Schema> schema_;
  };

  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Accquire top n tuples through heap sort */
  size_t index_;
  Cmp cmp_;
  std::vector<Tuple> container_;
};
}  // namespace bustub
