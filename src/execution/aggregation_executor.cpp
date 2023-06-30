//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  aht_.Clear();

  Tuple child_tuple{};
  RID child_rid{};
  AggregateKey aggr_key;
  AggregateValue aggr_val;
  while (child_->Next(&child_tuple, &child_rid)) {
    aggr_key = MakeAggregateKey(&child_tuple);
    aggr_val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(aggr_key, aggr_val);
  }
  if (aht_.Empty() && plan_->GetGroupBys().empty()) {
    aht_.InsertCombine(aggr_key, aggr_val);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  for (const auto &value : aht_iterator_.Key().group_bys_) {
    values.push_back(value);
  }
  for (const auto &value : aht_iterator_.Val().aggregates_) {
    values.push_back(value);
  }
  *tuple = Tuple(values, &GetOutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
