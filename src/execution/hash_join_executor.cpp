//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "include/type/value_factory.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  hash_join_table_.clear();

  Tuple left_tuple;
  Tuple right_tuple;
  RID rid;
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  auto left_schema = left_child_->GetOutputSchema();
  auto right_schema = right_child_->GetOutputSchema();

  // build hash table through right table
  while (right_child_->Next(&right_tuple, &rid)) {
    auto right_key = MakeRightJoinKey(&right_tuple);
    hash_join_table_.emplace(right_key, right_tuple);
  }

  // scan hash table through left table
  while (left_child_->Next(&left_tuple, &rid)) {
    auto left_key = MakeLeftJoinKey(&left_tuple);
    if (hash_join_table_.count(left_key) > 0) {
      auto range = hash_join_table_.equal_range(left_key);
      for (auto it = range.first; it != range.second; it++) {
        const auto &right_tuple = it->second;
        for (size_t k = 0; k < left_schema.GetColumnCount(); k++) {
          values.push_back(left_tuple.GetValue(&left_schema, k));
        }
        for (size_t k = 0; k < right_schema.GetColumnCount(); k++) {
          values.push_back(right_tuple.GetValue(&right_schema, k));
        }
        join_results_.emplace_back(values, &GetOutputSchema());
        values.clear();
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      for (size_t k = 0; k < left_schema.GetColumnCount(); k++) {
        values.push_back(left_tuple.GetValue(&left_schema, k));
      }
      for (const auto &column : right_schema.GetColumns()) {
        values.push_back(ValueFactory::GetNullValueByType(column.GetType()));
      }
      join_results_.emplace_back(values, &GetOutputSchema());
      values.clear();
    }
  }
  iter_ = join_results_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == join_results_.end()) {
    return false;
  }
  *tuple = *iter_++;
  return true;
}

}  // namespace bustub
