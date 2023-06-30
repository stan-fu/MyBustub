//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "include/type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  join_results_.clear();

  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  Tuple left_tuple;
  Tuple right_tuple;
  RID rid;
  bool match;
  while (left_executor_->Next(&left_tuple, &rid)) {
    match = false;
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &rid)) {
      if (plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        for (size_t k = 0; k < left_schema.GetColumnCount(); k++) {
          values.push_back(left_tuple.GetValue(&left_schema, k));
        }
        for (size_t k = 0; k < right_schema.GetColumnCount(); k++) {
          values.push_back(right_tuple.GetValue(&right_schema, k));
        }
        join_results_.emplace_back(values, &GetOutputSchema());
        values.clear();
        match = true;
      }
    }

    if (!match && plan_->GetJoinType() == JoinType::LEFT) {
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

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == join_results_.end()) {
    return false;
  }
  *tuple = *iter_++;
  return true;
}

}  // namespace bustub
