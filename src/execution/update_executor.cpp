//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  finished_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  finished_ = true;

  RID child_rid{};
  Tuple delete_tuple{};
  int32_t update_cnt = 0;
  std::vector<Value> values;

  // update tuple
  while (child_executor_->Next(&delete_tuple, &child_rid)) {
    // mark tuple as delete
    auto meta = table_info_->table_->GetTupleMeta(child_rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, child_rid);
    // Compute expressions
    std::vector<Value> child_values;
    child_values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      child_values.push_back(expr->Evaluate(&delete_tuple, child_executor_->GetOutputSchema()));
    }
    // insert tuple
    meta = {0, 0, false};
    auto insert_tuple = Tuple(child_values, &(child_executor_->GetOutputSchema()));
    auto insert_rid = table_info_->table_->InsertTuple(meta, insert_tuple);

    // update index
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      auto schema = table_info_->schema_;
      auto key_schema = index_info->key_schema_;
      auto key_attrs = index_info->index_->GetKeyAttrs();
      index_info->index_->DeleteEntry(delete_tuple.KeyFromTuple(schema, key_schema, key_attrs), delete_tuple.GetRid(),
                                      exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(insert_tuple.KeyFromTuple(schema, key_schema, key_attrs), *insert_rid,
                                      exec_ctx_->GetTransaction());
    }
    ++update_cnt;
  }

  values.emplace_back(TypeId::INTEGER, update_cnt);
  *tuple = Tuple(values, &GetOutputSchema());

  return true;
}

}  // namespace bustub
