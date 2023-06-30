//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  finished_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  finished_ = true;

  Tuple insert_tuple{};
  TupleMeta meta{0, 0, false};
  int32_t insert_cnt = 0;
  std::vector<Value> values;

  // insert tuple
  while (child_executor_->Next(&insert_tuple, rid)) {
    auto insert_rid = table_info_->table_->InsertTuple(meta, insert_tuple);
    insert_cnt++;
    // insert index
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      auto schema = table_info_->schema_;
      auto key_schema = index_info->key_schema_;
      auto key_attrs = index_info->index_->GetKeyAttrs();
      index_info->index_->InsertEntry(insert_tuple.KeyFromTuple(schema, key_schema, key_attrs), *insert_rid,
                                      exec_ctx_->GetTransaction());
    }
  }
  values.emplace_back(TypeId::INTEGER, insert_cnt);
  *tuple = Tuple(values, &GetOutputSchema());

  return true;
}

}  // namespace bustub
