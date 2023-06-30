//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  finished_ = false;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  finished_ = true;

  RID child_rid{};
  Tuple delete_tuple{};
  int32_t delete_cnt = 0;
  std::vector<Value> values;

  // delete tuple
  while (child_executor_->Next(&delete_tuple, &child_rid)) {
    auto meta = table_info_->table_->GetTupleMeta(child_rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, child_rid);
    ++delete_cnt;
    // delete index
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      auto schema = table_info_->schema_;
      auto key_schema = index_info->key_schema_;
      auto key_attrs = index_info->index_->GetKeyAttrs();
      index_info->index_->DeleteEntry(delete_tuple.KeyFromTuple(schema, key_schema, key_attrs), delete_tuple.GetRid(),
                                      exec_ctx_->GetTransaction());
    }
  }
  values.emplace_back(TypeId::INTEGER, delete_cnt);
  *tuple = Tuple(values, &GetOutputSchema());

  return true;
}

}  // namespace bustub
