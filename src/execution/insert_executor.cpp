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
  txn_ = exec_ctx_->GetTransaction();
  lock_mgr_ = exec_ctx_->GetLockManager();
  bool success;
  try {
    success = lock_mgr_->LockTable(txn_, LockManager::LockMode::EXCLUSIVE, plan_->TableOid());
  } catch (TransactionAbortException &e) {
    fmt::print("{}", e.GetInfo());
    throw e;
  }
  if (!success) {
    throw ExecutionException("LockTable Failed");
  }
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  finished_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  finished_ = true;

  Tuple insert_tuple{};
  TupleMeta meta{txn_->GetTransactionId(), 0, false};
  int32_t insert_cnt = 0;
  std::vector<Value> values;

  while (child_executor_->Next(&insert_tuple, rid)) {
    // insert tuple
    auto insert_rid = table_info_->table_->InsertTuple(meta, insert_tuple, lock_mgr_, txn_, plan_->TableOid());
    insert_cnt++;
    // insert table write record
    TableWriteRecord table_write_record = {plan_->TableOid(), *insert_rid, table_info_->table_.get()};
    table_write_record.wtype_ = WType::INSERT;
    txn_->AppendTableWriteRecord(table_write_record);
    // insert index from tuple into all indexes
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      auto schema = table_info_->schema_;
      auto key_schema = index_info->key_schema_;
      auto key_attrs = index_info->index_->GetKeyAttrs();
      index_info->index_->InsertEntry(insert_tuple.KeyFromTuple(schema, key_schema, key_attrs), *insert_rid,
                                      exec_ctx_->GetTransaction());
      // insert index write record
      IndexWriteRecord index_write_record = {*insert_rid,  plan_->TableOid(),      WType::INSERT,
                                             insert_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()};
      txn_->AppendIndexWriteRecord(index_write_record);
    }
  }
  values.emplace_back(TypeId::INTEGER, insert_cnt);
  *tuple = Tuple(values, &GetOutputSchema());

  return true;
}

}  // namespace bustub
