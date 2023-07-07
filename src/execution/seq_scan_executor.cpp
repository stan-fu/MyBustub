//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // accquire transaction
  txn_ = exec_ctx_->GetTransaction();
  lock_mgr_ = exec_ctx_->GetLockManager();
  if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    bool success;
    if (exec_ctx_->IsDelete()) {
      success = lock_mgr_->LockTable(txn_, LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid());
    } else {
      success = lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
    }
    if (!success) {
      throw ExecutionException("LockTable failed");
    }
  }
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_->IsEnd()) {
    bool success = true;
    if (exec_ctx_->IsDelete()) {
      success = lock_mgr_->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), table_iter_->GetRID());
    } else {
      if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        success = lock_mgr_->LockRow(txn_, LockManager::LockMode::SHARED, plan_->GetTableOid(), table_iter_->GetRID());
      }
    }
    if (!success) {
      throw ExecutionException("LockTuple failed");
    }
    auto [meta, tup] = table_iter_->GetTuple();
    ++(*table_iter_);
    // check tuple meta
    if (!meta.is_deleted_) {
      *tuple = tup;
      *rid = tup.GetRid();
      return true;
    }
    // TODO(optimize): check filter_predicate_ if implemented filter pushdown
    // force unlock tuple if it should not read by this transaction
    if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      lock_mgr_->UnlockRow(txn_, plan_->GetTableOid(), tup.GetRid(), true);
    }
  }
  return false;
}

}  // namespace bustub
