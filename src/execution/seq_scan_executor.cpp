
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

  if (exec_ctx_->IsDelete()) {
    lock_mgr_->LockTable(txn_, LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid());
  } else if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn_->IsTableExclusiveLocked(plan_->GetTableOid()) &&
        !txn_->IsTableIntentionExclusiveLocked(plan_->GetTableOid())) {
      lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
    }
  }
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_->IsEnd()) {
    auto tupleMeta = table_iter_->GetTuple().first;
    if (tupleMeta.is_deleted_) {
      ++(*table_iter_);
      continue;
    }
    if (exec_ctx_->IsDelete()) {
      lock_mgr_->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), table_iter_->GetRID());
    } else if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn_->IsRowExclusiveLocked(plan_->GetTableOid(), table_iter_->GetRID())) {
        lock_mgr_->LockRow(txn_, LockManager::LockMode::SHARED, plan_->GetTableOid(), table_iter_->GetRID());
      }
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
    // force unlock tuple if it should not read by this transaction:delete or update
    if (exec_ctx_->IsDelete() || txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      lock_mgr_->UnlockRow(txn_, plan_->GetTableOid(), tup.GetRid(), true);
    }
  }
  return false;
}

}  // namespace bustub