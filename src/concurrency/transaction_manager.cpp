//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  // fmt::print("Commit(txn = {})\n", txn->GetTransactionId());
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  // fmt::print("Abort(txn = {})\n", txn->GetTransactionId());
  ReleaseLocks(txn);
  auto table_write_set = txn->GetWriteSet();
  auto index_write_set = txn->GetIndexWriteSet();
  while (!table_write_set->empty()) {
    auto record = table_write_set->back();
    auto table = record.table_heap_;
    table_write_set->pop_back();
    if (record.wtype_ == WType::DELETE) {
      // revert delete
      auto meta = table->GetTupleMeta(record.rid_);
      meta.is_deleted_ = false;
      table->UpdateTupleMeta(meta, record.rid_);
    } else if (record.wtype_ == WType::INSERT) {
      // revert insert
      auto meta = table->GetTupleMeta(record.rid_);
      meta.is_deleted_ = true;
      meta.delete_txn_id_ = txn->GetTransactionId();
      table->UpdateTupleMeta(meta, record.rid_);
    } else {
      throw ExecutionException("update not implement");
    }
  }
  while (!index_write_set->empty()) {
    auto record = index_write_set->back();
    auto table_info = record.catalog_->GetTable(record.table_oid_);
    auto index_info = record.catalog_->GetIndex(record.index_oid_);
    auto tuple = record.old_tuple_;
    auto rid = record.rid_;
    auto schema = table_info->schema_;
    auto key_schema = index_info->key_schema_;
    auto key_attrs = index_info->index_->GetKeyAttrs();
    index_write_set->pop_back();

    if (record.wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(tuple.KeyFromTuple(schema, key_schema, key_attrs), rid, txn);
    } else if (record.wtype_ == WType::INSERT) {
      index_info->index_->DeleteEntry(tuple.KeyFromTuple(schema, key_schema, key_attrs), rid, txn);
    } else {
      throw ExecutionException("update not implement");
    }
  }

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
