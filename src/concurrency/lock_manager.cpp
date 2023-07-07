//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  fmt::print("LockTable(txn id:{} lock mode:{} oid:{}), state = {}\n", txn->GetTransactionId(), lock_mode, oid,
             txn->GetState());
  // 1. Check transaction state and isolation level
  CanTxnTakeLock(txn, lock_mode);

  // 2. find lock request queue of table oid
  std::shared_ptr<LockRequestQueue> lock_request_queue = nullptr;
  {
    std::lock_guard<std::mutex> table_latch(table_lock_map_latch_);
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    }
    lock_request_queue = table_lock_map_[oid];
  }

  // 3. Check whether is an upgrade lock request, remove old request if true
  auto txn_id = txn->GetTransactionId();
  auto hold_lock = GetTableLockMode(txn, oid);
  if (hold_lock == lock_mode) {  // already hold the lock
    return false;
  }
  if (hold_lock != std::nullopt) {                 // upgrade lock
    if (!CanLockUpgrade(*hold_lock, lock_mode)) {  // check upgrade compatibility
      auto e = TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      txn->SetState(TransactionState::ABORTED);
      fmt::print("{}\n", e.GetInfo());
      throw e;
    }
  }
  {
    auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
    std::unique_lock<std::mutex> queue_latch(lock_request_queue->latch_);
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {  // upgrade conflict
      auto e = TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      txn->SetState(TransactionState::ABORTED);
      fmt::print("{}\n", e.GetInfo());
      throw e;
    }
    auto it = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                           [txn_id](const std::shared_ptr<LockRequest> &req) { return req->txn_id_ == txn_id; });

    // if could upgrade
    if (it != lock_request_queue->request_queue_.end()) {
      lock_request_queue->upgrading_ = txn_id;
      auto lock_set = GetTableLockSet(txn, (*it)->lock_mode_);
      lock_set->erase((*it)->oid_);
      lock_request_queue->request_queue_.erase(it);
      auto iter = lock_request_queue->request_queue_.begin();
      for (; iter != lock_request_queue->request_queue_.end(); ++iter) {
        if (!(*iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(iter, lock_request);
    } else {
      lock_request_queue->request_queue_.emplace_back(lock_request);
    }
    // 5. Accquire lock
    // wait for lock request to be granted
    lock_request_queue->cv_.wait(queue_latch, [&]() {
      fmt::print("Wake up -- LockTable(txn id:{} lock mode:{} oid:{}), state = {}\n", txn->GetTransactionId(),
                 lock_mode, oid, txn->GetState());
      if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
        return true;
      }
      GrantNewLocksIfPossible(lock_request_queue);
      return lock_request->granted_;
    });
    if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
      if (lock_request->txn_id_ == lock_request_queue->upgrading_) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  // 6. Update transaction lock set
  auto lock_set = GetTableLockSet(txn, lock_mode);
  lock_set->insert(oid);
  return true;
}

//---------------------------------------------------------------------------------------------------------

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  fmt::print("UnlockTable(txn id = {} oid = {}),iso = {}, state = {}\n", txn->GetTransactionId(), oid,
             txn->GetIsolationLevel(), txn->GetState());
  auto txn_id = txn->GetTransactionId();
  // check all row lock in this table are unlocked
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (!(*s_row_lock_set)[oid].empty() || !(*x_row_lock_set)[oid].empty()) {
    auto e = TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    txn->SetState(TransactionState::ABORTED);
    fmt::print("{}\n", e.GetInfo());
    throw e;
  }
  // check table lock exist
  LockMode unlock_mode;
  if (txn->IsTableExclusiveLocked(oid)) {
    unlock_mode = LockMode::EXCLUSIVE;
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    unlock_mode = LockMode::INTENTION_EXCLUSIVE;
  } else if (txn->IsTableIntentionSharedLocked(oid)) {
    unlock_mode = LockMode::INTENTION_SHARED;
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    unlock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
  } else if (txn->IsTableSharedLocked(oid)) {
    unlock_mode = LockMode::SHARED;
  } else {
    auto e = TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    txn->SetState(TransactionState::ABORTED);
    fmt::print("{}\n", e.GetInfo());
    throw e;
  }

  // modify transaction state according to isolation level
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED: {
      BUSTUB_ASSERT(unlock_mode != LockMode::SHARED && unlock_mode != LockMode::INTENTION_SHARED, "");
      if (unlock_mode == LockMode::EXCLUSIVE) {
        fmt::print("SetState to SHRINKING\n");
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    }
    case IsolationLevel::READ_COMMITTED: {
      if (unlock_mode == LockMode::EXCLUSIVE) {
        fmt::print("SetState to SHRINKING\n");
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    }
    case IsolationLevel::REPEATABLE_READ: {
      if (unlock_mode == LockMode::EXCLUSIVE || unlock_mode == LockMode::SHARED) {
        fmt::print("SetState to SHRINKING\n");
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }

  // find target lock
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_.at(oid);
  {
    std::lock_guard<std::mutex> queue_latch(lock_request_queue->latch_);
    lock_request_queue->request_queue_.remove_if(
        [txn_id](const std::shared_ptr<LockRequest> &req) { return req->txn_id_ == txn_id; });
    lock_request_queue->cv_.notify_all();
  }
  // update txn
  auto lock_set = GetTableLockSet(txn, unlock_mode);
  lock_set->erase(oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  fmt::print("LockRow(txn id:{} lock mode:{} oid:{} {}), state = {}, iso = {}\n", txn->GetTransactionId(), lock_mode,
             oid, rid.ToString(), txn->GetState(), txn->GetIsolationLevel());
  // already hold lock and can't upgrade
  if ((txn->IsRowSharedLocked(oid, rid) && lock_mode == LockMode::SHARED) || txn->IsRowExclusiveLocked(oid, rid)) {
    return false;
  }

  try {
    CheckAppropriateLockOnTable(txn, oid, lock_mode);
    CanTxnTakeLock(txn, lock_mode);
  } catch (TransactionAbortException &e) {
    fmt::print("{}", e.GetInfo());
    throw e;
  }
  {
    // accquire row request queue
    std::shared_ptr<LockRequestQueue> lock_request_queue = nullptr;
    auto txn_id = txn->GetTransactionId();
    auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
    row_lock_map_latch_.lock();
    if (row_lock_map_.find(rid) == row_lock_map_.end()) {
      row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    }
    lock_request_queue = row_lock_map_[rid];

    // cv need unique lock
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    row_lock_map_latch_.unlock();
    auto it = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                           [txn_id](const std::shared_ptr<LockRequest> &req) { return req->txn_id_ == txn_id; });
    // if upgrade, erase old lock
    if (it != lock_request_queue->request_queue_.end()) {
      BUSTUB_ASSERT((*it)->granted_ == true, "WRONG LOCK REQ");
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        auto e = TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
        txn->SetState(TransactionState::ABORTED);
        fmt::print("{}\n", e.GetInfo());
        throw e;
      }
      lock_request_queue->upgrading_ = txn_id;
      auto lock_set = GetRowLockSet(txn, (*it)->lock_mode_);
      lock_set->at(oid).erase(rid);
      lock_request_queue->request_queue_.erase(it);
      // upgrading txn has highest priority, insert into head
      auto iter = lock_request_queue->request_queue_.begin();
      for (; iter != lock_request_queue->request_queue_.end(); ++iter) {
        if (!(*iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(iter, lock_request);
    } else {
      lock_request_queue->request_queue_.emplace_back(lock_request);
    }
    // wait for lock request to be granted
    lock_request_queue->cv_.wait(lock, [&]() {
      fmt::print("Wake up -- LockRow(txn id:{} lock mode:{} oid:{} {}), state = {}, iso = {}\n",
                 txn->GetTransactionId(), lock_mode, oid, rid.ToString(), txn->GetState(), txn->GetIsolationLevel());
      if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
        return true;
      }
      GrantNewLocksIfPossible(lock_request_queue);
      return lock_request->granted_;
    });
    if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
      if (lock_request->txn_id_ == lock_request_queue->upgrading_) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  // 6. Update transaction lock set
  auto lock_set = GetRowLockSet(txn, lock_mode);
  (*lock_set)[oid].insert(rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  fmt::print("UnlockRow(txn id:{} oid:{} {}), state = {}\n", txn->GetTransactionId(), oid, rid.ToString(),
             txn->GetState());
  auto txn_id = txn->GetTransactionId();
  LockMode unlock_mode;
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    unlock_mode = LockMode::EXCLUSIVE;
  } else if (txn->IsRowSharedLocked(oid, rid)) {
    unlock_mode = LockMode::SHARED;
  } else {
    auto e = TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    txn->SetState(TransactionState::ABORTED);
    fmt::print("{}\n", e.GetInfo());
    throw e;
  }
  // update transaction state
  if (!force) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED: {
        BUSTUB_ASSERT(unlock_mode != LockMode::SHARED, "");
        if (unlock_mode == LockMode::EXCLUSIVE) {
          fmt::print("SetState to SHRINKING\n");
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      }
      case IsolationLevel::READ_COMMITTED: {
        if (unlock_mode == LockMode::EXCLUSIVE) {
          fmt::print("SetState to SHRINKING\n");
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      }
      case IsolationLevel::REPEATABLE_READ: {
        fmt::print("SetState to SHRINKING\n");
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }

  // remove target lock request
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_.at(rid);
  {
    std::lock_guard<std::mutex> queue_latch(lock_request_queue->latch_);
    lock_request_queue->request_queue_.remove_if(
        [txn_id](const std::shared_ptr<LockRequest> &req) { return req->txn_id_ == txn_id; });
    lock_request_queue->cv_.notify_all();
  }

  auto lock_set = GetRowLockSet(txn, unlock_mode);
  lock_set->at(oid).erase(rid);
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

auto LockManager::GetTableLockMode(Transaction *txn, const table_oid_t &oid) -> std::optional<LockMode> {
  if (txn->IsTableExclusiveLocked(oid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return LockMode::INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return LockMode::INTENTION_SHARED;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableSharedLocked(oid)) {
    return LockMode::SHARED;
  }
  return {};
}

auto LockManager::GetRowLockMode(Transaction *txn, const table_oid_t &oid, const RID &rid) -> std::optional<LockMode> {
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsRowSharedLocked(oid, rid)) {
    return LockMode::SHARED;
  }
  return {};
}

/**
 * @brief determine whether the two locks are compatible, refer to compatibility matrix
 *
 */
auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED: {
      return l2 != LockMode::EXCLUSIVE;
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
    }
    case LockMode::SHARED: {
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      return l2 == LockMode::INTENTION_SHARED;
    }
    default: {
      return false;
    }
  }
}

/**
 * @brief determine whether this lock request is legal
 *
 */
void LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) {
  auto state = txn->GetState();
  auto txn_id = txn->GetTransactionId();
  BUSTUB_ENSURE(state != TransactionState::ABORTED && state != TransactionState::COMMITTED, "WRONG TRANS STATE");

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED: {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        auto e = TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        txn->SetState(TransactionState::ABORTED);
        fmt::print("{}\n", e.GetInfo());
        throw e;
      }
      if (state == TransactionState::SHRINKING) {
        auto e = TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        txn->SetState(TransactionState::ABORTED);
        fmt::print("{}\n", e.GetInfo());
        throw e;
      }
      break;
    }
    case IsolationLevel::READ_COMMITTED: {
      if (state == TransactionState::SHRINKING &&
          (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED)) {
        auto e = TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        txn->SetState(TransactionState::ABORTED);
        fmt::print("{}\n", e.GetInfo());
        throw e;
      }
      break;
    }
    case IsolationLevel::REPEATABLE_READ: {
      if (state == TransactionState::SHRINKING) {
        auto e = TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        txn->SetState(TransactionState::ABORTED);
        fmt::print("{}\n", e.GetInfo());
        throw e;
      }
      break;
    }
  }
}

/**
 * @brief grant new locks if compatible with granted locks order by FIFO, refer to LOCK_NOTE
 *
 */
void LockManager::GrantNewLocksIfPossible(const std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  std::unordered_set<LockMode> granted_lockmode;
  for (const auto &lock_req : lock_request_queue->request_queue_) {
    if (lock_req->granted_) {
      granted_lockmode.insert(lock_req->lock_mode_);
    }
  }

  // Multiple compatible locks are granted consecutively
  for (const auto &lock_req : lock_request_queue->request_queue_) {
    if (lock_req->granted_) {
      continue;
    }
    for (auto l2 : granted_lockmode) {
      if (!AreLocksCompatible(lock_req->lock_mode_, l2)) {
        return;
      }
    }
    if (lock_req->txn_id_ == lock_request_queue->upgrading_) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
    }
    lock_req->granted_ = true;
    granted_lockmode.insert(lock_req->lock_mode_);
  }
  lock_request_queue->cv_.notify_all();
}

/**
 * @brief determine whether current lock could upgrade to request lock, refer to lock note
 *
 */
auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED: {
      return requested_lock_mode != LockMode::INTENTION_SHARED;
    }
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE: {
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      return requested_lock_mode == LockMode::EXCLUSIVE;
    }
    default: {
      return false;
    }
  }
}

void LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) {
  auto txn_id = txn->GetTransactionId();
  if (row_lock_mode == LockMode::INTENTION_EXCLUSIVE || row_lock_mode == LockMode::INTENTION_SHARED ||
      row_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    auto e = TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    txn->SetState(TransactionState::ABORTED);
    fmt::print("{}\n", e.GetInfo());
    throw e;
  }
  if (row_lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      auto e = TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
      txn->SetState(TransactionState::ABORTED);
      fmt::print("{}\n", e.GetInfo());
      throw e;
    }
  }
}

/**------------------------------------------ deadlock ---------------------------------------------------------------*/

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  for (auto txn : waits_for_[t1]) {
    if (txn == t2) {
      return;
    }
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.count(t1) == 0) {
    return;
  }
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); ++it) {
    if (t2 == *it) {
      waits_for_[t1].erase(it);
      return;
    }
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  fmt::print("-----------------------HasCycle()-----------------------\n");
  for (auto [source, targets] : waits_for_) {
    fmt::print("{} : [", source);
    for (auto target : targets) {
      fmt::print("{}, ", target);
    }
    fmt::print("]\n");
  }

  std::unordered_set<txn_id_t> visited;
  std::vector<txn_id_t> sources;
  std::for_each(waits_for_.begin(), waits_for_.end(),
                [&](const std::pair<txn_id_t, std::vector<txn_id_t>> &it) { sources.push_back(it.first); });
  std::sort(sources.begin(), sources.end());
  for (auto source : sources) {
    if (visited.count(source) == 0) {
      std::vector<txn_id_t> path;
      std::unordered_set<txn_id_t> on_path;
      visited.insert(source);
      if (FindCycle(source, path, on_path, visited, txn_id)) {
        return true;
      }
    }
  }
  return false;
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  for (auto next_txn : waits_for_[source_txn]) {
    if (visited.count(next_txn) > 0) {
      for (auto it = path.begin(); it != path.end(); ++it) {
        if (*it == next_txn) {
          auto max_iter = std::max_element(it, path.end());
          *abort_txn_id = *max_iter;
          return true;
        }
      }
    }
    visited.insert(next_txn);
    path.push_back(next_txn);
    if (FindCycle(next_txn, path, on_path, visited, abort_txn_id)) {
      return true;
    }
    path.pop_back();
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto [source, targets] : waits_for_) {
    for (auto target : targets) {
      edges.emplace_back(source, target);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  fmt::print("-----------------------RunCycleDetection()-----------------------\n");
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    fmt::print("-----------------------start detection-----------------------\n");
    {  // TODO(students): detect deadlock
      waits_for_.clear();
      {  // init waits_for_
        std::lock_guard<std::mutex> lock(table_lock_map_latch_);
        for (auto [table_oid, lock_request_queue] : table_lock_map_) {
          std::lock_guard<std::mutex> queue_lock(lock_request_queue->latch_);
          auto pre = lock_request_queue->request_queue_.begin();
          for (auto it = pre; it != lock_request_queue->request_queue_.end(); ++it) {
            if (it == pre) {
              continue;
            }
            if (!(*it)->granted_) {
              AddEdge((*it)->txn_id_, (*pre)->txn_id_);
            }
            pre = it;
          }
        }
      }
      {
        std::lock_guard<std::mutex> lock(row_lock_map_latch_);
        for (auto [rid, lock_request_queue] : row_lock_map_) {
          std::lock_guard<std::mutex> queue_lock(lock_request_queue->latch_);
          auto pre = lock_request_queue->request_queue_.begin();
          for (auto it = pre; it != lock_request_queue->request_queue_.end(); ++it) {
            if (it == pre) {
              continue;
            }
            if (!(*it)->granted_) {
              AddEdge((*it)->txn_id_, (*pre)->txn_id_);
            }
            pre = it;
          }
        }
      }
      // sort waits_for_ by txn_id
      for (auto &[source, targets] : waits_for_) {
        std::sort(targets.begin(), targets.end());
      }

      txn_id_t aborted_txn = INVALID_TXN_ID;
      while (HasCycle(&aborted_txn)) {
        fmt::print("abort txn ==> {}\n", aborted_txn);
        auto *txn = txn_manager_->GetTransaction(aborted_txn);
        txn->SetState(TransactionState::ABORTED);

        if (waits_for_.count(aborted_txn) > 0) {
          waits_for_[aborted_txn].clear();
        }
        for (auto [source, targets] : waits_for_) {
          RemoveEdge(source, aborted_txn);
        }
      }
      if (aborted_txn != INVALID_TXN_ID) {
        for (auto [table_oid, req_queue] : table_lock_map_) {
          req_queue->cv_.notify_all();
        }
        for (auto [rid, req_queue] : row_lock_map_) {
          req_queue->cv_.notify_all();
        }
      }
    }
  }
}

}  // namespace bustub
