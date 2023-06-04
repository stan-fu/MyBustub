//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"
#include "common/macros.h"
#include "include/common/logger.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // check if any free frame availiable
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    // if frame has a dirty page, write it back to disk
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      FlushPage(evicted_page_id);
    }
    page_table_.erase(evicted_page_id);
    pages_[frame_id].ResetMemory();
  } else {
    return nullptr;  // no new page could be created
  }

  *page_id = AllocatePage();
  pages_[frame_id].page_id_ = *page_id;
  disk_manager_->ReadPage(*page_id, pages_[frame_id].data_);
  page_table_[*page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) > 0) {
    auto frame_id = page_table_[page_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      FlushPage(evicted_page_id);
    }
    page_table_.erase(evicted_page_id);
    pages_[frame_id].ResetMemory();
  } else {
    return nullptr;
  }

  pages_[frame_id].page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0 || pages_[page_table_[page_id]].pin_count_ == 0) {
    return false;
  }

  if (--pages_[page_table_[page_id]].pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }

  pages_[page_table_[page_id]].is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto [page_id, _] : page_table_) {
    FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  if (pages_[frame_id].IsDirty()) {
    FlushPage(page_id);
  }
  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  free_list_.push_back(frame_id);
  // DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
