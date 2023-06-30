//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"
namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
      : bpm_(bpm), page_id_(page_id), index_(index) {}
  IndexIterator(const IndexIterator &iter) = default;
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return index_ == itr.index_ && page_id_ == itr.page_id_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return index_ != itr.index_ || page_id_ != itr.page_id_; }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  page_id_t page_id_;
  int index_;
};

}  // namespace bustub
