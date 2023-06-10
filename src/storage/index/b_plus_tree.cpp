#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  fmt::print("BPlusTree -- leaf_max = {}, internal_max = {}\n", leaf_max_size, internal_max_size);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  fmt::print("GetValue( {} ) \n", key.ToString());
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  page_id_t root_id = GetRootPageId();
  guard = bpm_->FetchPageRead(root_id);
  const auto *page = guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    guard = bpm_->FetchPageRead(internal_page->Find(key, comparator_));
    page = guard.As<BPlusTreePage>();
  }

  auto leaf_page = reinterpret_cast<const LeafPage *>(page);
  auto [success, index] = leaf_page->Find(key, comparator_);
  if (success) {
    result->push_back(leaf_page->ValueAt(index));
  }
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  fmt::print("Insert( {} )\n", key.ToString());
  Context ctx;
  page_id_t pid;
  BPlusTreePage *page;
  WritePageGuard guard;

  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  pid = header_page->root_page_id_;
  // create new tree if root is empty
  if (pid == INVALID_PAGE_ID) {
    bpm_->NewPageGuarded(&pid);
    guard = bpm_->FetchPageWrite(pid);
    auto root_page = guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->Insert(key, value, comparator_);
    header_page->root_page_id_ = pid;
    return true;
  }

  // find the leaf node that should contain key
  guard = bpm_->FetchPageWrite(pid);
  page = guard.AsMut<BPlusTreePage>();
  ctx.root_page_id_ = pid;
  ctx.write_set_.push_back(std::move(guard));

  while (!page->IsLeafPage()) {
    WritePageGuard guard;
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    guard = bpm_->FetchPageWrite(internal_page->Find(key, comparator_));
    page = guard.AsMut<BPlusTreePage>();

    if (!page->IsFull()) {  // release all previous lock if cur page is safe
      ctx.header_page_ = std::nullopt;
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(guard));
  }

  LeafPage *old_leaf;
  old_leaf = reinterpret_cast<LeafPage *>(page);

  if (!page->IsFull()) {
    return old_leaf->Insert(key, value, comparator_);
  }

  // leaf page split
  std::vector<MappingType> array;
  old_leaf->GetArray(array);
  auto it = std::upper_bound(array.begin(), array.end(), key, [this](const KeyType &key, MappingType &mp) {
    return this->comparator_(key, mp.first) < 0;
  });
  int index = it - array.begin();
  if (index == 0 || comparator_(key, (it - 1)->first) != 0) {
    array.insert(it, {key, value});
  } else {
    return false;
  }

  LeafPage *new_leaf;
  page_id_t new_pid;
  bpm_->NewPageGuarded(&new_pid);
  guard = bpm_->FetchPageWrite(new_pid);
  new_leaf = guard.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);

  int old_size = (old_leaf->GetMaxSize() + 1) / 2;
  old_leaf->SetArray(array, 0, old_size);
  new_leaf->SetArray(array, old_size, static_cast<int>(array.size()));

  page_id_t next_pid = old_leaf->GetNextPageId();
  old_leaf->SetNextPageId(new_pid);
  new_leaf->SetNextPageId(next_pid);

  InsertInParent(ctx, new_leaf->KeyAt(0), new_pid);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(Context &ctx, const KeyType &key, page_id_t right_child_pid) {
  auto left_child_guard = std::make_unique<WritePageGuard>(std::move(ctx.write_set_.back()));
  auto left_child_pid = left_child_guard->PageId();
  ctx.write_set_.pop_back();
  if (ctx.IsRootPage(left_child_pid)) {
    // create new root
    page_id_t new_root_pid;
    auto new_root_guard = bpm_->NewPageGuarded(&new_root_pid);
    auto *new_root = new_root_guard.AsMut<InternalPage>();
    new_root->Init();  // TEST -- init as max size that page memory allowed
    new_root->Insert(key, left_child_pid, comparator_);
    new_root->Insert(key, right_child_pid, comparator_);
    BUSTUB_ASSERT(ctx.header_page_ != std::nullopt, "root latch release error");
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_pid;
    return;
  }

  left_child_guard.reset();
  auto *parent_page = ctx.write_set_.back().AsMut<InternalPage>();
  if (!parent_page->IsFull()) {
    parent_page->Insert(key, right_child_pid, comparator_);
  } else {
    page_id_t uncle_pid;
    auto uncle_guard = std::make_unique<BasicPageGuard>(bpm_->NewPageGuarded(&uncle_pid));
    auto *uncle_page = uncle_guard->AsMut<InternalPage>();
    uncle_page->Init();  // TEST
    std::vector<std::pair<KeyType, page_id_t>> array;
    parent_page->GetArray(array);
    auto it = std::upper_bound(
        array.begin() + 1, array.end(), key,
        [this](const KeyType &k, std::pair<KeyType, page_id_t> &kv) { return this->comparator_(k, kv.first) < 0; });
    array.insert(it, {key, right_child_pid});

    int min_size = parent_page->GetMinSize();
    parent_page->SetArray(array, 0, min_size);
    uncle_page->SetArray(array, min_size, array.size());
    auto key_to_uncle = array[min_size].first;
    uncle_guard.reset();
    InsertInParent(ctx, key_to_uncle, uncle_pid);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  fmt::print("Remove ( {} )\n", key.ToString());
  Context ctx;
  page_id_t pid;
  WritePageGuard guard;
  BPlusTreePage *page;

  // lock header page
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  pid = GetRootPageId();
  ctx.root_page_id_ = pid;
  ctx.header_page_ = std::move(header_guard);

  // accquire root page
  guard = bpm_->FetchPageWrite(pid);
  page = guard.AsMut<BPlusTreePage>();
  ctx.write_set_.push_back(std::move(guard));

  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    guard = bpm_->FetchPageWrite(internal_page->Find(key, comparator_));
    page = guard.AsMut<BPlusTreePage>();
    if (page->GetSize() > page->GetMinSize()) {
      ctx.write_set_.clear();
      ctx.header_page_ = std::nullopt;
    }
    ctx.write_set_.push_back(std::move(guard));
  }
  DeleteEntry(ctx, key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(Context &ctx, const KeyType &key) {
  auto guard = std::make_unique<WritePageGuard>(std::move(ctx.write_set_.back()));
  auto page = guard->AsMut<BPlusTreePage>();
  ctx.write_set_.pop_back();
  if (page->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<LeafPage *>(page);
    leaf_page->DeleteEntry(key, comparator_);
  } else {
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    internal_page->DeleteEntry(key, comparator_);
  }

  if (ctx.IsRootPage(guard->PageId())) {
    if (page->IsLeafPage()) {
      return;
    }
    if (page->GetSize() == 1) {
      // make the only child to be the new root
      auto internal_page = reinterpret_cast<InternalPage *>(page);
      auto guard = bpm_->FetchPageRead(internal_page->ValueAt(0));
      BUSTUB_ASSERT(ctx.header_page_ != std::nullopt, "root latch release error");
      auto root_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      root_page->root_page_id_ = guard.PageId();
      // should delete old root page through bpm, but bpm is not implemented, just ignore
    }
  } else if (page->GetSize() < page->GetMinSize()) {
    auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();
    int index = parent_page->ValueIndex(guard->PageId());
    BPlusTreePage *left_page;
    BPlusTreePage *right_page;
    KeyType parent_key;
    std::unique_ptr<WritePageGuard> sibling_guard;
    if (index < parent_page->GetSize() - 1) {  // parent has next child after page
      index += 1;
      parent_key = parent_page->KeyAt(index);  // key of page && next_page
      page_id_t next_pid = parent_page->ValueAt(index);
      sibling_guard = std::make_unique<WritePageGuard>(bpm_->FetchPageWrite(next_pid));
      right_page = sibling_guard->AsMut<BPlusTreePage>();
      left_page = page;
    } else {
      parent_key = parent_page->KeyAt(index);
      page_id_t pre_pid = parent_page->ValueAt(index - 1);
      sibling_guard = std::make_unique<WritePageGuard>(bpm_->FetchPageWrite(pre_pid));
      left_page = sibling_guard->AsMut<BPlusTreePage>();
      right_page = page;
    }
    if (left_page->GetSize() + right_page->GetSize() <= left_page->GetMaxSize()) {
      // merge into left node
      if (left_page->IsLeafPage()) {
        auto left_node = reinterpret_cast<LeafPage *>(left_page);
        auto right_node = reinterpret_cast<LeafPage *>(right_page);
        left_node->SetNextPageId(right_node->GetNextPageId());
        std::vector<MappingType> array;
        right_node->GetArray(array);
        for (auto &&entry : array) {
          left_node->Insert(entry.first, entry.second, comparator_);
        }
      } else {
        auto left_node = reinterpret_cast<InternalPage *>(left_page);
        auto right_node = reinterpret_cast<InternalPage *>(right_page);
        std::vector<std::pair<KeyType, page_id_t>> array;
        right_node->GetArray(array);
        array[0].first = parent_key;
        for (auto &&entry : array) {
          left_node->Insert(entry.first, entry.second, comparator_);
        }
      }

      // release all children's latch, otherwise will be deadlock
      guard.reset();
      sibling_guard.reset();
      DeleteEntry(ctx, parent_key);
    } else {
      // redistribution: borrow an entry from sibling
      // optimize: the ancestor node can be unlocked ahead of time
      while (ctx.write_set_.size() > 1) {
        ctx.write_set_.pop_front();
        ctx.header_page_ = std::nullopt;
      }
      if (left_page->IsLeafPage()) {
        auto left_node = reinterpret_cast<LeafPage *>(left_page);
        auto right_node = reinterpret_cast<LeafPage *>(right_page);
        int left_size = left_page->GetSize();
        if (left_size < left_page->GetMinSize()) {
          left_node->Insert(right_node->KeyAt(0), right_node->ValueAt(0), comparator_);
          right_node->DeleteEntry(right_node->KeyAt(0), comparator_);
        } else {
          right_node->Insert(left_node->KeyAt(left_size - 1), left_node->ValueAt(left_size - 1), comparator_);
          left_node->IncreaseSize(-1);
        }
        parent_page->SetKeyAt(index, right_node->KeyAt(0));
      } else {
        auto left_node = reinterpret_cast<InternalPage *>(left_page);
        auto right_node = reinterpret_cast<InternalPage *>(right_page);
        int left_size = left_page->GetSize();
        if (left_size < left_page->GetMinSize()) {
          left_node->Insert(parent_key, right_node->ValueAt(0), comparator_);
          parent_page->SetKeyAt(index, right_node->KeyAt(1));
          right_node->DeleteEntry(0);
        } else {
          right_node->Insert(parent_key, left_node->ValueAt(left_size - 1), 0);
          parent_page->SetKeyAt(index, left_node->KeyAt(left_size - 1));
          left_node->IncreaseSize(-1);
        }
      }
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  fmt::print("Begin()\n");
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  page_id_t root_id = GetRootPageId();
  guard = bpm_->FetchPageRead(root_id);
  auto page = guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    guard = bpm_->FetchPageRead(internal_page->ValueAt(0));
    page = guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  fmt::print("Begin( {} )\n", key.ToString());
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  page_id_t root_id = GetRootPageId();
  guard = bpm_->FetchPageRead(root_id);
  auto page = guard.As<BPlusTreePage>();

  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    guard = bpm_->FetchPageRead(internal_page->Find(key, comparator_));
    page = guard.As<BPlusTreePage>();
  }
  auto leaf_page = guard.As<LeafPage>();
  return INDEXITERATOR_TYPE(bpm_, guard.PageId(), leaf_page->Find(key, comparator_).second);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  page_id_t root_id = GetRootPageId();
  guard = bpm_->FetchPageRead(root_id);
  auto page = guard.As<BPlusTreePage>();

  while (!page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<const InternalPage *>(page);
    guard = bpm_->FetchPageRead(internal_page->ValueAt(page->GetSize() - 1));
    page = guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, guard.PageId(), page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
