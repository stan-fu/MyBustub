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
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto header_guard = bpm_->FetchPageBasic(header_page_id_);
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
  page_id_t root_id = GetRootPageId();
  auto guard = bpm_->FetchPageBasic(root_id);
  const auto *page = guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(page);
    int page_size = internal_page->GetSize();
    for (int i = 1; i < page_size; i++) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        guard = bpm_->FetchPageBasic(internal_page->ValueAt(i - 1));
        page = guard.As<BPlusTreePage>();
        break;
      }
    }
    if (comparator_(key, internal_page->KeyAt(page_size - 1)) >= 0) {
      guard = bpm_->FetchPageBasic(internal_page->ValueAt(page_size - 1));
      page = guard.As<BPlusTreePage>();
    }
  }

  auto leaf_page = reinterpret_cast<const LeafPage *>(page);
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      return true;
    }
  }

  return false;
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
  // Declaration of context instance.
  Context ctx;
  page_id_t root_id = GetRootPageId();
  ctx.root_page_id_ = root_id;

  // create an empty leaf node, which is also the root
  if (root_id == INVALID_PAGE_ID) {
    page_id_t page_id;
    auto guard = bpm_->NewPageGuarded(&page_id);
    auto *page = guard.AsMut<LeafPage>();
    auto header_guard = bpm_->FetchPageBasic(header_page_id_);
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    page->Init(leaf_max_size_);
    page->Insert(key, value, comparator_);
    header_page->root_page_id_ = page_id;
    return true;
  }

  // find the leaf node that should contain key
  auto guard = bpm_->FetchPageBasic(root_id);
  auto *page = guard.AsMut<BPlusTreePage>();
  ctx.access_record_.push_back(std::move(guard));  // tmp: store BasicPageGuard as record

  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    int page_size = internal_page->GetSize();
    for (int i = 1; i < page_size; i++) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        BasicPageGuard guard = bpm_->FetchPageBasic(internal_page->ValueAt(i - 1));
        page = guard.AsMut<BPlusTreePage>();
        ctx.access_record_.push_back(std::move(guard));
        break;
      }
    }
    if (comparator_(key, internal_page->KeyAt(page_size - 1)) >= 0) {
      BasicPageGuard guard = bpm_->FetchPageBasic(internal_page->ValueAt(page_size - 1));
      page = guard.AsMut<BPlusTreePage>();
      ctx.access_record_.push_back(std::move(guard));
    }
  }

  if (!page->IsFull()) {
    auto leaf_page = reinterpret_cast<LeafPage *>(page);
    return leaf_page->Insert(key, value, comparator_);
  }
  // split leaf page
  std::vector<MappingType> tmp_array;
  auto cur_leaf = reinterpret_cast<LeafPage *>(page);
  cur_leaf->AccquireArray(tmp_array);
  size_t array_size = tmp_array.size();
  if (comparator_(key, tmp_array[array_size - 1].first) > 0) {
    tmp_array.push_back({key, value});
  }
  for (size_t i = 0; i < array_size; i++) {
    int res = comparator_(key, tmp_array[i].first);
    if (res > 0) {
      continue;
    } else if (res < 0) {
      tmp_array.insert(tmp_array.begin() + i, {key, value});
      break;
    } else {
      return false;
    }
  }

  page_id_t new_pid;
  auto new_leaf_guard = bpm_->NewPageGuarded(&new_pid);
  auto *new_leaf = new_leaf_guard.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);
  cur_leaf->Reallocate(tmp_array, 0, cur_leaf->GetMinSize());
  new_leaf->Reallocate(tmp_array, cur_leaf->GetMinSize(), static_cast<int>(tmp_array.size()));
  cur_leaf->SetNextPageId(new_pid);
  auto new_key = new_leaf->KeyAt(0);
  InsertInParent(ctx, new_key, new_pid);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(Context &context, KeyType &key, page_id_t right_child_pid) {
  auto left_child_guard = std::move(context.access_record_.back());
  auto left_child_pid = left_child_guard.PageId();
  context.access_record_.pop_back();
  if (context.IsRootPage(left_child_pid)) {
    page_id_t parent_pid;
    auto parent_guard = bpm_->NewPageGuarded(&parent_pid);  // create new root
    auto *parent_page = parent_guard.AsMut<InternalPage>();
    parent_page->Init(internal_max_size_);
    parent_page->Insert(key, left_child_pid, comparator_);
    parent_page->Insert(key, right_child_pid, comparator_);
    auto header_guard = bpm_->FetchPageBasic(header_page_id_);
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = parent_pid;
    return;
  }

  auto parent_guard = std::move(context.access_record_.back());
  auto *parent_page = parent_guard.AsMut<InternalPage>();
  context.access_record_.pop_back();
  if (!parent_page->IsFull()) {
    parent_page->Insert(key, right_child_pid, comparator_);
  } else {
    page_id_t uncle_pid;
    auto uncle_guard = bpm_->NewPageGuarded(&uncle_pid);
    auto *uncle_page = uncle_guard.AsMut<InternalPage>();
    uncle_page->Init(internal_max_size_);
    std::vector<std::pair<KeyType, page_id_t>> tmp_array;
    parent_page->AccquireArray(tmp_array);
    size_t array_size = tmp_array.size();
    for (size_t i = 1; i < array_size; i++) {
      if (comparator_(key, tmp_array[i].first) < 0) {
        tmp_array.insert(tmp_array.begin() + i, {key, right_child_pid});
        break;
      }
    }
    if (comparator_(key, tmp_array[array_size - 1].first) > 0) {
      tmp_array.push_back({key, right_child_pid});
    }
    int min_page_size = parent_page->GetMinSize();
    parent_page->Reallocate(tmp_array, 0, min_page_size);
    uncle_page->Reallocate(tmp_array, min_page_size, tmp_array.size());
    context.access_record_.push_back(std::move(parent_guard));
    auto key_to_uncle = tmp_array[min_page_size].first;
    InsertInParent(context, key_to_uncle, uncle_pid);
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
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
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
  page_id_t root_id = GetRootPageId();
  auto page_guard = bpm_->FetchPageBasic(root_id);
  auto page = page_guard.AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    page_guard = bpm_->FetchPageBasic(internal_page->ValueAt(0));
    page = page_guard.AsMut<BPlusTreePage>();
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page);
  iterator_ = new INDEXITERATOR_TYPE(leaf_page, 0);
  return *iterator_;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t root_id = GetRootPageId();
  auto page_guard = bpm_->FetchPageBasic(root_id);
  auto page = page_guard.AsMut<BPlusTreePage>();

  while (!page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    int page_size = internal_page->GetSize();
    for (int i = 1; i < page_size; i++) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        page_guard = bpm_->FetchPageBasic(internal_page->ValueAt(i - 1));
        page = page_guard.AsMut<BPlusTreePage>();
        break;
      }
    }
    if (comparator_(key, internal_page->KeyAt(page_size - 1)) >= 0) {
      page_guard = bpm_->FetchPageBasic(internal_page->ValueAt(page_size - 1));
      page = page_guard.AsMut<BPlusTreePage>();
    }
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page);
  iterator_ = new INDEXITERATOR_TYPE(leaf_page, 0);
  while (comparator_((**iterator_).first, key) != 0) {
    ++(*iterator_);
  }
  return *iterator_;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  INDEXITERATOR_TYPE end_iterator(*iterator_);
  end_iterator.SetEnd();
  return end_iterator;
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
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
