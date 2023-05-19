//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, DISABLED_SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, DISABLED_DropTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_0, page_id_1, page_id_2;
  auto *page0 = bpm->NewPage(&page_id_0);
  auto *page1 = bpm->NewPage(&page_id_1);
  auto *page2 = bpm->NewPage(&page_id_2);
  {
    auto basic_page_guard0 = BasicPageGuard(bpm.get(), page0);
    basic_page_guard0.Drop();
    basic_page_guard0.Drop();
  }
  auto read_page_guard1 = ReadPageGuard(bpm.get(), page1);
  auto write_page_guard2 = WritePageGuard(bpm.get(), page2);
  // auto read_page_guard2 = ReadPageGuard(bpm.get(), page2);

  // auto basic_page_guard1 = bpm->NewPageGuarded(&page_id_1);
  // auto basic_page_guard2 = bpm->NewPageGuarded(&page_id_2);

  // auto read_page_guard1 = bpm->FetchPageRead(1);
  // auto write_page_guard1 = bpm->FetchPageWrite(page_id_1);
  // auto read_page_guard2 = bpm->FetchPageRead(2);
  // auto write_page_guard2 = bpm->FetchPageWrite(page_id_2);

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, DISABLED_MoveTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 3;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_0, page_id_1;
  auto *page0 = bpm->NewPage(&page_id_0);
  auto *page1 = bpm->NewPage(&page_id_1);

  auto guarded_page0 = BasicPageGuard(bpm.get(), page0);
  auto guarded_page1 = BasicPageGuard(bpm.get(), page1);
  guarded_page0 = std::move(guarded_page1);
  EXPECT_EQ(0, page0->GetPinCount());
  EXPECT_EQ(1, page1->GetPinCount());

  BasicPageGuard guarded_page_copy(std::move(guarded_page0));

  page_id_t page_id_2, page_id_3;
  auto *page2 = bpm->NewPage(&page_id_2);
  auto *page3 = bpm->NewPage(&page_id_3);
  auto guarded_page2 = ReadPageGuard(bpm.get(), page2);
  auto guarded_page3 = ReadPageGuard(bpm.get(), page3);
  guarded_page2 = std::move(guarded_page3);

  auto guarded_page_tmp = std::move(guarded_page2);

  page_id_t page_id_4;
  auto *page4 = bpm->NewPage(&page_id_4);
  auto guarded_page4 = WritePageGuard(bpm.get(), page4);
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, BPMTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_0;
  auto guarded_page0 = bpm->NewPageGuarded(&page_id_0);

  auto *page0copy = bpm->FetchPage(page_id_0);
  guarded_page0.Drop();
  guarded_page0.Drop();
  EXPECT_EQ(1, page0copy->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
