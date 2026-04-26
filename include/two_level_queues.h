#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "kv_engine.h"

#define ALLOCATE_BUFFER_SIZE (512UL) // 16MB
#define RECLAIM_ALLOCATE_BUFFER_SIZE (16 << 10) // 64MB
#define DEALLOCATE_BUFFER_SIZE (16 << 10) // 64MB
#define NUM_ONLINE_CPUS 128 // Must match kernel NUM_KFIFOS_{ALLOC,FREE}
#define NUM_CPUS_PER_THREAD 48
#define LOW_MEM_WATERMARK (8UL << 10)
#define MID_MEM_WATERMARK (12UL << 10)
#define HIGH_MEM_WATERMARK (16UL << 10)
#define MAX_QUEUE_LEN (20UL << 10)
#define MEM_NODE_NUM 4

#define ALLOCATOR_DEVICE "/allocator_page_queue"
#define DEALLOCATOR_DEVICE "/deallocator_page_queue"

struct allocator_page_queue {
    std::atomic<int32_t> rkey[MEM_NODE_NUM];
    std::atomic<int64_t> begin;
    std::atomic<int64_t> end;
    std::atomic<int64_t> pages[ALLOCATE_BUFFER_SIZE];
};

struct deallocator_page_queue {
    std::atomic<int64_t> begin;
    std::atomic<int64_t> end;
    std::atomic<int64_t> pages[DEALLOCATE_BUFFER_SIZE];
};

struct allocator_page_queues {
  struct allocator_page_queue queues[NUM_ONLINE_CPUS];
};

struct deallocator_page_queues {
  struct deallocator_page_queue queues[NUM_ONLINE_CPUS];
};

struct allocator_page_queues *queues_allocator = nullptr;
struct deallocator_page_queues *queues_deallocator = nullptr;

int page_queue_shm_init() {
  int fd = shm_open(ALLOCATOR_DEVICE, O_CREAT | O_RDWR, 0600);
  if (fd < 0) {
    perror("shm_open allocator");
    return -1;
  }
  if (ftruncate(fd, sizeof(struct allocator_page_queues)) == -1) {
    perror("ftruncate");
    close(fd);
    return -1;
  }
  queues_allocator = (struct allocator_page_queues *)mmap(NULL, sizeof(struct allocator_page_queues), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (queues_allocator == MAP_FAILED) {
    perror("Failed to mmap queue_allocator.");
    close(fd);
    return -1;
  }
  //memset(queue_allocator, 0, sizeof(struct allocator_page_queue));
  for(uint32_t i = 0;i < NUM_ONLINE_CPUS; ++i) {
    auto queue_allocator = &queues_allocator->queues[i];
    for(uint32_t j = 0; j < MEM_NODE_NUM; ++j) {
        queue_allocator->rkey[j].store(0);
    }
    queue_allocator->begin.store(0);
    queue_allocator->end.store(0);
    for(uint32_t j = 0;j < ALLOCATE_BUFFER_SIZE; ++j) {
      queue_allocator->pages[j].store(0);
    }
  }

  fd = shm_open(DEALLOCATOR_DEVICE, O_CREAT | O_RDWR, 0600);
  if (fd < 0) {
    perror("shm_open deallocator");
    return -1;
  }
  if (ftruncate(fd, sizeof(struct deallocator_page_queues)) == -1) {
    perror("ftruncate");
    close(fd);
    return -1;
  }
  queues_deallocator = (deallocator_page_queues *)mmap(NULL, sizeof(struct deallocator_page_queues), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (queues_deallocator == MAP_FAILED) {
    perror("Failed to mmap queue_deallocator.");
    close(fd);
    return -1;
  }
  //memset(queue_deallocator, 0, sizeof(struct deallocator_page_queue));
  for(uint32_t i = 0;i < NUM_ONLINE_CPUS; ++i) {
    auto queue_deallocator = &queues_deallocator->queues[i];
    queue_deallocator->begin.store(0);
    queue_deallocator->end.store(0);
    for(uint64_t i = 0;i < DEALLOCATE_BUFFER_SIZE; ++i) {
      queue_deallocator->pages[i].store(0);
    }
  }
  
  return 0;
}

uint64_t get_length_allocator(uint32_t id) {
    auto queue_allocator = &queues_allocator->queues[id];
    uint64_t begin = queue_allocator->begin.load();
    uint64_t end = queue_allocator->end.load();
    if (begin == end) {
        return 0;
    }
    if (end > begin) {
        return (end - begin);
    } else {
        return (ALLOCATE_BUFFER_SIZE - begin + end);
    }
}

uint64_t get_length_deallocator(uint32_t id) {
    auto queue_deallocator = &queues_deallocator->queues[id];
    uint64_t begin = queue_deallocator->begin.load();
    uint64_t end = queue_deallocator->end.load();
    if (begin == end) {
        return 0;
    }
    if (end > begin) {
        return (end - begin);
    } else {
        return (DEALLOCATE_BUFFER_SIZE - begin + end);
    }
}

int push_queue_allocator(uint64_t page_addr, uint32_t id) {
    auto queue_allocator = &queues_allocator->queues[id];
    int ret = 0;
    uint64_t prev_end = queue_allocator->end.load();
    while(get_length_allocator(id) >= ALLOCATE_BUFFER_SIZE - 1) ;
    queue_allocator->end.store((prev_end+ 1) % ALLOCATE_BUFFER_SIZE);
    queue_allocator->pages[prev_end].store(page_addr);
    return ret;
}

uint64_t pop_queue_deallocator(uint32_t id) {
    auto queue_deallocator = &queues_deallocator->queues[id];
    uint64_t ret = 0;
    while(get_length_deallocator(id) == 0) ;
    uint64_t prev_begin = queue_deallocator->begin.load();
    queue_deallocator->begin.store((prev_begin + 1) % DEALLOCATE_BUFFER_SIZE);
    while(queue_deallocator->pages[prev_begin].load() == 0) ;
    ret = queue_deallocator->pages[prev_begin].load();
    queue_deallocator->pages[prev_begin].store(0);
    return ret;
}


struct local_pool {
  uint64_t allocate_one_page() {
    assert(length > 0);
    uint64_t ret = remote_addrs[begin];
    begin = (begin + 1) % capacity;
    length -= 1;

    if(length < LOW_MEM_WATERMARK) {
      batch_allocate(MAX_BATCH_SIZE);
    }

    return ret;
  }

  bool deallocate_one_page(uint64_t page_addr) {
    assert(length < capacity);
    remote_addrs[end] = page_addr;
    end = (end + 1) % capacity;
    length += 1;
    
    if(length > HIGH_MEM_WATERMARK) {
      batch_deallocate(MAX_BATCH_SIZE);
    }

    return true;
  }

  local_pool(kv::LocalEngine *kv, uint16_t mnode_num) : conn(kv), begin(0), end(0), length(0), capacity(MAX_QUEUE_LEN), mnode_num(mnode_num) {
    int ret;
    remote_addrs = new uint64_t[capacity];
    assert(conn != nullptr);
    assert(mnode_num > 0 && mnode_num <= MEM_NODE_NUM);
    while(length < MID_MEM_WATERMARK) {
      if (!batch_allocate(MAX_BATCH_SIZE)) {
        usleep(1000);
      }
    }
  }

private:
  kv::LocalEngine *conn;
  uint16_t mnode_num = 1;
  uint16_t round_robin = 0;
  uint64_t* remote_addrs;
  uint64_t begin;
  uint64_t end;
  uint64_t capacity;
  uint64_t length;
  uint64_t batch_size;
  uint32_t selection_log_count = 0;
  static constexpr uint64_t kMnodeMask = (((uint64_t)0x7FUL) << 57);
  static constexpr uint32_t kSelectionLogLimit = 8;

  struct batch_allocate_candidate {
    uint16_t mnode;
    uint64_t free_bytes;
  };

  uint16_t round_robin_distance(uint16_t mnode) const {
    return (mnode + mnode_num - round_robin) % mnode_num;
  }

  void tag_remote_addr(uint64_t& addr, uint16_t mnode) {
    // Preserve the original page address and encode the chosen memory node in the high bits.
    addr &= ~kMnodeMask;
    addr |= (static_cast<uint64_t>(mnode) << 57);
  }

  bool read_memory_node_status(uint16_t mnode, kv::MemoryNodeStatus& status) {
    return conn[mnode].read_memory_node_status(status) == 0;
  }

  void tag_allocated_pages(uint16_t mnode, uint64_t start, uint64_t size) {
    for (uint64_t i = 0; i < size; ++i) {
      tag_remote_addr(remote_addrs[(start + i) % capacity], mnode);
    }
  }

  bool allocate_batch_from_node(uint16_t mnode, uint64_t size) {
    int ret;
    if (capacity - end >= size) {
      ret = conn[mnode].allocate_remote_page_batch(remote_addrs + end, size);
      if (ret) {
        std::cerr << "allocate_remote_page_batch fail on mnode "
                  << mnode << std::endl;
        return false;
      }
      tag_allocated_pages(mnode, end, size);
    } else {
      uint64_t remote_addrs_tmp[MAX_BATCH_SIZE];
      ret = conn[mnode].allocate_remote_page_batch(remote_addrs_tmp, size);
      if (ret) {
        std::cerr << "allocate_remote_page_batch fail on mnode "
                  << mnode << std::endl;
        return false;
      }
      uint64_t size_first = capacity - end;
      std::copy(remote_addrs_tmp, remote_addrs_tmp + size_first,
                remote_addrs + end);
      std::copy(remote_addrs_tmp + size_first, remote_addrs_tmp + size,
                remote_addrs);
      tag_allocated_pages(mnode, end, size);
    }
    round_robin = (mnode + 1) % mnode_num;
    end = (end + size) % capacity;
    length += size;
    return true;
  }

  void maybe_log_batch_selection(const batch_allocate_candidate* candidates,
                                 uint16_t chosen_mnode, uint64_t size) {
    if (selection_log_count >= kSelectionLogLimit) {
      return;
    }
    std::cout << "[batch_allocate] size=" << size
              << " rr_hint=" << round_robin
              << " choose_mnode=" << chosen_mnode
              << " candidates_free_bytes={";
    for (uint16_t i = 0; i < mnode_num; ++i) {
      if (i != 0) {
        std::cout << ", ";
      }
      std::cout << candidates[i].mnode << ":" << candidates[i].free_bytes;
    }
    std::cout << "}" << std::endl;
    selection_log_count++;
  }

  bool batch_allocate(uint64_t size) {
    assert(size + length <= capacity);
    batch_allocate_candidate candidates[MEM_NODE_NUM];
    const uint64_t required_bytes = size << PAGE_SHIFT;
    for (uint16_t i = 0; i < mnode_num; ++i) {
      kv::MemoryNodeStatus status = {};
      uint64_t free_bytes = 0;
      if (read_memory_node_status(i, status)) {
        free_bytes = status.free_bytes;
      }
      candidates[i] = {i, free_bytes};
    }

    // Prefer nodes with the most free memory; round_robin only breaks ties.
    std::sort(candidates, candidates + mnode_num,
              [this, required_bytes](const batch_allocate_candidate& lhs,
                                     const batch_allocate_candidate& rhs) {
                const bool lhs_has_capacity = lhs.free_bytes >= required_bytes;
                const bool rhs_has_capacity = rhs.free_bytes >= required_bytes;
                if (lhs_has_capacity != rhs_has_capacity) {
                  return lhs_has_capacity > rhs_has_capacity;
                }
                if (lhs.free_bytes != rhs.free_bytes) {
                  return lhs.free_bytes > rhs.free_bytes;
                }
                return round_robin_distance(lhs.mnode) <
                       round_robin_distance(rhs.mnode);
              });

    for (uint16_t i = 0; i < mnode_num; ++i) {
      if (allocate_batch_from_node(candidates[i].mnode, size)) {
        maybe_log_batch_selection(candidates, candidates[i].mnode, size);
        return true;
      }
    }

    round_robin = (round_robin + 1) % mnode_num;
    return false;
  }

  void batch_deallocate(uint64_t size) {
    assert(size <= length); // 确保有足够的元素可释放

    int ret;
    if (begin + size <= capacity) {
      for(int i = 0; i < size; ++i) {
        uint64_t addr = remote_addrs[(begin + i) % capacity];
        uint16_t mnode = (addr >> 57);
        if(mnode != 0) {
            // std::cout << "Free Remote address: " << addr << ", ";
            addr &= ~kMnodeMask; // 清除高位的mnode tag
            // std::cout << addr << std::endl;
        }
        ret = conn[mnode].free_remote_page(addr);
        if (ret) {
            std::cerr << "free_remote_page_batch fail: " << addr << std::endl;
            return;
        }
      }
    } else {
        uint64_t size_first = capacity - begin;
        uint64_t remote_addrs_tmp[MAX_BATCH_SIZE];
        std::copy(remote_addrs + begin, remote_addrs + capacity, remote_addrs_tmp);
        std::copy(remote_addrs, remote_addrs + size-size_first, remote_addrs_tmp + size_first);
        for(int i = 0; i < size; ++i) {
          uint64_t addr = remote_addrs_tmp[i];
          uint16_t mnode = (addr >> 57);
          if(mnode != 0 ) {
            // std::cout << "Free Remote address: " << addr << ", ";
            addr &= ~kMnodeMask; // 清除高位的mnode tag
            // std::cout << addr << std::endl;
          }
          ret = conn[mnode].free_remote_page(addr);
          if (ret) {
              std::cerr << "free_remote_page_batch fail: " << addr << std::endl;
              return;
          }
        }
    }
    begin = (begin + size) % capacity;
    length -= size;
  }
};
