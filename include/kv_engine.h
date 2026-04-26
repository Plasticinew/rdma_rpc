#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <mutex>
#include <tbb/concurrent_set.h>
#include <iostream>
//#include <boost/asio.hpp>
#include <memory>

#include "kv_engine.h"
#include "msg.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
#include "string"
#include "thread"
#include "unordered_map"

#define PAGE_SHIFT 12
#define SWAP_AREA_SHIFT 36
#define SHARDING_NUM 32
static_assert(((SHARDING_NUM & (~SHARDING_NUM + 1)) == SHARDING_NUM),
              "RingBuffer's size must be a positive power of 2");

namespace kv {

struct Block {
  uint64_t addr;
  uint32_t rkey;

  Block(uint64_t a, uint32_t k) : addr(a), rkey(k) {

  }

  Block() : addr(0), rkey(0) {

  }
};

struct BlockQueue {
  uint64_t begin;
  uint64_t end;
  Block* blocks;
  uint64_t block_num;
  uint64_t capacity;
  std::mutex mtx;

  BlockQueue(uint64_t len) : begin(0), end(0), block_num(0), capacity(len) {
    blocks = new Block[len];
    assert(blocks != nullptr);
  }

  ~BlockQueue() {
    delete[] blocks;
  }

  int allocate(uint64_t& addr, uint32_t& rkey) {
    if(block_num == 0)
      return -1;
    addr = blocks[begin].addr;
    rkey = blocks[begin].rkey;
    begin = (begin + 1) % capacity;
    block_num--;
    return 0;
  }

  int free(uint64_t addr, uint32_t rkey) {
    if(block_num == capacity)
      return -1;
    blocks[end].addr = addr;
    blocks[end].rkey = rkey;
    end = (end + 1) % capacity;
    block_num++;
    return 0;
  }
};




struct PageQueue {
  uint64_t base_addr;
  uint64_t begin;
  uint64_t end;
  uint64_t* pages_addr;
  uint64_t page_num;
  uint64_t capacity;
  std::mutex mtx;

  PageQueue(uint64_t len, uint64_t base_addr) : base_addr(base_addr), begin(0), end(0), page_num(len), capacity(len) {
    pages_addr = new uint64_t[len];
    for(uint64_t i = 0;i < len; ++i)
      pages_addr[i] = base_addr + (i << PAGE_SHIFT);
  }

  ~PageQueue() {
    delete[] pages_addr;
  }

  int allocate(uint64_t& addr) {
    if(page_num == 0)
      return -1;
    addr = pages_addr[begin];
    begin = (begin + 1) % capacity;
    page_num--;
    return 0;
  }

  int allocate_batch(uint64_t* addrs, int num) {
    if (page_num < num) {
        return -1;
    }

    int first_chunk_size = std::min((uint64_t)num, capacity - begin);
    
    std::copy(pages_addr + begin, pages_addr + begin + first_chunk_size, addrs);

    if (num > first_chunk_size) {
        std::copy(pages_addr, pages_addr + (num - first_chunk_size), addrs + first_chunk_size);
    }

    begin = (begin + num) % capacity;
    page_num -= num;

    return 0; 
  }

  int free(uint64_t addr) {
    if(page_num == capacity)
      return -1;
    pages_addr[end] = addr;
    end = (end + 1) % capacity;
    page_num++;
    return 0;
  }

  int free_batch(uint64_t* addrs, int num) {
    if (capacity - page_num < (uint64_t)num) {
        return -1;
    }

    int first_chunk_size = std::min((uint64_t)num, capacity - end);

    std::copy(addrs, addrs + first_chunk_size, pages_addr + end);

    if (num > first_chunk_size) {
        std::copy(addrs + first_chunk_size, addrs + num, pages_addr);
    }

    end = (end + num) % capacity;
    page_num += num;

    return 0;  

  }
};

/* Abstract base engine */
class Engine {
 public:
  virtual ~Engine(){};

  virtual bool start(const std::string addr, const std::string port) = 0;
  virtual void stop() = 0;

  virtual bool alive() = 0;
};

/* Local-side engine */
class LocalEngine : public Engine {
 public:
  struct internal_value_t {
    uint64_t remote_addr;
    uint32_t rkey;
    uint32_t size;
  };

  ~LocalEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

  bool set_mnode(uint16_t mnode);
  bool write(const std::string key, const std::string value);
  bool read(const std::string key, std::string &value);
  bool write_block(uint64_t laddr, uint64_t raddr, uint64_t len, uint32_t rkey);
  bool read_block(uint64_t laddr, uint64_t raddr, uint64_t len, uint32_t rkey);
  int read_remote_buffer(void *buf, uint64_t len, uint64_t remote_addr,
                         uint32_t rkey);
  int read_memory_node_status(MemoryNodeStatus& status);

  int allocate_remote_page(uint64_t& value);
  int allocate_remote_block(uint64_t& value, uint32_t& rkey);
  int allocate_remote_page_batch(uint64_t* addr, int num);
  int free_remote_page(uint64_t value);
  int free_remote_page_batch(uint64_t* addr, int num);
  int get_global_rkey(uint32_t& global_rkey, uint64_t& memory_status_addr,
                      uint32_t& memory_status_rkey);

 private:
 kv::ConnectionManager *m_rdma_conn_;
  /* NOTE: should use some concurrent data structure, and also should take the
   * extra memory overhead into consideration */
  std::unordered_map<std::string, internal_value_t> m_map_[SHARDING_NUM];
  std::mutex m_mutex_[SHARDING_NUM];
  RDMAMemPool *m_rdma_mem_pool_;
  uint16_t m_mnode_;
  uint64_t m_memory_status_addr_ = 0;
  uint32_t m_memory_status_rkey_ = 0;
};

/* Remote-side engine */
class RemoteEngine : public Engine {
 public:
  struct WorkerInfo {
    CmdMsgBlock *cmd_msg;
    CmdMsgRespBlock *cmd_resp_msg;
    struct ibv_mr *msg_mr;
    struct ibv_mr *resp_mr;
    rdma_cm_id *cm_id;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;
    std::mutex cq_mutex;
  };

  ~RemoteEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

 private:
  void handle_connection();

  int create_connection(struct rdma_cm_id *cm_id, uint8_t connect_type);

  struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

  int remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey,
                   uint32_t length, uint64_t remote_addr, uint32_t rkey);

  int allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                   uint64_t size);

  int allocate_page(uint64_t &addr);
  int free_page(uint64_t addr);

  int allocate_block(uint64_t& addr, uint32_t& rkey);

  int allocate_page_batch(uint64_t* addr, int num);
  int free_page_batch(uint64_t* addr, int num);

  int allocate_page_regmr(uint64_t &addr);
  int free_page_deregmr(uint64_t addr);

  int allocate_page_malloc(uint64_t &addr);
  int free_page_malloc(uint64_t addr);

  void worker(WorkerInfo *work_info, uint32_t num);
  void main_worker();
  void handle_cq_async(ibv_comp_channel *comp_channel, ibv_cq *cq);

  void worker_handel_cq(ibv_cq * cq);

  void startWorker(int num);
  //boost::asio::awaitable<void> worker(WorkerInfo worker_info, int num);
  //void run();

  struct rdma_event_channel *m_cm_channel_;
  struct rdma_cm_id *m_listen_id_;
  struct ibv_pd *m_pd_;
  struct ibv_context *m_context_;
  struct PageQueue* page_queue = nullptr;
  struct BlockQueue* block_queue = nullptr;
  //boost::asio::io_context io_context_;

  void* base_addr = nullptr;
  ibv_mr* global_mr = nullptr;
  MemoryNodeStatus* memory_node_status_ = nullptr;
  ibv_mr* memory_node_status_mr_ = nullptr;

  std::unordered_map<uint64_t, ibv_mr*> mrmap;
  std::mutex mrmap_mtx;
  bool m_stop_;
  std::thread *m_conn_handler_;
  WorkerInfo **m_worker_info_;
  tbb::concurrent_set<WorkerInfo*> active_workers;
  uint32_t m_worker_num_;
  std::thread **m_worker_threads_;
  std::thread *main_worker_thread_;
  //std::thread *async_cq_thread;
  //std::condition_variable cv;
  std::mutex mtx;
};

}  // namespace kv
