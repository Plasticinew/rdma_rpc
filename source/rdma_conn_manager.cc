#include "rdma_conn_manager.h"
#include "atomic"

namespace kv {

int ConnectionManager::init(const std::string ip, const std::string port,
                            uint32_t rpc_conn_num,
                            uint32_t one_sided_conn_num) {
  m_rpc_conn_queue_ = new ConnQue();
  m_one_sided_conn_queue_ = new ConnQue();
  if (rpc_conn_num > MAX_SERVER_WORKER) {
    printf(
        "max server worker is %d, rpc_conn_num is: %d, reset rpc_conn_num to "
        "%d\n",
        MAX_SERVER_WORKER, rpc_conn_num, MAX_SERVER_WORKER);
    //rpc_conn_num = MAX_SERVER_WORKER;
  }

  for (uint32_t i = 0; i < rpc_conn_num; i++) {
    RDMAConnection *conn = new RDMAConnection();
    if (conn->init(ip, port, CONN_RPC)) {
      // TODO: release resources
      return -1;
    }
    m_rpc_conn_queue_->enqueue(conn);
  }

  for (uint32_t i = 0; i < one_sided_conn_num; i++) {
    RDMAConnection *conn = new RDMAConnection();
    if (conn->init(ip, port, CONN_ONESIDE)) {
      // TODO: release resources
      return -1;
    }
    m_one_sided_conn_queue_->enqueue(conn);
  }
  return 0;
}

int ConnectionManager::allocate_remote_page(uint64_t &addr) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->rdma_allocate_remote_page(addr);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::allocate_remote_block(uint64_t &addr, uint32_t &rkey) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->rdma_allocate_remote_block(addr, rkey);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::get_global_rkey(uint32_t& global_rkey,
                                       uint64_t& memory_status_addr,
                                       uint32_t& memory_status_rkey) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->rdma_get_global_rkey(global_rkey, memory_status_addr,
                                       memory_status_rkey);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::allocate_remote_page_batch(uint64_t* addrs, int num) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->rdma_allocate_remote_page_batch(addrs, num);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::free_remote_page(uint64_t addr) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->rdma_free_remote_page(addr);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::free_remote_page_batch(uint64_t* addrs, int num) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->rdma_free_remote_page_batch(addrs, num);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::register_remote_memory(uint64_t &addr, uint32_t &rkey,
                                              uint64_t size) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->register_remote_memory(addr, rkey, size);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::remote_read(void *ptr, uint32_t size,
                                   uint64_t remote_addr, uint32_t rkey) {
  RDMAConnection *conn = m_one_sided_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_read(ptr, size, remote_addr, rkey);
  m_one_sided_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::remote_write(void *ptr, uint32_t size,
                                    uint64_t remote_addr, uint32_t rkey) {
  RDMAConnection *conn = m_one_sided_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_write(ptr, size, remote_addr, rkey);
  m_one_sided_conn_queue_->enqueue(conn);
  return ret;
}

}  // namespace kv
