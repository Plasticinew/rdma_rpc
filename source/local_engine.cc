#include "assert.h"
#include "atomic"
#include "kv_engine.h"

namespace kv {

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
  m_rdma_conn_ = new ConnectionManager();
  if (m_rdma_conn_ == nullptr) return -1;
  if (m_rdma_conn_->init(addr, port, 1, 1)) return false;
  m_rdma_mem_pool_ = new RDMAMemPool(m_rdma_conn_);
  if (m_rdma_mem_pool_) return true;
  return false;
}

bool LocalEngine::set_mnode(uint16_t mnode) {
  m_mnode_ = mnode;
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void LocalEngine::stop(){
    // TODO
};

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool LocalEngine::alive() { return true; }

/**
 * @description: put a key-value pair to engine
 * @param {string} key
 * @param {string} value
 * @return {bool} true for success
 */
bool LocalEngine::write(const std::string key, const std::string value) {
  assert(m_rdma_conn_ != nullptr);
  internal_value_t internal_value;
  uint64_t remote_addr;
  uint32_t rkey;

  int index = std::hash<std::string>()(key) & (SHARDING_NUM - 1);

  m_mutex_[index].lock();
  bool found = false;
  auto it = m_map_[index].find(key);
  if (it != m_map_[index].end()) {
    /* Reuse the old addr. In this case, the new value size should be the same
     * with old one. TODO: Solve the situation that the new value size is larger
     * than the old  one */
    assert(it->second.size >= value.size());
    remote_addr = it->second.remote_addr;
    rkey = it->second.rkey;
    found = true;
  } else {
    if (m_rdma_mem_pool_->get_mem(value.size(), remote_addr, rkey)) {
      m_mutex_[index].unlock();
      return false;
    }
    // printf("get mem %lld %d\n", remote_addr, rkey);
  }
  m_mutex_[index].unlock();

  /* Optimization: local node could cache some hot data. No need to write the
   * data to the remote for each insertion. Moving local data to the remote
   * could be done in the background instead of the critical path. */
  /* Also, we can batch some KV pairs together, writing them to remote in one
   * RDMA write in the background */

  int ret = m_rdma_conn_->remote_write((void *)value.c_str(), value.size(),
                                       remote_addr, rkey);
  if (ret) return false;
  // printf("write key: %s, value: %s, %lld %d\n", key.c_str(), value.c_str(),
  //        remote_addr, rkey);
  if (found) return true; /* no need to update hash map */
  internal_value.remote_addr = remote_addr;
  internal_value.rkey = rkey;
  internal_value.size = value.size();
  /* Optimization: To support concurrent insertion */
  m_mutex_[index].lock();
  m_map_[index][key] = internal_value;
  m_mutex_[index].unlock();
  return true;
}

bool LocalEngine::write_block(uint64_t laddr, uint64_t raddr, uint64_t len, uint32_t rkey) {
  int ret = m_rdma_conn_->remote_write((void *)laddr, len,
                                       raddr, rkey);
  return ret == 0;
}

bool LocalEngine::read_block(uint64_t laddr, uint64_t raddr, uint64_t len, uint32_t rkey) {
  int ret = m_rdma_conn_->remote_read((void *)laddr, len,
                                       raddr, rkey);
  return ret == 0;
}

int LocalEngine::read_remote_buffer(void *buf, uint64_t len, uint64_t remote_addr,
                                    uint32_t rkey) {
  return m_rdma_conn_->remote_read(buf, len, remote_addr, rkey);
}

int LocalEngine::read_memory_node_status(MemoryNodeStatus& status) {
  if (m_memory_status_addr_ == 0 || m_memory_status_rkey_ == 0) {
    return -1;
  }
  // Each LocalEngine instance caches the status MR metadata of its paired memory node.
  return read_remote_buffer(&status, sizeof(status), m_memory_status_addr_,
                            m_memory_status_rkey_);
}

int LocalEngine::allocate_remote_page(uint64_t& addr) {
  int ret = m_rdma_conn_->allocate_remote_page(addr);
  return ret;
}

int LocalEngine::allocate_remote_block(uint64_t& addr, uint32_t& rkey) {
  int ret = m_rdma_conn_->allocate_remote_block(addr, rkey);
  return ret;
}

int LocalEngine::get_global_rkey(uint32_t& global_rkey,
                                 uint64_t& memory_status_addr,
                                 uint32_t& memory_status_rkey) {
  int ret = m_rdma_conn_->get_global_rkey(global_rkey, memory_status_addr,
                                          memory_status_rkey);
  if (ret == 0) {
    m_memory_status_addr_ = memory_status_addr;
    m_memory_status_rkey_ = memory_status_rkey;
  }
  return ret;
}

int LocalEngine::allocate_remote_page_batch(uint64_t* addrs, int num) {
  assert(num <= MAX_BATCH_SIZE && num > 0);
  int ret = m_rdma_conn_->allocate_remote_page_batch(addrs, num);
  return ret;
}

int LocalEngine::free_remote_page(uint64_t addr) {
  int ret = m_rdma_conn_->free_remote_page(addr);
  return ret;
}

int LocalEngine::free_remote_page_batch(uint64_t* addrs, int num) {
  assert(num <= MAX_BATCH_SIZE && num > 0);
  int ret = m_rdma_conn_->free_remote_page_batch(addrs, num);
  return ret;
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string key, std::string &value) {
  int index = std::hash<std::string>()(key) & (SHARDING_NUM - 1);
  internal_value_t inter_val;
  m_mutex_[index].lock();
  auto it = m_map_[index].find(key);
  if (it == m_map_[index].end()) {
    m_mutex_[index].unlock();
    return false;
  }
  inter_val = it->second;
  m_mutex_[index].unlock();
  value.resize(inter_val.size, '0');
  if (m_rdma_conn_->remote_read((void *)value.c_str(), inter_val.size,
                                inter_val.remote_addr, inter_val.rkey))
    return false;
  // printf("read key: %s, value: %s, size:%d, %lld %d\n", key.c_str(),
  //        value.c_str(), value.size(), inter_val.remote_addr, inter_val.rkey);
  return true;
}

}  // namespace kv
