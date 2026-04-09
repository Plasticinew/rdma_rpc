#include "kv_engine.h"
#include <unistd.h> 
#include <sys/mman.h>

#define MEM_ALIGN_SIZE 4096
#define CORE_ID 31

const uint64_t TOTAL_PAGES =  8ULL * 1024 * 1024;
const uint64_t REMOTE_MEM_SIZE =  TOTAL_PAGES << PAGE_SHIFT;


namespace kv {

void set_thread_affinity(std::thread* t, int core_id) {
    // 获取线程的原始句柄
    pthread_t native_handle = t->native_handle();

    // 定义CPU亲和性集
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);           // 清空亲和性集
    CPU_SET(core_id, &cpuset);   // 将指定核心添加到亲和性集

    // 设置线程的亲和性
    int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
    if (result != 0) {
        std::cerr << "Error setting thread affinity: " << result << std::endl;
    } else {
        std::cout << "Success setting thread affinity." << std::endl;
    }
    
}

/**
 * @description: start remote engine service
 * @param {string} addr   empty string for RemoteEngine as server
 * @param {string} port   the port the server listened
 * @return {bool} true for success
 */
bool RemoteEngine::start( const std::string addr, const std::string port) {
  m_stop_ = false;

  const std::string device = "mlx5_2";

  m_worker_info_ = new WorkerInfo *[MAX_SERVER_WORKER];
  m_worker_threads_ = new std::thread *[MAX_SERVER_WORKER];
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    m_worker_info_[i] = nullptr;
    m_worker_threads_[i] = nullptr;
  }
  m_worker_num_ = 0;

  struct ibv_context **ibv_ctxs;
  int nr_devices_;
  ibv_ctxs = rdma_get_devices(&nr_devices_);
  if (!ibv_ctxs) {
    perror("get device list fail");
    return false;
  }

  for(int i = 0; i< nr_devices_; i++) {
    if(device.compare(ibv_ctxs[i]->device->name) == 0){
      m_context_ = ibv_ctxs[i];
      break;
    }
  }

  //m_context_ = ibv_ctxs[0];
  m_pd_ = ibv_alloc_pd(m_context_);
  if (!m_pd_) {
    perror("ibv_alloc_pd fail");
    return false;
  }

  m_cm_channel_ = rdma_create_event_channel();
  if (!m_cm_channel_) {
    perror("rdma_create_event_channel fail");
    return false;
  }

  if (rdma_create_id(m_cm_channel_, &m_listen_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return false;
  }

  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_port = htons(stoi(port));
  sin.sin_addr.s_addr = INADDR_ANY;

  if (rdma_bind_addr(m_listen_id_, (struct sockaddr *)&sin)) {
    perror("rdma_bind_addr fail");
    return false;
  }

  if (rdma_listen(m_listen_id_, 2048)) {
    perror("rdma_listen fail");
    return false;
  }

  main_worker_thread_ = new std::thread(&RemoteEngine::main_worker, this);
  set_thread_affinity(main_worker_thread_, CORE_ID);

  m_conn_handler_ = new std::thread(&RemoteEngine::handle_connection, this);
  

  //m_conn_handler_->join();
  /*
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    if (m_worker_threads_[i] != nullptr) {
      m_worker_threads_[i]->join();
    }
  }*/
  
  //main_worker_thread_->join();


  base_addr = mmap((void*)REMOTE_MEM_SIZE, REMOTE_MEM_SIZE, PROT_READ | PROT_WRITE, 
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
  if(base_addr == MAP_FAILED) {
    perror("mmap failed.");
    return -1;
  }

  std::cout << "successfully malloc " << REMOTE_MEM_SIZE << " bytes memory block at " << base_addr << std::endl;
  //base_addr = (void*)((((uint64_t)base_addr +(1 << PAGE_SHIFT))  >> PAGE_SHIFT) << PAGE_SHIFT);

  global_mr = ibv_reg_mr(m_pd_, base_addr, (TOTAL_PAGES << PAGE_SHIFT),
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE);
  if(!global_mr) {
    perror("global memory region register fail.");
    return false;
  }

  std::cout << "successfully register " << (TOTAL_PAGES << PAGE_SHIFT) << " bytes MR at " << base_addr << std::endl;

  this->page_queue = new PageQueue(TOTAL_PAGES, (uint64_t)base_addr);
  uint64_t addr_tmp;
  for(int i = 0;i < 10; ++i) {
    this->page_queue->allocate(addr_tmp);
  }
  if(this->page_queue == nullptr) {
    perror("page queue init fail.");
    return false;
  } else {
    std::cout << "page queue init success" << std::endl;
  }
  /*
  base_addr = mmap((void*)0x1000000000, REMOTE_MEM_SIZE, PROT_READ | PROT_WRITE, 
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
  if(base_addr == MAP_FAILED) {
    perror("mmap failed.");
    return -1;
  }

  block_queue = new BlockQueue(REMOTE_MEM_SIZE/BLOCK_SIZE);

  for(auto p = base_addr;p < base_addr + REMOTE_MEM_SIZE; p += BLOCK_SIZE) {
    memset(p, 777777, 4);
    auto mr = ibv_reg_mr(m_pd_, p, BLOCK_SIZE,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE);
    block_queue->free((uint64_t)p, mr->rkey);
  }

  std::cout << "successfully register " << REMOTE_MEM_SIZE << " bytes MR at " << base_addr << std::endl;
  std::cout << "blocks queue length is " << block_queue->block_num << std::endl;*/

  return true;
}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool RemoteEngine::alive() {  // TODO
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void RemoteEngine::stop() {
  m_stop_ = true;
  if (m_conn_handler_ != nullptr) {
    m_conn_handler_->join();
    delete m_conn_handler_;
    m_conn_handler_ = nullptr;
  }
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    if (m_worker_threads_[i] != nullptr) {
      m_worker_threads_[i]->join();
      delete m_worker_threads_[i];
      m_worker_threads_[i] = nullptr;
    }
  }
  // TODO: release resources
}

void RemoteEngine::handle_connection() {
  printf("start handle_connection\n");
  struct rdma_cm_event *event;
  while (true) {
    if (m_stop_) break;
    if (rdma_get_cm_event(m_cm_channel_, &event)) {
      perror("rdma_get_cm_event fail");
      return;
    }

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
      struct rdma_cm_id *cm_id = event->id;
      rdma_ack_cm_event(event);
      ConnMesg msg = *(ConnMesg*)event->param.conn.private_data;
      create_connection(cm_id, msg.access_type);
    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(event);
    } else {
      rdma_ack_cm_event(event);
    }
  }
  printf("exit handle_connection\n");
}

int RemoteEngine::create_connection(struct rdma_cm_id *cm_id, uint8_t connect_type) {
  if (!m_pd_) {
    perror("ibv_pibv_alloc_pdoll_cq fail");
    return -1;
  }

  struct ibv_comp_channel *comp_chan = ibv_create_comp_channel(m_context_);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  struct ibv_cq *cq = ibv_create_cq(m_context_, 1, NULL, comp_chan, 0);
  if (!cq) {
    perror("ibv_create_cq fail");
    return -1;
  }

  if (ibv_req_notify_cq(cq, 0)) {
    perror("ibv_req_notify_cq fail");
    return -1;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = 4096;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = 5120;
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.cap.max_inline_data = 256;
  qp_attr.sq_sig_all = 0;
  qp_attr.send_cq = cq;
  qp_attr.recv_cq = cq;
  qp_attr.qp_type = IBV_QPT_RC;

  if (rdma_create_qp(cm_id, m_pd_, &qp_attr)) {
    perror("rdma_create_qp fail:RemoteEngine::create_connection");
    return -1;
  }

  struct PData rep_pdata;
  CmdMsgBlock *cmd_msg = nullptr;
  CmdMsgRespBlock *cmd_resp = nullptr;
  struct ibv_mr *msg_mr = nullptr;
  struct ibv_mr *resp_mr = nullptr;
  cmd_msg = new CmdMsgBlock();
  memset(cmd_msg, 0, sizeof(CmdMsgBlock));
  msg_mr = rdma_register_memory((void *)cmd_msg, sizeof(CmdMsgBlock));
  if (!msg_mr) {
    perror("ibv_reg_mr cmd_msg fail");
    return -1;
  }

  cmd_resp = new CmdMsgRespBlock();
  memset(cmd_resp, 0, sizeof(CmdMsgRespBlock));
  resp_mr = rdma_register_memory((void *)cmd_resp, sizeof(CmdMsgRespBlock));
  if (!msg_mr) {
    perror("ibv_reg_mr cmd_resp fail");
    return -1;
  }

  rep_pdata.buf_addr = (uintptr_t)cmd_msg;
  rep_pdata.buf_rkey = msg_mr->rkey;
  rep_pdata.size = sizeof(CmdMsgRespBlock);

  if(connect_type == CONN_RPC) {
    int num = m_worker_num_++;
    if (m_worker_num_ <= MAX_SERVER_WORKER) {
      assert(m_worker_info_[num] == nullptr);
      m_worker_info_[num] = new WorkerInfo();
      m_worker_info_[num]->cmd_msg = cmd_msg;
      m_worker_info_[num]->cmd_resp_msg = cmd_resp;
      m_worker_info_[num]->msg_mr = msg_mr;
      m_worker_info_[num]->resp_mr = resp_mr;
      m_worker_info_[num]->cm_id = cm_id;
      m_worker_info_[num]->cq = cq;
      m_worker_info_[num]->comp_channel = comp_chan;
      assert(m_worker_threads_[num] == nullptr);
      active_workers.insert(m_worker_info_[num]);
      std::cout << "add a new worker: " << num << std::endl;
    }
  }
  

  //m_worker_threads_[0] =
        //new std::thread(&RemoteEngine::worker, this, m_worker_info_[num], num);

  struct rdma_conn_param conn_param = {};
  conn_param.responder_resources = 16;
  conn_param.initiator_depth = 16;
  conn_param.private_data = &rep_pdata;
  conn_param.retry_count = 7;
  conn_param.rnr_retry_count = 7;
  conn_param.private_data_len = sizeof(rep_pdata);

  // printf("connection created, private data: %ld, addr: %ld, key: %d\n",
  //        *((uint64_t *)rep_pdata.buf_addr), rep_pdata.buf_addr,
  //        rep_pdata.buf_rkey);

  if (rdma_accept(cm_id, &conn_param)) {
    perror("rdma_accept fail");
    return -1;
  }

  return 0;
}

struct ibv_mr *RemoteEngine::rdma_register_memory(void *ptr, uint64_t size) {
  struct ibv_mr *mr =
      ibv_reg_mr(m_pd_, ptr, size,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return nullptr;
  }
  return mr;
}

int RemoteEngine::allocate_page(uint64_t &addr) {
  int ret;
  //page_queue->mtx.lock();
  ret = this->page_queue->allocate(addr);
  //page_queue->mtx.unlock();
  return ret;
}

int RemoteEngine::allocate_block(uint64_t &addr, uint32_t &rkey) {
  int ret;
  ret = this->block_queue->allocate(addr, rkey);
  return ret;
}

int RemoteEngine::allocate_page_batch(uint64_t* addrs, int num) {
  assert(num > 0 && num <= MAX_BATCH_SIZE);
  int ret;
  //page_queue->mtx.lock();
  ret = this->page_queue->allocate_batch(addrs, num);
  //page_queue->mtx.unlock();
  return ret;
}

int RemoteEngine::allocate_page_regmr(uint64_t &addr) {
  auto ptr = malloc(4096);
  auto mr = ibv_reg_mr(m_pd_, ptr, 4096,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE);
  if(!mr)
    return -1;
  addr = uint64_t(ptr);
  mrmap_mtx.lock();
  if(mrmap.find(addr) != mrmap.end())
    return -1;
  mrmap.insert({addr, mr});
  mrmap_mtx.unlock();
  
  return 0;
}

int RemoteEngine::allocate_page_malloc(uint64_t &addr) {
  auto ptr = malloc(4096);
  if(!ptr)
    return -1;
  addr = uint64_t(ptr);
  return 0;
}

int RemoteEngine::free_page_malloc(uint64_t addr) {
  free((void*)addr);
  return 0;
}

int RemoteEngine::free_page(uint64_t addr) {
  int ret;
  //page_queue->mtx.lock();
  ret = this->page_queue->free(addr);
  //page_queue->mtx.unlock();
  return ret;
}

int RemoteEngine::free_page_batch(uint64_t* addrs, int num) {
  assert(num > 0 && num <= MAX_BATCH_SIZE);
  int ret;
  //page_queue->mtx.lock();
  ret = this->page_queue->free_batch(addrs, num);
  //page_queue->mtx.unlock();
  return ret;
}

int RemoteEngine::free_page_deregmr(uint64_t addr) {
  int ret;
  mrmap_mtx.lock();
  auto pair = mrmap.find(addr);
  if(pair == mrmap.end())
    return -1;
  ret = ibv_dereg_mr(pair->second);
  mrmap_mtx.unlock();
  return ret;
}

int RemoteEngine::allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                               uint64_t size) {
  /* align mem */
  uint64_t total_size = size + MEM_ALIGN_SIZE;
  uint64_t mem = (uint64_t)malloc(total_size);
  addr = mem;
  if (addr % MEM_ALIGN_SIZE != 0)
    addr = addr + (MEM_ALIGN_SIZE - addr % MEM_ALIGN_SIZE);
  struct ibv_mr *mr = rdma_register_memory((void *)addr, size);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return -1;
  }
  rkey = mr->rkey;
  // printf("allocate and register memory %ld %d\n", addr, rkey);
  // TODO: save this memory info for later delete
  return 0;
}

int RemoteEngine::remote_write(WorkerInfo *work_info, uint64_t local_addr,
                               uint32_t lkey, uint32_t length,
                               uint64_t remote_addr, uint32_t rkey) {
  struct ibv_sge sge;
  sge.addr = (uintptr_t)local_addr;
  sge.length = length;
  sge.lkey = lkey;

  struct ibv_send_wr send_wr = {};
  struct ibv_send_wr *bad_send_wr;
  send_wr.wr_id = 0;
  send_wr.num_sge = 1;
  send_wr.next = NULL;
  send_wr.opcode = IBV_WR_RDMA_WRITE;
  send_wr.sg_list = &sge;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = remote_addr;
  send_wr.wr.rdma.rkey = rkey; 
  int ret = 0;
  //work_info->cq_mutex.lock();
  ret = ibv_post_send(work_info->cm_id->qp, &send_wr, &bad_send_wr);

  if(ret != 0) {
    int err = errno;
    std::cerr << "ibv_post_send fail, errno: " << ret << std::endl;
    //work_info->cq_mutex.unlock();
    return ret;
  }

  //work_info->cq_mutex.unlock();
  return ret;

  // printf("remote write %ld %d\n", remote_addr, rkey);
 /*
  auto start = TIME_NOW;
  struct ibv_wc wc;
  int ret = -1;
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      perror("remote write timeout");
      return -1;
    }
    int rc = ibv_poll_cq(work_info->cq, 1, &wc);
    if (rc > 0) {
      if (IBV_WC_SUCCESS == wc.status) {
        ret = 0;
        break;
      } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
        perror("cmd_send IBV_WC_WR_FLUSH_ERR");
        break;
      } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
        perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
        break;
      } else {
        perror("cmd_send ibv_poll_cq status error");
        break;
      }
    } else if (0 == rc) {
      continue;
    } else {
      perror("ibv_poll_cq fail");
      break;
    }
  }
  return ret;*/
}

void RemoteEngine::main_worker() {
  printf("start main worker on core: %d\n", CORE_ID);
  while(true) {
    for(auto worker_info : active_workers) { // poll all active workers
      worker(worker_info, 1);
    }
  }
}

void RemoteEngine::worker_handel_cq(ibv_cq * cq) {
  struct ibv_wc wc;
  //work_info->cq_mutex.lock();
  int rc = ibv_poll_cq(cq, 1, &wc);
  if (rc > 0) {
    if (IBV_WC_SUCCESS == wc.status) {
      //
    } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
      perror("cmd_send IBV_WC_WR_FLUSH_ERR");  
    } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
      perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR"); 
    } else {
      perror("cmd_send ibv_poll_cq status error"); 
    }
  } else if (0 == rc) {
    //
  } else {
    perror("ibv_poll_cq fail");
  }
  return;
}

void RemoteEngine::worker(WorkerInfo *work_info, uint32_t num) {
  
  CmdMsgBlock *cmd_msg = work_info->cmd_msg;
  CmdMsgRespBlock *cmd_resp = work_info->cmd_resp_msg;
  struct ibv_mr *resp_mr = work_info->resp_mr;
  cmd_resp->notify = NOTIFY_WORK;

  worker_handel_cq(work_info->cq);

  if (m_stop_) return;
  if (cmd_msg->notify == NOTIFY_IDLE) return;
  cmd_msg->notify = NOTIFY_IDLE;
  RequestsMsg *request = (RequestsMsg *)cmd_msg;
    
  switch (request->type) {
  case MSG_ALLOCATEPAGE: {
    //auto start = std::chrono::high_resolution_clock::now();
    AllocatePageRequest* alloc_page_req = (AllocatePageRequest *)request;
    AllocatePageResponse* resp_msg = (AllocatePageResponse *)cmd_resp;

    if (allocate_page(resp_msg->addr)) {
      resp_msg->status = RES_FAIL;
    } else {
      resp_msg->status = RES_OK;
    }

    remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                 sizeof(CmdMsgRespBlock), alloc_page_req->resp_addr,
                 alloc_page_req->resp_rkey);

    auto end = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double, std::micro> duration = end - start;
    //if (duration.count() > 2)
      //std::cout << "allocate latency is " << duration.count() << " us" << std::endl;

    break;
  }

  case MSG_ALLOCATEBLOCK: {
    auto start = std::chrono::high_resolution_clock::now();
    AllocateBlockRequest* alloc_page_req = (AllocateBlockRequest *)request;
    AllocateBlockResponse* resp_msg = (AllocateBlockResponse *)cmd_resp;

    if (allocate_block(resp_msg->addr, resp_msg->rkey)) {
      resp_msg->status = RES_FAIL;
    } else {
      resp_msg->status = RES_OK;
    }

    remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                 sizeof(CmdMsgRespBlock), alloc_page_req->resp_addr,
                 alloc_page_req->resp_rkey);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> duration = end - start;
    if (duration.count() > 0)
      std::cout << "allocate block latency is " << duration.count() << " us" << std::endl;

    break;
  }

  case MSG_FREEPAGE: {
    //auto start = std::chrono::high_resolution_clock::now();
    FreePageRequest* free_page_req = (FreePageRequest *)request;
    FreePageResponse* resp_msg = (FreePageResponse *)cmd_resp;

    if (free_page(free_page_req->addr)) {
      resp_msg->status = RES_FAIL;
    } else {
      resp_msg->status = RES_OK;
    }

    remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                 sizeof(CmdMsgRespBlock), free_page_req->resp_addr,
                 free_page_req->resp_rkey);

    //auto end = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double, std::micro> duration = end - start;
    //if (duration.count() > 2)
    //  std::cout << "free latency is " << duration.count() << " us" << std::endl;

    break;
  }

  case MSG_ALLOCATEPAGEBATCH: {
    //auto start = std::chrono::high_resolution_clock::now();
    AllocatePageBatchRequest* allocate_page_batch_req = (AllocatePageBatchRequest *)request;
    AllocatePageBatchResponse* resp_msg = (AllocatePageBatchResponse *)cmd_resp;

    if (allocate_page_batch(resp_msg->addrs, allocate_page_batch_req->num_to_allocate)) {
      resp_msg->status = RES_FAIL;
    } else {
      resp_msg->status = RES_OK;
    }

    remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                 sizeof(CmdMsgRespBlock), allocate_page_batch_req->resp_addr,
                 allocate_page_batch_req->resp_rkey);

    //auto end = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double, std::micro> duration = end - start;
    //if (duration.count() > 5)
    //  std::cout << "batch allocate latency is " << duration.count() << " us" << std::endl;

    break;
  }

  case MSG_FREEPAGEBATCH: {
    //auto start = std::chrono::high_resolution_clock::now();
    FreePageBatchRequest* free_page_batch_req = (FreePageBatchRequest *)request;
    FreePageBatchResponse* resp_msg = (FreePageBatchResponse *)cmd_resp;

    if (free_page_batch(free_page_batch_req->addrs, free_page_batch_req->num_to_free)) {
      resp_msg->status = RES_FAIL;
    } else {
      resp_msg->status = RES_OK;
    }

    remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                 sizeof(CmdMsgRespBlock), free_page_batch_req->resp_addr,
                 free_page_batch_req->resp_rkey);

    //auto end = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double, std::micro> duration = end - start;
    //if (duration.count() > 5)
    //  std::cout << "batch free latency is " << duration.count() << " us" << std::endl;

    break;
  }

  case MSG_GETGLOBALRKEY: {
    GetGlobalRKeyRequest *reg_req = (GetGlobalRKeyRequest *)request;
    GetGlobalRKeyResponse *resp_msg = (GetGlobalRKeyResponse *)cmd_resp;

    assert(global_mr != nullptr);
    resp_msg->global_rkey = global_mr->rkey;

    remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                 sizeof(CmdMsgRespBlock), reg_req->resp_addr,
                 reg_req->resp_rkey);

    break;
  }

  case MSG_REGISTER: {
    RegisterRequest *reg_req = (RegisterRequest *)request;
    RegisterResponse *resp_msg = (RegisterResponse *)cmd_resp;

    if (allocate_and_register_memory(resp_msg->addr, resp_msg->rkey, reg_req->size)) {
      resp_msg->status = RES_FAIL;
    } else {
      resp_msg->status = RES_OK;
    }

    remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                 sizeof(CmdMsgRespBlock), reg_req->resp_addr,
                 reg_req->resp_rkey);

    break;
  }

  case MSG_UNREGISTER: {
    UnregisterRequest *unreg_req = (UnregisterRequest *)request;
    printf("receive a memory unregister message, addr: %ld\n", unreg_req->addr);

    // TODO: Implement memory unregister logic here.

    break;
  }

  default:
    printf("wrong request type\n");
    break;
}


}

}  // namespace kv
