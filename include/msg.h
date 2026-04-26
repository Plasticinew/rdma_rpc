#pragma once

#include <assert.h>
#include <stdint.h>
#include <chrono>

#define MAX_BATCH_SIZE 512

namespace kv {

#define NOTIFY_WORK 0xFF
#define NOTIFY_IDLE 0x00
#define MAX_MSG_SIZE 4124
#define MAX_SERVER_WORKER 32
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US 10000000  // 10s
#define MAX_REMOTE_SIZE (1UL << 25)

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END)                                      \
  (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)) \
       .count())

enum MsgType { MSG_REGISTER, MSG_UNREGISTER, MSG_ALLOCATEPAGE, MSG_FREEPAGE, MSG_ALLOCATEPAGEBATCH, MSG_FREEPAGEBATCH, MSG_GETGLOBALRKEY, MSG_ALLOCATEBLOCK, MSG_FREEBLOCK};

enum ResStatus { RES_OK, RES_FAIL };

enum ConnTYPE {CONN_ONESIDE, CONN_RPC};

#define CHECK_RDMA_MSG_SIZE(T) \
  static_assert(sizeof(T) < MAX_MSG_SIZE, #T " msg size is too big!")

struct PData {
  uint64_t buf_addr;
  uint32_t buf_rkey;
  uint32_t size;
};

struct alignas(64) MemoryNodeStatus {
  // Exposed as a tiny MR so compute nodes can poll memory pressure via one-sided RDMA reads.
  uint64_t free_bytes;
  uint64_t total_bytes;
  uint64_t page_size;
  uint64_t reserved;
};

struct CmdMsgBlock {
  uint8_t rsvd1[MAX_MSG_SIZE - 1];
  volatile uint8_t notify;
};

struct CmdMsgRespBlock {
  uint8_t rsvd1[MAX_MSG_SIZE - 1];
  volatile uint8_t notify;
};

class RequestsMsg {
 public:
  uint64_t resp_addr;
  uint32_t resp_rkey;
  uint8_t type;
};
CHECK_RDMA_MSG_SIZE(RequestsMsg);

class ResponseMsg {
 public:
  uint8_t status;
};
CHECK_RDMA_MSG_SIZE(ResponseMsg);

class RegisterRequest : public RequestsMsg {
 public:
  uint64_t size;
};
CHECK_RDMA_MSG_SIZE(RegisterRequest);


class RegisterResponse : public ResponseMsg {
 public:
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(RegisterResponse);

class AllocatePageRequest : public RequestsMsg {
 public:
  uint64_t size;
};
CHECK_RDMA_MSG_SIZE(AllocatePageRequest);

class AllocatePageResponse : public ResponseMsg {
 public:
  uint64_t addr;
};
CHECK_RDMA_MSG_SIZE(AllocatePageResponse);

class AllocateBlockRequest : public RequestsMsg {
 public:
  uint64_t size;
};
CHECK_RDMA_MSG_SIZE(AllocatePageRequest);

class AllocateBlockResponse : public ResponseMsg {
 public:
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(AllocatePageResponse);

class AllocatePageBatchRequest : public RequestsMsg {
 public:
  uint64_t num_to_allocate;
};
CHECK_RDMA_MSG_SIZE(AllocatePageBatchRequest);

class AllocatePageBatchResponse : public ResponseMsg {
 public:
  uint64_t addrs[MAX_BATCH_SIZE];
};
CHECK_RDMA_MSG_SIZE(AllocatePageBatchResponse);

class FreePageRequest : public RequestsMsg {
 public:
  uint64_t addr;
};
CHECK_RDMA_MSG_SIZE(FreePageRequest);

class FreePageResponse : public ResponseMsg {
 public:
};
CHECK_RDMA_MSG_SIZE(FreePageResponse);

class FreePageBatchRequest : public RequestsMsg {
 public:
  uint64_t num_to_free;
  uint64_t addrs[MAX_BATCH_SIZE];
};
CHECK_RDMA_MSG_SIZE(FreePageBatchRequest);

class FreePageBatchResponse : public ResponseMsg {
 public:
};
CHECK_RDMA_MSG_SIZE(FreePageBatchResponse);

class GetGlobalRKeyRequest : public RequestsMsg {
 public:
};
CHECK_RDMA_MSG_SIZE(GetGlobalRKeyRequest);

class GetGlobalRKeyResponse : public ResponseMsg {
 public:
 uint32_t global_rkey;
 uint64_t memory_status_addr;
 uint32_t memory_status_rkey;
};
CHECK_RDMA_MSG_SIZE(GetGlobalRKeyResponse);

struct UnregisterRequest : public RequestsMsg {
 public:
  uint64_t addr;
};
CHECK_RDMA_MSG_SIZE(UnregisterRequest);

struct UnregisterResponse : public ResponseMsg {};
CHECK_RDMA_MSG_SIZE(UnregisterResponse);

struct ConnMesg {
    uint8_t access_type;
};

}  // namespace kv
