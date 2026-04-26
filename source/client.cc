#include <string>
#include <iostream>
#include <chrono>
#include <vector>
#include <algorithm>

#include "kv_engine.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
#include "two_level_queues.h"

static uint64_t mnode_num = 1;

void reverse(char str[], int length) {
   
    int start = 0;
    int end = length - 1;
    while (start < end) {
   
        char temp = str[start];
        str[start] = str[end];
        str[end] = temp;
        start++;
        end--;
    }
}

char* itoa(int num, char *str, int base) {
   
    int i = 0;
    int isNegative = 0;

    if (num == 0) {
        str[i++] = '0';
        str[i] = '\0';
        return str;
    }

    if (num < 0 && base == 10) {
        isNegative = 1;
        num = -num;
    }

    while (num != 0) {
        int rem = num % base;
        str[i++] = (rem > 9) ? (rem - 10) + 'a' : rem + '0';
        num = num / base;
    }

    if (isNegative) {
        str[i++] = '-';
    }

    str[i] = '\0';
    reverse(str, i);
    return str;
}

int atoi(const char* str) 
{
	assert(str);
	while (isspace(*str)) 
	{
		str++;
	}
	int is_positive = 1;
	if (*str - '+' == 0 ) 
	{
		is_positive = 1;
		str++;
	}
	if (*str - '-' == 0)
	{
		is_positive = 0;
		str++;
	}
	long long result = 0;
  while (isdigit(*str)) 
	{
		result *= 10;
		result += *str - '0'; 
		if (result > INT_MAX || result < INT_MIN) 
		{
			return result > INT_MAX ? INT_MAX : INT_MIN; 
		}

		str++;
	}
	return is_positive ? (result) : -result;
}

int ipstr_to_u8array(char* ip_str, int str_len, int* array){
    char tmp[4] = { 0 };
    int tmp_count = 0;
    int array_count = 0;
    char *ip_head = ip_str;
    char *ip_end = &ip_str[str_len];
    do{
        if(*ip_head == '.' || ip_head == ip_end){
            tmp_count = 0;
            array[array_count] = atoi(tmp);
            memset(tmp, 0, 4);
            array_count++;
        }
        else{
            tmp[tmp_count] = *ip_head;
            tmp_count++;
        }
        ip_head++;
    }while(ip_head <= ip_end);
    return 0;
}

int u8array_to_ipstr(int* array, char* ip_str){
    char temp[4] = {0};
    itoa(array[0], temp, 10);
    strcat(ip_str, temp);
    strcat(ip_str, ".");
    itoa(array[1], temp, 10);
    strcat(ip_str, temp);
    strcat(ip_str, ".");
    itoa(array[2], temp, 10);
    strcat(ip_str, temp);
    strcat(ip_str, ".");
    itoa(array[3], temp, 10);
    strcat(ip_str, temp);
    return 0;
}

int get_remote_global_rkey(kv::LocalEngine *kv_imp) {
  uint32_t rkey[mnode_num];
    for(int i = 0; i < mnode_num; i++){
        uint64_t memory_status_addr = 0;
        uint32_t memory_status_rkey = 0;
        if (kv_imp[i].get_global_rkey(rkey[i], memory_status_addr,
                                      memory_status_rkey)) {
            perror("get global rkey fail.\n");
            return -1;
        } else {
            std::cout << "global rkey of mnode " << i << " is " << rkey[i] << std::endl;
            std::cout << "memory status MR of mnode " << i << " addr "
                      << memory_status_addr << " rkey "
                      << memory_status_rkey << std::endl;
        }
    }

  assert(queues_allocator != nullptr);
  for(uint32_t i = 0;i < NUM_ONLINE_CPUS; ++i) {
    auto queue_allocator = &queues_allocator->queues[i];
    for(uint32_t j = 0; j < mnode_num; ++j) {
      queue_allocator->rkey[j].store(rkey[j]);
    }
  }
  std::cout << "published global rkeys for " << mnode_num
            << " memory nodes to allocator queues" << std::endl;
  return 0;
}




int fill_allocate_page_queue(local_pool* pool, const std::vector<int>& online_cpus) {
  uint64_t remote_addr;
  uint64_t count = 0;
    for(auto id : online_cpus) {
      while(get_length_allocator(id) < ALLOCATE_BUFFER_SIZE - 1) {
          remote_addr = pool->allocate_one_page();
          assert(remote_addr != 0);
          assert((remote_addr & ((1 << PAGE_SHIFT) - 1)) == 0);
          push_queue_allocator(remote_addr, id);
          count++;
          if(count == 1) {
            std::cout << "remote addr is " << remote_addr << std::endl;
          }
        }
      } 
  return 0;
}



void allocation_thread(kv::LocalEngine *kv_imp,  const std::vector<int>& online_cpus) {
  uint64_t count = 0;
  std::vector<double> res;
  int ret;
  local_pool* pool = new local_pool(kv_imp, mnode_num);

  if (!online_cpus.empty()) {
    std::cout << "allocation thread online for cpu range ["
              << online_cpus.front() << ", " << online_cpus.back()
              << "], mnode_num=" << mnode_num << std::endl;
  }

  fill_allocate_page_queue(pool, online_cpus);

  while(true) {
    count++;
    for(auto id : online_cpus) {
      uint64_t cur_queue_allocator_len = get_length_allocator(id);
      uint64_t cur_queue_deallocator_len = get_length_deallocator(id);

      if(cur_queue_allocator_len < ALLOCATE_BUFFER_SIZE - 1) {
        push_queue_allocator(pool->allocate_one_page(), id);
      }

      if(cur_queue_deallocator_len > 0) {
        pool->deallocate_one_page(pop_queue_deallocator(id));
      }
    }
    
    /*
    if(count % 500000000 == 0) {
      for(auto id : online_cpus) {
        auto queue_allocator = &queues_allocator->queues[id];
        //std::cout << "count: " << count << std::endl;
        //printf("allocator queue: len = %d, begin = %d, end = %d, first = %ld\n", (int)get_length_allocator(id), (int)queue_allocator->begin, (int)queue_allocator->end, (unsigned long)queue_allocator->begin);
        //std::cout << "allocator queue: len = " << get_length_allocator() << ", begin = " << queue_allocator->begin << ""<< std::endl;
        //std::cout << "deallocator queue len:" << get_length_deallocator(id) << std::endl;
      }
    }*/
  }
  
  kv_imp->stop();
  delete kv_imp;
}

int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  mnode_num = atoi(argv[3]);
  //const uint64_t interval = atoi(argv[3]);

  page_queue_shm_init();

  std::vector<std::vector<int>> online_cpus;
  int start = 0;  
  while (start < NUM_ONLINE_CPUS) {
      int end = std::min(start + NUM_CPUS_PER_THREAD, NUM_ONLINE_CPUS);
      std::vector<int> currentVec;
      for (int i = start; i < end; ++i) {
          currentVec.push_back(i);
      }
      online_cpus.push_back(currentVec);
      start = end;  
  }

  std::vector<std::thread *> adaptive_scaler;
  for(auto v : online_cpus) {
    char ip_temp[16] = {0};
    int ip_array[4] = {0};
    ipstr_to_u8array((char*)rdma_addr.c_str(), rdma_addr.length(), ip_array);
    kv::LocalEngine *kv_imp = new kv::LocalEngine[MEM_NODE_NUM];
      for(int i = 0; i < mnode_num; i++) {
        // (kv_imp + i) = new kv::LocalEngine();
        memset(ip_temp, 0, 16);
        u8array_to_ipstr(ip_array, ip_temp);
        // ipstr_to_u8array(ip_temp, strlen(ip_temp), ip_array);
        assert(kv_imp);
        std::cout << "connecting allocator client to mnode " << i
                  << " at " << ip_temp << ":" << rdma_port << std::endl;
        kv_imp[i].start(ip_temp, rdma_port);
        kv_imp[i].set_mnode(i);
        ip_array[3] += 1;
      }
      get_remote_global_rkey(kv_imp);
      auto t = new std::thread(&allocation_thread, kv_imp, v);
      adaptive_scaler.push_back(t);
  }

  for(auto t : adaptive_scaler) {
    t->join();
  }

  return 0;
}
