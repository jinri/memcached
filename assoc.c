/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

//扩容等待条件变量，扩容线程再次条件变量上等待，
//当work线程插入item后发现哈希表需要扩展，就向该条件变量发送信号。
static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t maintenance_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t hash_items_counter_lock = PTHREAD_MUTEX_INITIALIZER;

typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

/* 哈希表大小，2的次幂可以通过hashpower设置 */
unsigned int hashpower = HASHPOWER_DEFAULT;

#define hashsize(n) ((ub4)1<<(n))  //哈希表大小
#define hashmask(n) (hashsize(n)-1)  //哈希掩码值

/* 哈希表数组指针. */
static item** primary_hashtable = 0;

/*
 * Previous hash table. During expansion, we look here for keys that haven't
 * been moved over to the primary yet.
 * 旧表，在哈希表扩容期间使用
 */
static item** old_hashtable = 0;

/* 哈希表数目 */
static unsigned int hash_items = 0;

/* Flag: Are we in the middle of expanding now? */
static bool expanding = false;  //是否在迁移中
static bool started_expanding = false;  //是否开始扩容

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 */
static unsigned int expand_bucket = 0;  //指向待迁移的桶

//初始化哈希表
void assoc_init(const int hashtable_init) {
    //如何设置了哈希表大小，用设置大小，否则使用默认参数。
    if (hashtable_init) {
        hashpower = hashtable_init;
    }
	//申请哈希表空间
    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (! primary_hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }
	//记录哈希表大小和占用字节
    STATS_LOCK();
    stats.hash_power_level = hashpower;
    stats.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    unsigned int oldbucket;

    //哈希表处理迁移数据状态，且还没有迁移到该桶。(在还没有迁移到该桶时，assoc_insert保证哈希到该区间的键插入到旧表)
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it = old_hashtable[oldbucket];
    } else {
        //找到key所在桶
        it = primary_hashtable[hv & hashmask(hashpower)];
    }

    item *ret = NULL;
    int depth = 0;
	//遍历冲突链
    while (it) {
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */

//查找item。返回前驱节点的h_next成员地址,如果查找失败那么就返回冲突链中最后  
//一个节点的h_next成员地址。因为最后一个节点的h_next的值为NULL。通过对返回值  
//使用 * 运算符即可知道有没有查找成功。  
static item** _hashitem_before (const char *key, const size_t nkey, const uint32_t hv) {
    item **pos;
    unsigned int oldbucket;

    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        pos = &old_hashtable[oldbucket];
    } else {
        pos = &primary_hashtable[hv & hashmask(hashpower)];
    }

    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* 将哈希表的容量增加到2倍 */
static void assoc_expand(void) {
    old_hashtable = primary_hashtable;

    primary_hashtable = calloc(hashsize(hashpower + 1), sizeof(void *));
    if (primary_hashtable) {
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        hashpower++;
        expanding = true;
        expand_bucket = 0;
        STATS_LOCK();
        stats.hash_power_level = hashpower;
        stats.hash_bytes += hashsize(hashpower) * sizeof(void *);
        stats.hash_is_expanding = 1;
        STATS_UNLOCK();
    } else {
        primary_hashtable = old_hashtable;
        /* Bad news, but we can keep running. */
    }
}

//唤醒扩容线程
static void assoc_start_expand(void) {
    if (started_expanding)
        return;

    started_expanding = true;
    pthread_cond_signal(&maintenance_cond);
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(item *it, const uint32_t hv) {
    unsigned int oldbucket;

//    assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */

    //哈希表处于数据迁移状态，并且还没有迁移到该桶，插入到旧表。
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it->h_next = old_hashtable[oldbucket];
        old_hashtable[oldbucket] = it;
    } else {
        //采用头插法插入到桶中
        it->h_next = primary_hashtable[hv & hashmask(hashpower)];
        primary_hashtable[hv & hashmask(hashpower)] = it;
    }

    pthread_mutex_lock(&hash_items_counter_lock);
    hash_items++;
	//哈希表节点数据大于容量的1.5倍，发送扩容信号，唤醒扩容线程。
    if (! expanding && hash_items > (hashsize(hashpower) * 3) / 2) {
        assoc_start_expand();
    }
    pthread_mutex_unlock(&hash_items_counter_lock);

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hash_items);
    return 1;
}

//删除节点方式:找到节点前驱节点，利用前驱节点删除。
void assoc_delete(const char *key, const size_t nkey, const uint32_t hv) {
    item **before = _hashitem_before(key, nkey, hv);

    if (*before) {
        item *nxt;
        pthread_mutex_lock(&hash_items_counter_lock);
        hash_items--;
        pthread_mutex_unlock(&hash_items_counter_lock);
        /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey, hash_items);
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;
        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    assert(*before != 0);
}

//是否运行扩容线程，可以通过stop_assoc_maintenance_thread设置为0使扩容线程退出
static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
//每次迁移多少个桶，默认一个。由于数据迁移时需要加锁，迁移桶的粒度越小，work线程更易获取到锁。
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

/* 扩容线程函数，扩容策略如下:
 * 扩容线程在main函数中创建，在assoc_insert后发现item数目大于哈希表容量1.5倍，唤醒扩容线程。
 * 扩容线程先创建一个2倍容量的新哈希表，然后进行把数据从旧哈希表迁移到新哈希表。
 * 迁移从旧表索引0开始，每次迁移一个桶(可以增加迁移粒度，但由于迁移需要加锁，可能导致work线程获取锁的等待时间增加)，
 * 迁移完成后释放旧表。
 */
static void *assoc_maintenance_thread(void *arg) {

    mutex_lock(&maintenance_lock);
    while (do_run_maintenance_thread) {
        int ii = 0;

        /* There is only one expansion thread, so no need to global lock. */
		//如果expanding为true才会进入循环体，所以迁移线程刚创建的时候，并不会进入循环体
        for (ii = 0; ii < hash_bulk_move && expanding; ++ii) {
            item *it, *next;
            int bucket;
            void *item_lock = NULL;

            /* bucket = hv & hashmask(hashpower) =>the bucket of hash table
             * is the lowest N bits of the hv, and the bucket of item_locks is
             *  also the lowest M bits of hv, and N is greater than M.
             *  So we can process expanding with only one item_lock. cool! */
             //获取单个桶锁
            if ((item_lock = item_trylock(expand_bucket))) {
				    //迁移一个桶中所有item
                    for (it = old_hashtable[expand_bucket]; NULL != it; it = next) {
                        next = it->h_next;
						//重新计算哈希值
                        bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower);
                        it->h_next = primary_hashtable[bucket];
                        primary_hashtable[bucket] = it;
                    }

                    old_hashtable[expand_bucket] = NULL;

                    expand_bucket++;
					//迁移完成
                    if (expand_bucket == hashsize(hashpower - 1)) {
                        expanding = false;  //将迁移标志设置0
                        free(old_hashtable);  //释放旧表
                        STATS_LOCK();
                        stats.hash_bytes -= hashsize(hashpower - 1) * sizeof(void *);
                        stats.hash_is_expanding = 0;
                        STATS_UNLOCK();
                        if (settings.verbose > 1)
                            fprintf(stderr, "Hash table expansion done\n");
                    }

            } else {
                usleep(10*1000);
            }

            if (item_lock) {
                item_trylock_unlock(item_lock);
                item_lock = NULL;
            }
        }

        if (!expanding) {
            /* We are done expanding.. just wait for next invocation */
			//不需要迁移，挂起迁移线程，直到worker线程插入数据后发现item数量已经到了1.5倍哈希表大小，  
            //此时调用worker线程调用assoc_start_expand函数，该函数会调用pthread_cond_signal  
            //唤醒迁移线程  
            started_expanding = false;
            pthread_cond_wait(&maintenance_cond, &maintenance_lock);
            /* assoc_expand() swaps out the hash table entirely, so we need
             * all threads to not hold any references related to the hash
             * table while this happens.
             * This is instead of a more complex, possibly slower algorithm to
             * allow dynamic hash table expansion without causing significant
             * wait times.
             */
            pause_threads(PAUSE_ALL_THREADS);
            assoc_expand();  //申请更大的哈希表,并将expanding设置为true
            pause_threads(RESUME_ALL_THREADS);
        }
    }
    return NULL;
}

//扩容线程ID
static pthread_t maintenance_tid;

//启动扩容线程
int start_assoc_maintenance_thread() {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }
    pthread_mutex_init(&maintenance_lock, NULL);
    if ((ret = pthread_create(&maintenance_tid, NULL,
                              assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

//停止扩容线程
void stop_assoc_maintenance_thread() {
    mutex_lock(&maintenance_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    mutex_unlock(&maintenance_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}

