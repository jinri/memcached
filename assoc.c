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

//���ݵȴ����������������߳��ٴ����������ϵȴ���
//��work�̲߳���item���ֹ�ϣ����Ҫ��չ��������������������źš�
static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t maintenance_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t hash_items_counter_lock = PTHREAD_MUTEX_INITIALIZER;

typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

/* ��ϣ���С��2�Ĵ��ݿ���ͨ��hashpower���� */
unsigned int hashpower = HASHPOWER_DEFAULT;

#define hashsize(n) ((ub4)1<<(n))  //��ϣ���С
#define hashmask(n) (hashsize(n)-1)  //��ϣ����ֵ

/* ��ϣ������ָ��. */
static item** primary_hashtable = 0;

/*
 * Previous hash table. During expansion, we look here for keys that haven't
 * been moved over to the primary yet.
 * �ɱ��ڹ�ϣ�������ڼ�ʹ��
 */
static item** old_hashtable = 0;

/* ��ϣ����Ŀ */
static unsigned int hash_items = 0;

/* Flag: Are we in the middle of expanding now? */
static bool expanding = false;  //�Ƿ���Ǩ����
static bool started_expanding = false;  //�Ƿ�ʼ����

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 */
static unsigned int expand_bucket = 0;  //ָ���Ǩ�Ƶ�Ͱ

//��ʼ����ϣ��
void assoc_init(const int hashtable_init) {
    //��������˹�ϣ���С�������ô�С������ʹ��Ĭ�ϲ�����
    if (hashtable_init) {
        hashpower = hashtable_init;
    }
	//�����ϣ��ռ�
    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (! primary_hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }
	//��¼��ϣ���С��ռ���ֽ�
    STATS_LOCK();
    stats.hash_power_level = hashpower;
    stats.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    unsigned int oldbucket;

    //��ϣ����Ǩ������״̬���һ�û��Ǩ�Ƶ���Ͱ��(�ڻ�û��Ǩ�Ƶ���Ͱʱ��assoc_insert��֤��ϣ��������ļ����뵽�ɱ�)
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it = old_hashtable[oldbucket];
    } else {
        //�ҵ�key����Ͱ
        it = primary_hashtable[hv & hashmask(hashpower)];
    }

    item *ret = NULL;
    int depth = 0;
	//������ͻ��
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

//����item������ǰ���ڵ��h_next��Ա��ַ,�������ʧ����ô�ͷ��س�ͻ�������  
//һ���ڵ��h_next��Ա��ַ����Ϊ���һ���ڵ��h_next��ֵΪNULL��ͨ���Է���ֵ  
//ʹ�� * ���������֪����û�в��ҳɹ���  
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

/* ����ϣ����������ӵ�2�� */
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

//���������߳�
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

    //��ϣ��������Ǩ��״̬�����һ�û��Ǩ�Ƶ���Ͱ�����뵽�ɱ�
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it->h_next = old_hashtable[oldbucket];
        old_hashtable[oldbucket] = it;
    } else {
        //����ͷ�巨���뵽Ͱ��
        it->h_next = primary_hashtable[hv & hashmask(hashpower)];
        primary_hashtable[hv & hashmask(hashpower)] = it;
    }

    pthread_mutex_lock(&hash_items_counter_lock);
    hash_items++;
	//��ϣ��ڵ����ݴ���������1.5�������������źţ����������̡߳�
    if (! expanding && hash_items > (hashsize(hashpower) * 3) / 2) {
        assoc_start_expand();
    }
    pthread_mutex_unlock(&hash_items_counter_lock);

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hash_items);
    return 1;
}

//ɾ���ڵ㷽ʽ:�ҵ��ڵ�ǰ���ڵ㣬����ǰ���ڵ�ɾ����
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

//�Ƿ����������̣߳�����ͨ��stop_assoc_maintenance_thread����Ϊ0ʹ�����߳��˳�
static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
//ÿ��Ǩ�ƶ��ٸ�Ͱ��Ĭ��һ������������Ǩ��ʱ��Ҫ������Ǩ��Ͱ������ԽС��work�̸߳��׻�ȡ������
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

/* �����̺߳��������ݲ�������:
 * �����߳���main�����д�������assoc_insert����item��Ŀ���ڹ�ϣ������1.5�������������̡߳�
 * �����߳��ȴ���һ��2���������¹�ϣ��Ȼ����а����ݴӾɹ�ϣ��Ǩ�Ƶ��¹�ϣ��
 * Ǩ�ƴӾɱ�����0��ʼ��ÿ��Ǩ��һ��Ͱ(��������Ǩ�����ȣ�������Ǩ����Ҫ���������ܵ���work�̻߳�ȡ���ĵȴ�ʱ������)��
 * Ǩ����ɺ��ͷžɱ�
 */
static void *assoc_maintenance_thread(void *arg) {

    mutex_lock(&maintenance_lock);
    while (do_run_maintenance_thread) {
        int ii = 0;

        /* There is only one expansion thread, so no need to global lock. */
		//���expandingΪtrue�Ż����ѭ���壬����Ǩ���̸߳մ�����ʱ�򣬲��������ѭ����
        for (ii = 0; ii < hash_bulk_move && expanding; ++ii) {
            item *it, *next;
            int bucket;
            void *item_lock = NULL;

            /* bucket = hv & hashmask(hashpower) =>the bucket of hash table
             * is the lowest N bits of the hv, and the bucket of item_locks is
             *  also the lowest M bits of hv, and N is greater than M.
             *  So we can process expanding with only one item_lock. cool! */
             //��ȡ����Ͱ��
            if ((item_lock = item_trylock(expand_bucket))) {
				    //Ǩ��һ��Ͱ������item
                    for (it = old_hashtable[expand_bucket]; NULL != it; it = next) {
                        next = it->h_next;
						//���¼����ϣֵ
                        bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower);
                        it->h_next = primary_hashtable[bucket];
                        primary_hashtable[bucket] = it;
                    }

                    old_hashtable[expand_bucket] = NULL;

                    expand_bucket++;
					//Ǩ�����
                    if (expand_bucket == hashsize(hashpower - 1)) {
                        expanding = false;  //��Ǩ�Ʊ�־����0
                        free(old_hashtable);  //�ͷžɱ�
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
			//����ҪǨ�ƣ�����Ǩ���̣߳�ֱ��worker�̲߳������ݺ���item�����Ѿ�����1.5����ϣ���С��  
            //��ʱ����worker�̵߳���assoc_start_expand�������ú��������pthread_cond_signal  
            //����Ǩ���߳�  
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
            assoc_expand();  //�������Ĺ�ϣ��,����expanding����Ϊtrue
            pause_threads(RESUME_ALL_THREADS);
        }
    }
    return NULL;
}

//�����߳�ID
static pthread_t maintenance_tid;

//���������߳�
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

//ֹͣ�����߳�
void stop_assoc_maintenance_thread() {
    mutex_lock(&maintenance_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    mutex_unlock(&maintenance_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}

