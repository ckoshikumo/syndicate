#ifndef INCLUDE_SYNDICATE_H
#define INCLUDE_SYNDICATE_H

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#if defined(SYNDICATE_DEFAULT_CALLOC) && defined(SYNDICATE_DEFAULT_FREE)
#elif !defined(SYNDICATE_DEFAULT_CALLOC) && !defined(SYNDICATE_DEFAULT_FREE)
#else
#error                                                                         \
    "Must define both SYNDICATE_DEFAULT_CALLOC and SYNDICATE_DEFAULT_FREE, or neither."
#endif

#ifndef SYNDICATE_DEFAULT_CALLOC
#define SYNDICATE_DEFAULT_CALLOC calloc
#define SYNDICATE_DEFAULT_FREE free
#endif

#ifndef SYNDICATE_TASK_LOCAL_DATA_BYTES
#define SYNDICATE_TASK_LOCAL_DATA_BYTES 16
#endif

#ifndef SYNDICATE_MAX_TASKS_PER_QUEUE
#define SYNDICATE_MAX_TASKS_PER_QUEUE 128
#endif

#ifndef SYNDICATE_MAX_TASKS_PER_BATCH
#define SYNDICATE_MAX_TASKS_PER_BATCH 32
#endif

#ifndef SYNDICATE_MAX_BATCHES
#define SYNDICATE_MAX_BATCHES 32
#endif

#ifndef SYN_API
#define SYN_API
#endif

typedef void (*syn_task_fn)(void *data, uint8_t *local);

typedef struct {
	syn_task_fn fn;
	void *data;
	uint8_t local[SYNDICATE_TASK_LOCAL_DATA_BYTES];
} syn_task_s;

typedef struct {
	size_t cnt;
	syn_task_s tasks[SYNDICATE_MAX_TASKS_PER_BATCH];
} syn_task_batch_s;

typedef struct syndicate_s syndicate_s;

typedef struct {
	size_t entry_idx;
	syndicate_s *pool;
} syn_batch_task_data_s;

static_assert(SYNDICATE_TASK_LOCAL_DATA_BYTES >= sizeof(syn_batch_task_data_s),
              "SYNDICATE_TASK_LOCAL_DATA_BYTES must be large enough to hold "
              "sizeof(syn_batch_task_data_s)");

SYN_API syndicate_s *syn_create(int worker_cnt, int (*worker_fn)(void *data));
SYN_API syndicate_s *syn_create_alloc(int worker_cnt,
                                      int (*worker_fn)(void *data),
                                      void *(*calloc_fn)(size_t n, size_t sz),
                                      void (*free_fn)(void *ptr));
SYN_API void syn_destroy(syndicate_s *pool);
SYN_API int syn_worker_loop(void *data);
SYN_API int syn_task_submit(syndicate_s *pool, syn_task_s task);
SYN_API int syn_batch_submit(syndicate_s *pool, syn_task_batch_s batch);

#endif // INCLUDE_SYNDICATE_H

#ifdef SYNDICATE_IMPLEMENTATION

#include <stdlib.h>

// Types and structs

typedef enum {
	SYN_QUEUE_BATCH = 0,
	SYN_QUEUE_SINGLE,
	SYN_QUEUE_N
} syn_task_queue_e;

typedef struct syn_task_queue_s {
	size_t head, tail, cnt;
	syn_task_s tasks[SYNDICATE_MAX_TASKS_PER_QUEUE];
} syn_task_queue_s;

struct syn_batch_entry_s;

typedef struct syn_batch_entry_s {
	size_t index;
	twn_atomic_s task_count;
	twn_cond_s *task_completed;
	twn_mutex_s *mutex;
	syn_task_s tasks[SYNDICATE_MAX_TASKS_PER_BATCH];
} syn_batch_entry_s;

typedef struct {
	syn_task_s *real_task;
	size_t entry_idx;
} syn_batch_info_s;

typedef struct syndicate_s {
	size_t worker_cnt;
	bool running;
	twn_thread_s **workers;
	twn_mutex_s *pool_mutex;
	twn_cond_s *not_empty;
	syn_task_queue_s *queues;

	twn_mutex_s *batches_mutex;
	size_t batches_top;
	size_t batches_free_list[SYNDICATE_MAX_BATCHES];
	syn_batch_entry_s batches[SYNDICATE_MAX_BATCHES];
	void (*free_fn)(void *ptr);
} syndicate_s;

// Forward declarations

static void batch_task_fn(void *data, uint8_t *local);
static int task_next(syndicate_s *pool, syn_task_s *out_task, bool block);
static int task_submit(syndicate_s *pool, syn_task_queue_e queue_type,
                       syn_task_s task);
static void wait_for_batch(syndicate_s *pool, syn_batch_entry_s *entry);
static void batch_entry_free(syndicate_s *pool, syn_batch_entry_s *entry);
static syn_batch_entry_s *get_empty_batch_entry(syndicate_s *pool);

// Static variables

// Implementation

// Public API

SYN_API syndicate_s *syn_create_alloc(int worker_cnt,
                                      int (*worker_fn)(void *data),
                                      void *(*calloc_fn)(size_t n, size_t sz),
                                      void (*free_fn)(void *ptr)) {

	guard_exit(!(calloc_fn && !free_fn) && !(!calloc_fn && free_fn),
	           "Syndicate: either both calloc_fn AND free_fn are defined or "
	           "neither.");

	void *(*c)(size_t n, size_t sz) =
	    calloc_fn ? calloc_fn : SYNDICATE_DEFAULT_CALLOC;

	syndicate_s *pool = c(1, sizeof(syndicate_s));
	guard_mem(pool);

	pool->workers = c(worker_cnt, sizeof(twn_thread_s *));
	guard_mem(pool->workers);

	pool->worker_cnt = worker_cnt;
	pool->pool_mutex = twn_mutex_create();
	pool->not_empty = twn_condition_create();
	pool->running = true;
	pool->free_fn = free_fn ? free_fn : SYNDICATE_DEFAULT_FREE;

	pool->queues = c(SYN_QUEUE_N, sizeof(syn_task_queue_s));

	for (size_t i = 0; i < SYN_QUEUE_N; i++)
		pool->queues[i] = (syn_task_queue_s) {0};

	for (int i = 0; i < worker_cnt; i++)
		pool->workers[i] = twn_thread_create(worker_fn, "twn_worker", pool);

	pool->batches_mutex = twn_mutex_create();
	pool->batches_top = SYNDICATE_MAX_BATCHES;

	for (size_t i = 0; i < SYNDICATE_MAX_BATCHES; i++) {
		pool->batches_free_list[i] = SYNDICATE_MAX_BATCHES - i - 1;
		pool->batches[i].mutex = twn_mutex_create();
		pool->batches[i].task_completed = twn_condition_create();
	}

	return pool;
}

SYN_API syndicate_s *syn_create(int worker_cnt, int (*worker_fn)(void *data)) {
	return syn_create_alloc(worker_cnt, worker_fn, NULL, NULL);
}

// This should be called by a function that will initialize a worker thread and
// make it part of the worker pool.
SYN_API int syn_worker_loop(void *data) {
	syndicate_s *pool = (syndicate_s *) data;
	syn_task_s task = {0};

	while (task_next(pool, &task, true) == 0) task.fn(task.data, task.local);

	return 0;
}

SYN_API int syn_task_submit(syndicate_s *pool, syn_task_s task) {
	return task_submit(pool, SYN_QUEUE_SINGLE, task);
}

SYN_API int syn_batch_submit(syndicate_s *pool, syn_task_batch_s batch) {
	syn_batch_entry_s *entry = get_empty_batch_entry(pool);
	guard_exit(entry, "Syndicate: Too many task batches");

	twn_atomic_s *count = &entry->task_count;
	twn_atomic_set(count, 0);
	size_t enqueued = 0;

	for (size_t i = 0; i < batch.cnt; i++) {
		syn_task_s *real_task = &batch.tasks[i];

		syn_task_s wrapper_task = {.fn = batch_task_fn, .data = real_task};

		memcpy(wrapper_task.local,
		       &((syn_batch_task_data_s) {.entry_idx = entry->index,
		                                  .pool = pool}),
		       sizeof(syn_batch_task_data_s));

		if (task_submit(pool, SYN_QUEUE_BATCH, wrapper_task) != 0) break;

		twn_atomic_inc(count);
		enqueued++;
	}

	if (enqueued == 0) {
		batch_entry_free(pool, entry);
		return -1;
	}

	wait_for_batch(pool, entry);
	return (enqueued == batch.cnt) ? 0 : enqueued;
}

SYN_API void syn_destroy(syndicate_s *pool) {
	twn_with_mutex(pool->pool_mutex, {
		pool->running = false;
		twn_cond_broadcast(pool->not_empty);
	});

	for (size_t i = 0; i < pool->worker_cnt; i++)
		twn_thread_wait(pool->workers[i], NULL);

	twn_mutex_destroy(pool->pool_mutex);
	twn_cond_destroy(pool->not_empty);
	twn_mutex_destroy(pool->batches_mutex);

	for (size_t i = 0; i < SYNDICATE_MAX_BATCHES; i++) {
		twn_mutex_destroy(pool->batches[i].mutex);
		twn_cond_destroy(pool->batches[i].task_completed);
	}

	pool->free_fn(pool->queues);
	pool->free_fn(pool->workers);
	pool->free_fn(pool);
}

// Helpers

static int task_submit(syndicate_s *pool, syn_task_queue_e queue_type,
                       syn_task_s task) {
	if (pool->worker_cnt == 0) {
		// No worker threads; execute immediately
		task.fn(task.data, task.local);
		return 0;
	}

	syn_task_queue_s *queue = &pool->queues[queue_type];
	twn_mutex_lock(pool->pool_mutex);

	if (queue->cnt == SYNDICATE_MAX_TASKS_PER_QUEUE || !pool->running) {
		twn_mutex_release(pool->pool_mutex);
		return -1;
	}

	queue->tasks[queue->tail] = task;
	queue->tail = (queue->tail + 1) % SYNDICATE_MAX_TASKS_PER_QUEUE;
	queue->cnt++;

	twn_cond_signal(pool->not_empty);
	twn_mutex_release(pool->pool_mutex);

	return 0;
}

static int task_next(syndicate_s *pool, syn_task_s *out_task, bool block) {
	int rc = -1;

	// If we're blocking, worker_thread is calling, so we cycle through all
	// queues. If we're not blocking, then we're waiting for a batch to
	// complete; so we'll only pick tasks from the batch queue.
	size_t max = block ? SYN_QUEUE_N : SYN_QUEUE_SINGLE;

	twn_mutex_lock(pool->pool_mutex);
	while (pool->running) {
		for (size_t q = 0; q < max; q++) {
			syn_task_queue_s *queue = &pool->queues[q];
			if (queue->cnt > 0) {
				*out_task = queue->tasks[queue->head];
				queue->head = (queue->head + 1) % SYNDICATE_MAX_TASKS_PER_QUEUE;
				queue->cnt--;
				rc = 0;
				goto done;
			}
		}

		if (!block) goto done;
		twn_cond_wait(pool->not_empty, pool->pool_mutex);
	}

done:
	twn_mutex_release(pool->pool_mutex);
	return rc;
}

static syn_batch_entry_s *get_empty_batch_entry(syndicate_s *pool) {
	twn_mutex_lock(pool->batches_mutex);

	if (pool->batches_top == 0) {
		twn_mutex_release(pool->batches_mutex);
		return NULL;
	}

	size_t index = pool->batches_free_list[--pool->batches_top];
	syn_batch_entry_s *entry = &pool->batches[index];
	guard_exit((twn_atomic_get(&entry->task_count) == 0),
	           "Syndicate: got an \"empty\" batch_entry with count != 0");

	entry->index = index;
	twn_mutex_release(pool->batches_mutex);
	return entry;
}

static void batch_entry_free(syndicate_s *pool, syn_batch_entry_s *entry) {
	size_t index = entry->index;

	twn_with_mutex(pool->batches_mutex,
	               { pool->batches_free_list[pool->batches_top++] = index; });
}

static void wait_for_batch(syndicate_s *pool, syn_batch_entry_s *entry) {
	syn_task_s task;

	for (;;) {
		while (task_next(pool, &task, false) == 0)
			task.fn(task.data, task.local);

		if (twn_atomic_get(&entry->task_count) == 0) break;

		twn_with_mutex(entry->mutex, {
			while (twn_atomic_get(&entry->task_count) > 0)
				twn_cond_wait(entry->task_completed, entry->mutex);
		});
	}

	batch_entry_free(pool, entry);
}

static void batch_task_fn(void *data, uint8_t *local) {
	syn_task_s *task = (syn_task_s *) data;

	task->fn(task->data, task->local);

	syn_batch_task_data_s *task_data = (syn_batch_task_data_s *) local;
	syn_batch_entry_s *entry = &task_data->pool->batches[task_data->entry_idx];

	twn_with_mutex(entry->mutex, {
		twn_atomic_dec(&entry->task_count);
		twn_cond_signal(entry->task_completed);
	});
}

#endif // SYNDICATE_IMPLEMENTATION
