/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_RINGBUFFER_H__
#define __SO_RINGBUFFER_H__

#include <sys/types.h>
#include <string.h>

typedef struct so_ring_buffer_t {
	char *data;

	size_t read_pos;
	size_t write_pos;

	size_t len;
	size_t cap;

	/* TODO: Add syncronization primitives */
	// Synchronization primitives
	int stop;
    pthread_mutex_t mutex;
    pthread_cond_t cond_full;    // Condition for full buffer
    pthread_cond_t cond_empty;   // Condition for empty buffer

	unsigned long next_seq;
	unsigned long *seq_to_timestamp;
	unsigned long seq_counter;
} so_ring_buffer_t;

int     ring_buffer_init(so_ring_buffer_t *rb, size_t cap);
ssize_t ring_buffer_enqueue(so_ring_buffer_t *rb, void *data, size_t size);
ssize_t ring_buffer_dequeue(so_ring_buffer_t *rb, void *data, size_t size);
void    ring_buffer_destroy(so_ring_buffer_t *rb);
void    ring_buffer_stop(so_ring_buffer_t *rb);

#endif /* __SO_RINGBUFFER_H__ */
