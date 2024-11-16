// SPDX-License-Identifier: BSD-3-Clause

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "ring_buffer.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	ring->data = malloc(cap);
	if (ring->data == NULL)
		return -1;

	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->len = 0;
	ring->cap = cap;
	ring->stop = 0;

	pthread_mutex_init(&ring->mutex, NULL);
	pthread_cond_init(&ring->cond_full, NULL);
	pthread_cond_init(&ring->cond_empty, NULL);

	ring->seq_to_timestamp = malloc(20100 * sizeof(unsigned long));
	ring->next_seq = 0;
	ring->seq_counter = 0;

	return 1;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	pthread_mutex_lock(&ring->mutex);

	while (ring->len + size > ring->cap)
		pthread_cond_wait(&ring->cond_full, &ring->mutex);

	size_t end_space = ring->cap - ring->write_pos;

	if (size <= end_space) { // data fits in buffer
		memcpy(ring->data + ring->write_pos, data, size);
	} else { // split data at the end and beginning of buffer
		memcpy(ring->data + ring->write_pos, data, end_space);
		memcpy(ring->data, (char *)data + end_space, size - end_space);
	}

	ring->write_pos = (ring->write_pos + size) % ring->cap;
	ring->len += size;

	pthread_cond_signal(&ring->cond_empty);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */
	pthread_mutex_lock(&ring->mutex);

	while (ring->len < size) {
		if (ring->stop == 1) { // stop condition
			pthread_mutex_unlock(&ring->mutex);
			return 0;
		}
		pthread_cond_wait(&ring->cond_empty, &ring->mutex);
	}

	size_t end_space = ring->cap - ring->read_pos;

	if (size <= end_space) { // data fits in buffer
		memcpy(data, ring->data + ring->read_pos, size);
	} else { // split data at the end and beginning of buffer
		memcpy(data, ring->data + ring->read_pos, end_space);
		memcpy((char *)data + end_space, ring->data, size - end_space);
	}

	ring->read_pos = (ring->read_pos + size) % ring->cap;
	ring->len -= size;

	pthread_cond_signal(&ring->cond_full);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	free(ring->data);
	free(ring->seq_to_timestamp);
	pthread_mutex_destroy(&ring->mutex);
	pthread_cond_destroy(&ring->cond_full);
	pthread_cond_destroy(&ring->cond_empty);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	ring->stop = 1;
	pthread_mutex_lock(&ring->mutex);
	pthread_cond_broadcast(&ring->cond_empty);
	pthread_cond_broadcast(&ring->cond_full);
	pthread_mutex_unlock(&ring->mutex);
}
