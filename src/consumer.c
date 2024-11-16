// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

size_t timestamp_to_seq(struct so_ring_buffer_t *rb, unsigned long timestamp)
{
	for (size_t i = 0; i < rb->seq_counter; i++) {
		if (rb->seq_to_timestamp[i] == timestamp)
			return i;
	}

	return -1;
}

void *consumer_thread(so_consumer_ctx_t *ctx)
{
	char buffer[PKT_SZ];
	struct so_packet_t *pkt;

	memset(buffer, 0, PKT_SZ);

	while (1) {
		size_t ret = ring_buffer_dequeue(ctx->producer_rb, buffer, PKT_SZ);

		if (ret <= 0)
			break;

		pkt = (struct so_packet_t *)buffer;
		int action = process_packet(pkt);
		unsigned long hash = packet_hash(pkt);
		unsigned long timestamp = pkt->hdr.timestamp;
		size_t seq = timestamp_to_seq(ctx->producer_rb, timestamp);

		if (seq == (size_t)-1) {
			printf("Invalid timestamp %lu\n", timestamp);
			continue;
		}

		// Wait for the correct sequence to log
		pthread_mutex_lock(&ctx->seq_mutex);
		while (ctx->producer_rb->next_seq != seq)
			pthread_cond_wait(&ctx->seq_cond, &ctx->seq_mutex);

		// Log the packet
		pthread_mutex_lock(&ctx->log_mutex);
		dprintf(ctx->out_fd, "%s %016lx %lu\n", RES_TO_STR(action), hash, timestamp);
		pthread_mutex_unlock(&ctx->log_mutex);

		// Update the global sequence and signal waiting threads
		ctx->producer_rb->next_seq++;
		pthread_cond_broadcast(&ctx->seq_cond);
		pthread_mutex_unlock(&ctx->seq_mutex);
	}

	return NULL;
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{
	int out_fd = open(out_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);

	if (out_fd < 0)
		return -1;

	so_consumer_ctx_t *ctx = malloc(num_consumers * sizeof(so_consumer_ctx_t));

	ctx->producer_rb = rb;
	ctx->out_fd = out_fd;
	ctx->producer_rb->next_seq = 0;

	pthread_mutex_init(&ctx->log_mutex, NULL);
	pthread_mutex_init(&ctx->seq_mutex, NULL);
	pthread_cond_init(&ctx->seq_cond, NULL);

	for (int i = 0; i < num_consumers; i++)
		pthread_create(&tids[i], NULL, (void * (*)(void *))consumer_thread, ctx);

	return num_consumers;
}
