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

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	char buffer[PKT_SZ];
	memset(buffer, 0, PKT_SZ);
	struct so_packet_t *pkt;

	while (1) {
		size_t ret = ring_buffer_dequeue(ctx->producer_rb, buffer, PKT_SZ);

		if (ret <= 0)
			break;

		pkt = (struct so_packet_t *)buffer;

		int action = process_packet(pkt);
		unsigned long hash = packet_hash(pkt);
		unsigned long timestamp = pkt->hdr.timestamp;

		pthread_mutex_lock(&ctx->log_mutex);
		dprintf(ctx->out_fd, "%s %016lx %lu\n", RES_TO_STR(action), hash, timestamp);

		pthread_mutex_unlock(&ctx->log_mutex);
	}
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{
	int out_fd = open(out_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);

	if (out_fd < 0) {
		return -1;
	}

	so_consumer_ctx_t *ctx = malloc(num_consumers * sizeof(so_consumer_ctx_t));
	ctx->producer_rb = rb;
	ctx->out_fd = out_fd;
	pthread_mutex_init(&ctx->log_mutex, NULL);

	for (int i = 0; i < num_consumers; i++) {
		pthread_create(&tids[i], NULL, (void * (*)(void *))consumer_thread, ctx);
	}

	return num_consumers;
}
