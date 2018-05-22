/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/* Enable POSIX features beyond c99 for modern pthread and clock_gettime */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "./pni_epoll.h"
#undef NDEBUG                   /* keep assertions in release build */
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <stdio.h>              /* FIXME aconway 2018-05-18: perror */

#define FIXME_ASSERT(C) do { if (!(C)) { perror("FIXME: " #C); assert(0); } } while(0)

/* FIXME aconway 2018-05-22:  */
/*
 * Design
 *
 * Only one pni_epoll_wait() thread calls the real epoll_wait()
 * - marks fds *not armed* under lock and then disarms them in epoll
 * - buffers events for distribution
 * - other threads return buffered events
 *
 * split epoll_ctl MOD into 2 distinct calls:
 * - pni_arm() only called in worker thread. Resets the in_use flag and does epoll_ctl()
 * - pni_update() is a no-op if in_use==true, otherwise does epoll_ctl()
 */

/* Batch size should be >= max-concurrency, not a problem if too big */
#define PNI_EPOLL_BATCH 256
struct pni_epoll_batch {
  struct epoll_event buf[PNI_EPOLL_BATCH];
  size_t pos, len;
};

static inline bool batch_empty(struct pni_epoll_batch *b) { return b->pos >= b->len; }

static inline bool batch_pop(struct pni_epoll_batch *b, struct epoll_event* ev_out) {
  bool ok = !batch_empty(b);
  if (ok) memcpy(ev_out, &b->buf[b->pos++], sizeof(*ev_out));
  return ok;
}

static inline void batch_swap(struct pni_epoll_batch **a, struct pni_epoll_batch **b) {
  struct pni_epoll_batch *tmp = *a; *a = *b; *b = tmp;
}

static inline void batch_init(struct pni_epoll_batch *b, size_t pos, size_t len) {
  b->pos = pos; b->len = len;
}

void pni_epoll_data_init(pni_epoll_data_t *ed, int fd, uint32_t events) {
  ed->fd = fd;
  ed->events = events;
  ed->armed = false;
}

struct pni_epoll_t {
  int fd;                       /* epoll fd */
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool epoll_waiting;           /* A thread is calling the real epoll_wait() */
  /* Return events from ret; get new events in get. Swap and repeat */
  struct pni_epoll_batch *ret, *get;
  struct pni_epoll_batch batch1, batch2; /* The actual batches */
};

pni_epoll_t *pni_epoll(void) {
  int fd = epoll_create(1);
  if (fd < 0) return NULL;
  pni_epoll_t *ep = (pni_epoll_t *)calloc(1, sizeof(*ep));
  ep->fd = fd;
  pthread_mutex_init(&ep->mutex, NULL);
  pthread_cond_init(&ep->cond, NULL);
  batch_init((ep->ret = &ep->batch1), 0, 0);
  batch_init((ep->get = &ep->batch2), 0, 0);
  ep->epoll_waiting = false;
  return ep;
}

void pni_epoll_free(pni_epoll_t *ep) {
  if (ep == 0) return;
  assert(!ep->epoll_waiting);
  pthread_cond_destroy(&ep->cond);
  pthread_mutex_destroy(&ep->mutex);
  close(ep->fd);
  free(ep);
}

/* Get an event, return false if none. Call with ep->mutex locked */
static bool get_event_lh(pni_epoll_t *ep, struct epoll_event* ev_out) {
  /* If we're at the end of ep->ret and ep->get is not in use and has events, swap them. */
  if (batch_empty(ep->ret) && !ep->epoll_waiting && !batch_empty(ep->get)) {
    batch_swap(&ep->ret, &ep->get);
    pthread_cond_broadcast(&ep->cond); /* Notify threads that we've added events to ep->ret */
  }
  return batch_pop(ep->ret, ev_out);
}

static int epoll_ctl_lh(pni_epoll_t *ep, int op, pni_epoll_data_t *ed) {
  assert(ed->fd >= 0);
  struct epoll_event ev;
  ev.data.ptr = ed;
  ev.events = ed->events | EPOLLONESHOT;
  int err = epoll_ctl(ep->fd, op, ed->fd, &ev);
  FIXME_ASSERT(!err);
  return err;
}

/* Do the real epoll wait. Call with ep->mutex locked. */
static int real_epoll_wait_lh(pni_epoll_t *ep, int timeout, struct epoll_event *ev_out) {
  assert(batch_empty(ep->get)); /* Can't get here while there are buffered events */
  ep->epoll_waiting = true;
  pthread_mutex_unlock(&ep->mutex);
  int result = epoll_wait(ep->fd, ep->get->buf, PNI_EPOLL_BATCH, timeout);
  pthread_mutex_lock(&ep->mutex);
  ep->epoll_waiting = false;
  pthread_cond_signal(&ep->cond); /* allow another thread to wait */
  if (result > 0) {               /* Got at least one event */
    batch_init(ep->get, 0, result);
    /* Prevent false wake-up in case pni_epoll_update was called after
     * epoll_wait() returned but before re-lock of ep->mutex.  Make sure all
     * returned fds are disabled in epoll.  This is a thread-safe version of
     * the EPOLLONESHOT flag.
     */
    for (int i = 0; i < result; ++i) {
      pni_epoll_data_t *ed = (pni_epoll_data_t*)ep->get->buf[i].data.ptr;
      assert(ed->armed);
      int err = epoll_ctl_lh(ep, EPOLL_CTL_DEL, ed);
      if (!err) ed->armed = false;
      FIXME_ASSERT(!err); /* FIXME aconway 2018-05-22:  */
    }
    get_event_lh(ep, ev_out);
    return 1;
  }
  return result;
}

/* Compute absolute deadline from millisecond timeout */
static void deadline_from_timeout(struct timespec *deadline, int timeout) {
  if (timeout == -1) return;    /* We won't be using the deadline */
  clock_gettime(CLOCK_MONOTONIC, deadline);
  long long ns = deadline->tv_nsec + (timeout * 1000000L);
  deadline->tv_sec += ns / 1000000000L;
  deadline->tv_nsec = ns % 1000000000L;
}

int pni_epoll_wait(pni_epoll_t *ep, struct epoll_event *ev_out, int timeout) {
  pthread_mutex_lock(&ep->mutex);
  int result = 0;
  if (get_event_lh(ep, ev_out)) { /* Check buffers first */
    result = 1;
  } else if (timeout == 0) {    /* Try non-blocking wait */
      if (!ep->epoll_waiting) { /* But only if no other thread is waiting */
        result = real_epoll_wait_lh(ep, 0, ev_out);
      }
  } else {                      /* Do blocking wait */
    struct timespec deadline;
    deadline_from_timeout(&deadline, timeout);
    while(1) {
      assert(batch_empty(ep->get) && batch_empty(ep->ret)); /* Loop precondition */
      if (!ep->epoll_waiting) {
        result = real_epoll_wait_lh(ep, timeout, ev_out);
        break;
      } else {                  /* Wait for signal from epoll_wait thread */
        int err;
        if (timeout == -1) {
          err = pthread_cond_wait(&ep->cond, &ep->mutex);
        } else {
          err = pthread_cond_timedwait(&ep->cond, &ep->mutex, &deadline);
        }
        if (get_event_lh(ep, ev_out)) {
          result = 1;
          break;
        } else if (err == ETIMEDOUT) {
          result = 0;
          break;
        }
      }
    }
  }
  pthread_mutex_unlock(&ep->mutex);
  return result;
}

int pni_epoll_arm(pni_epoll_t *ep, pni_epoll_data_t *ed) {
  int err = 0;
  pthread_mutex_lock(&ep->mutex);
  if (ed->armed) {
    err = epoll_ctl_lh(ep, EPOLL_CTL_MOD, ed);
  } else {
    err = epoll_ctl_lh(ep, EPOLL_CTL_ADD, ed);
    if (!err) ed->armed = true;
  }
  pthread_mutex_unlock(&ep->mutex);
  FIXME_ASSERT(!err);           /* FIXME aconway 2018-05-18:  */
  return err;
}

int pni_epoll_disarm(pni_epoll_t *ep, pni_epoll_data_t *ed) {
  int err = 0;
  pthread_mutex_lock(&ep->mutex);
  if (ed->armed) {
    err = epoll_ctl_lh(ep, EPOLL_CTL_DEL, ed);
    if (!err) ed->armed = false;
  }
  pthread_mutex_unlock(&ep->mutex);
  FIXME_ASSERT(!err);               /* FIXME aconway 2018-05-18:  */
  return err;
}


int pni_epoll_update(pni_epoll_t *ep, pni_epoll_data_t *ed) {
  int err = 0;
  pthread_mutex_lock(&ep->mutex);
  if (ed->armed) {
    err = epoll_ctl_lh(ep, EPOLL_CTL_MOD, ed);
  }
  pthread_mutex_unlock(&ep->mutex);
  FIXME_ASSERT(!err);
  return err;
}
