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

/* This test is intended to be run under a race detector (e.g. helgrind,
   libtsan) to uncover races by generating concurrent, pseudo-random activity.
   It should also be run under memory checkers (e.g. valgrind memcheck, libasan)
   for race conditions that leak memory.

   The goal is to drive concurrent activity as hard as possible while staying
   within the rules of the API. It may do things that an application is unlikely
   to do (e.g. close a listener before it opens) but that are not disallowed by
   the API rules - that's the goal.

   It is impossible to get repeatable behaviour with multiple threads due to
   unpredictable scheduling. Currently using plain old rand(), if quality of
   randomness is problem we can upgrade.

   TODO
   - message tracking
   - closing connections
   - pn_proactor_release_connection and re-use with pn_proactor_connect/accept
   - sending/receiving/tracking messages
*/

#include "thread.h"

#include <proton/connection.h>
#include <proton/event.h>
#include <proton/listener.h>
#include <proton/netaddr.h>
#include <proton/proactor.h>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#undef NDEBUG                   /* Enable assert even in release builds */
#include <assert.h>

#define BACKLOG 16              /* Listener backlog */
#define TIMEOUT_MAX 100         /* Milliseconds */
#define SLEEP_MAX 100           /* Milliseconds */

static pthread_mutex_t debug_lock = PTHREAD_MUTEX_INITIALIZER;
static size_t debug_count = 0;
static bool debug_enable = false;

void debug(void *obj, const char *fmt, ...) {
  pthread_mutex_lock(&debug_lock);
  ++debug_count;
  if (debug_enable) {
    fprintf(stderr, "%lx [%p] ", pthread_self(), obj);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
  }
  pthread_mutex_unlock(&debug_lock);
}

/* Thread safe pools of structs.

   An element can be "picked" at random concurrently by many threads, and is
   guaranteed not to be freed until "done" is called by all threads.  Element
   structs must have their own locks if picking threads can modify the element.
*/

struct pool;

/* This must be the first field in structs that go in a pool */
typedef struct pool_entry {
  struct pool *pool;
  int ref;                      /* threads using this entry */
} pool_entry;

typedef void (*pool_free_fn)(void*);

typedef struct pool {
  pthread_mutex_t lock;
  pool_entry **entries;
  size_t size, max;
  pool_free_fn free_fn;
} pool;


void pool_init(pool *p, size_t max, pool_free_fn free_fn) {
  pthread_mutex_init(&p->lock, NULL);
  p->entries = (pool_entry**)calloc(max, sizeof(*p->entries));
  p->size = 0;
  p->max = max;
  p->free_fn = free_fn;
}

void pool_destroy(pool *p) {
  pthread_mutex_destroy(&p->lock);
  free(p->entries);
}

bool pool_add(pool *p, pool_entry *entry) {
  entry->pool = p;
  entry->ref = 1;
  bool ok = false;
  pthread_mutex_lock(&p->lock);
  if (p->size < p->max) {
    p->entries[p->size++] = entry;
    ok = true;
  }
  pthread_mutex_unlock(&p->lock);
  return ok;
}

void pool_unref(pool_entry *entry) {
  pool *p = (pool*)entry->pool;
  pthread_mutex_lock(&p->lock);
  if (--entry->ref == 0) {
    size_t i = 0;
    while (i < p->size && p->entries[i] != entry) ++i;
    if (i < p->size) {
      --p->size;
      memmove(p->entries + i, p->entries + i + 1, (p->size - i)*sizeof(*p->entries));
      (*p->free_fn)(entry);
    }
  }
  pthread_mutex_unlock(&p->lock);
}

  /* Pick a random element, NULL if empty. Call pool_unref(entry) when finished */
pool_entry *pool_pick(pool *p) {
  pool_entry *entry = NULL;
  pthread_mutex_lock(&p->lock);
  if (p->size) {
    entry = p->entries[rand() % p->size];
    ++entry->ref;
  }
  pthread_mutex_unlock(&p->lock);
  return entry;
}

/* Macro for type-safe pools */
#define DECLARE_POOL(P, T)                                              \
  typedef struct P { pool p; } P;                                       \
  void P##_init(P *p, size_t max, void (*free_fn)(T*)) { pool_init((pool*)p, max, (pool_free_fn) free_fn); } \
  bool P##_add(P *p, T *entry) { return pool_add((pool*)p, &entry->entry); } \
  void P##_unref(T *entry) { pool_unref(&entry->entry); }               \
  T *P##_pick(P *p) { return (T*)pool_pick((pool*) p); } \
  void P##_destroy(P *p) { pool_destroy(&p->p); } 


/* Connection pool */

typedef struct uconnection {
  pool_entry entry;
  pn_connection_t *pn_connection;
  pthread_mutex_t lock;    /* Lock PN_TRANSPORT_CLOSED vs. pn_connection_wake() */
} uconnection;

DECLARE_POOL(cpool, uconnection)

static uconnection* uconnection_new(void) {
  uconnection *u = (uconnection*)malloc(sizeof(*u));
  u->pn_connection = pn_connection();
  pthread_mutex_init(&u->lock, NULL);
  pn_connection_set_context(u->pn_connection, u);
  return u;
}

static void uconnection_free(uconnection* u) {
  /* Don't free u->pn_connection, freed by proactor */
  pthread_mutex_destroy(&u->lock);
  free(u);
}

void cpool_connect(cpool *cp, pn_proactor_t *proactor, const char *addr) {
  uconnection *u = uconnection_new();
  if (cpool_add(cp, u)) {
    debug(u, "connection start %s", addr);
    pn_proactor_connect(proactor, u->pn_connection, addr);
  } else {
    uconnection_free(u);
  }
}

void cpool_wake(cpool *cp) {
  uconnection *u = cpool_pick(cp);
  if (u) {
    pthread_mutex_lock(&u->lock);
    if (u && u->pn_connection) pn_connection_wake(u->pn_connection);
    pthread_mutex_unlock(&u->lock);
    cpool_unref(u);
  }
}

static void uconnection_on_close(uconnection *u) {
  debug(u, "connection closed");
  pthread_mutex_lock(&u->lock);
  u->pn_connection = NULL;
  pthread_mutex_unlock(&u->lock);
  cpool_unref(u);
}

/* Listener pool */

typedef struct ulistener {
  pool_entry entry;
  pn_listener_t *pn_listener;
  pthread_mutex_t lock;         /* Lock PN_LISTENER_CLOSE vs. pn_listener_close() */
  char addr[PN_MAX_ADDR];
} ulistener;

DECLARE_POOL(lpool, ulistener)

static ulistener* ulistener_new(void) {
  ulistener *u = (ulistener*)malloc(sizeof(*u));
  u->pn_listener = pn_listener();
  pthread_mutex_init(&u->lock, NULL);
  /* Use "invalid:address" because "" is treated as ":5672" */
  strncpy(u->addr, "invalid:address", sizeof(u->addr));
  pn_listener_set_context(u->pn_listener, u);
  return u;
}

static void ulistener_free(ulistener *u) {
  /* Does not free u->pn_listener, it  is freed by the proactor */
  pthread_mutex_destroy(&u->lock);
  free(u);
}

static void lpool_listen(lpool *lp, pn_proactor_t *proactor) {
  char a[PN_MAX_ADDR];
  pn_proactor_addr(a, sizeof(a), "", "0");
  ulistener *u = ulistener_new();
  if (lpool_add(lp, u)) {
    debug(u, "listener start");
    pn_proactor_listen(proactor, u->pn_listener, a, BACKLOG);
  } else {
    pn_listener_free(u->pn_listener); /* Won't be freed by proactor */
    ulistener_free(u);
  }
}

/* Advertise address once open */
static void ulistener_on_open(ulistener *u) {
  pthread_mutex_lock(&u->lock);
  if (u->pn_listener) pn_netaddr_str(pn_listener_addr(u->pn_listener), u->addr, sizeof(u->addr));
  pthread_mutex_unlock(&u->lock);
  debug(u, "listening on %s", u->addr);
}

static void ulistener_on_close(ulistener *u) {
  debug(u, "listener closed %s", u->addr);
  pthread_mutex_lock(&u->lock);
  u->pn_listener = NULL;
  pthread_mutex_unlock(&u->lock);
  lpool_unref(u);
}

/* Pick a random listening address from the listener pool.
   Returns "invalid:address" for no address.
*/
static void lpool_addr(lpool *lp, char* a, size_t s) {
  strncpy(a, "invalid:address", s);
  ulistener *u = lpool_pick(lp);
  if (u) {
    pthread_mutex_lock(&u->lock);
    strncpy(a, u->addr, s);
    pthread_mutex_unlock(&u->lock);
    lpool_unref(u);
  }
}

void lpool_close(lpool *lp) {
  ulistener *u = lpool_pick(lp);
  if (u) {
    debug(u, "listener close %s", u->addr);
    pthread_mutex_lock(&u->lock);
    if (u->pn_listener) pn_listener_close(u->pn_listener);
    pthread_mutex_unlock(&u->lock);
    lpool_unref(u);
  }
}

/* Global state */

typedef struct {
  pn_proactor_t *proactor;
  int threads;
  lpool listeners;              /* waiting for PN_PROACTOR_LISTEN */
  cpool connections_active;     /* active in the proactor */
  cpool connections_idle;       /* released and can be reused */

  pthread_mutex_t lock;
  bool shutdown;
} global;

void global_init(global *g, int threads) {
  memset(g, 0, sizeof(*g));
  g->proactor = pn_proactor();
  g->threads = threads;
  lpool_init(&g->listeners, g->threads/2, ulistener_free);
  cpool_init(&g->connections_active, g->threads/2, uconnection_free);
  cpool_init(&g->connections_idle, g->threads/2, uconnection_free);
  pthread_mutex_init(&g->lock, NULL);
}

void global_destroy(global *g) {
  pn_proactor_free(g->proactor);
  lpool_destroy(&g->listeners);
  cpool_destroy(&g->connections_active);
  cpool_destroy(&g->connections_idle);
  pthread_mutex_destroy(&g->lock);
}

bool global_get_shutdown(global *g) {
  pthread_mutex_lock(&g->lock);
  bool ret = g->shutdown;
  pthread_mutex_unlock(&g->lock);
  return ret;
}

void global_set_shutdown(global *g, bool set) {
  pthread_mutex_lock(&g->lock);
  g->shutdown = set;
  pthread_mutex_unlock(&g->lock);
}

/* NOTE: listen/connect/accept must be synchronized with shutdown */

void global_listen(global *g) {
  pthread_mutex_lock(&g->lock);
  if (!g->shutdown) lpool_listen(&g->listeners, g->proactor);
  pthread_mutex_unlock(&g->lock);
}

/* addr=NULL means use random choice of lpool listeners */
void global_connect(global *g, const char *addr) {
  char a[PN_MAX_ADDR];
  if (!addr) {
    lpool_addr(&g->listeners, a, sizeof(a));
    addr = a;
  }
  pthread_mutex_lock(&g->lock);
  if (!g->shutdown) cpool_connect(&g->connections_active, g->proactor, addr);
  pthread_mutex_unlock(&g->lock);
}

/* FIXME aconway 2018-03-12:  */
/* void global_accept(global *g) { */
/*   pthread_mutex_lock(&g->lock); */
/*   pthread_mutex_unlock(&g->lock); */
/* } */

void global_timeout(global *g) {
  int t = rand() % TIMEOUT_MAX;
  debug(NULL, "timeout %d", t);
  pn_proactor_set_timeout(g->proactor, t);
}

/* Return true with given probability */
static bool maybe(double probability) {
  if (probability == 1.0) return true;
  return rand() < (probability * RAND_MAX);
}

/* Run random activities that can be done from any thread. */
static void global_do_stuff(global *g) {
  if (maybe(0.5)) global_connect(g, NULL);
  if (maybe(0.3)) global_listen(g);
  if (maybe(0.5)) cpool_wake(&g->connections_active);
  if (maybe(0.5)) cpool_wake(&g->connections_idle);
  if (maybe(0.1)) lpool_close(&g->listeners);
  if (maybe(0.5)) global_timeout(g);
}

static void* user_thread(void* void_g) {
  debug(NULL, "user_thread start");
  global *g = (global*)void_g;
  while (!global_get_shutdown(g)) {
    global_do_stuff(g);
    millisleep(rand() % SLEEP_MAX);
  }
  debug(NULL, "user_thread end");
  return NULL;
}

static bool handle(global *g, pn_event_t *e) {
  switch (pn_event_type(e)) {

   case PN_PROACTOR_TIMEOUT: {
     global_do_stuff(g);
     break;
   }
   case PN_LISTENER_OPEN: {
     ulistener *u = (ulistener*)pn_listener_get_context(pn_event_listener(e));
     ulistener_on_open(u);
     global_connect(g, u->addr); /* Start at least one connection */
     break;
   }
   case PN_LISTENER_CLOSE: {
     ulistener_on_close((ulistener*)pn_listener_get_context(pn_event_listener(e)));
     break;
   }
   case PN_TRANSPORT_CLOSED: {
     uconnection_on_close((uconnection*)pn_connection_get_context(pn_event_connection(e)));
     break;
   }
   case PN_PROACTOR_INACTIVE:           /* Shutting down */
    pn_proactor_interrupt(g->proactor); /* Interrupt remaining threads */
    return false;

   case PN_PROACTOR_INTERRUPT:
    pn_proactor_interrupt(g->proactor); /* Pass the interrupt along */
    return false;

    /* FIXME aconway 2018-03-09: MORE */

   default:
    break;
  }
  return true;
}

static void* proactor_thread(void* void_g) {
  debug(NULL, "proactor_thread start");
  global *g = (global*) void_g;
  bool ok = true;
  while (ok) {
    pn_event_batch_t *events = pn_proactor_wait(g->proactor);
    pn_event_t *e;
    while (ok && (e = pn_event_batch_next(events))) {
      ok = ok && handle(g, e);
    }
    pn_proactor_done(g->proactor, events);
  }
  debug(NULL, "proactor_thread end");
  return NULL;
}

/* Command line arguments */

static const int default_runtime = 1;
static const int default_threads = 8;

void usage(const char *prog, const char *msg) {
  if (msg) fprintf(stderr, "%s\n", msg);
  fprintf(stderr, "usage: %s [TIME [THREADS]]\n", prog);
  fprintf(stderr, "  TIME: total run-time in seconds (default %d)\n", default_runtime);
  fprintf(stderr, "  THREADS: total number of threads (default %d)\n", default_threads);
  fprintf(stderr, "environment: set PN_TRACE_THR to print actions to stderr\n");
  exit(1);
}

int main(int argc, const char* argv[]) {
  const char **arg = argv + 1;
  const char **end = argv + argc;

  int runtime = default_runtime;
  if (arg < end) runtime = atoi(*arg++);
  if (runtime <= 0) usage(argv[0], "invalid time");

  int threads = default_threads;
  if (arg < end) threads = atoi(*arg++);
  if (threads <= 0) usage(argv[0], "invalid thread count");
  if (threads % 2) threads += 1; /* Round up to even: half proactor, half user */

  if (arg < end) usage(argv[0], "too many arguments");

  debug_enable = getenv("PN_TRACE_THR");

  /* Set up global state, start threads */
  global g;
  global_init(&g, threads);
  global_listen(&g);            /* Start initial listener */

  pthread_t *thread = (pthread_t*)calloc(threads, sizeof(*thread));
  int i = 0;
  while (i < threads) {
    pthread_create(&thread[i++], NULL, user_thread, &g);
    pthread_create(&thread[i++], NULL, proactor_thread, &g);
  }
  millisleep(runtime*1000);
  debug(NULL, "shut down");
  global_set_shutdown(&g, true);
  pn_proactor_disconnect(g.proactor, NULL);
  for (i = 0; i < threads; ++i) {
    void *ignore;
    pthread_join(thread[i], &ignore);
  }
  free(thread);
  global_destroy(&g);
  printf("threaderciser actions: %zd\n", debug_count);
}
