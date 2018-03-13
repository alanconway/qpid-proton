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

/* Set of actions that can be enabled/disabled */
typedef enum { A_LISTEN, A_LCLOSE, A_CONNECT, A_CCLOSE, A_WAKE, A_TIMEOUT } action;
const char* action_name[] = { "listen", "lclose", "connect", "cclose", "wake", "timeout" };
#define action_count (sizeof(action_name)/sizeof(*action_name))
bool action_enabled[action_count] = { 0 } ;

static int assert_no_err(int n) { assert(n >= 0); return n; }

static bool debug_enable = false;

/*
   NOTE: We don't use any sync or atomics in debug() to avoid additional memory
   barriers that might mask race conditions. We print the message in a single
   call to fprintf to minimise intermingling.
*/
static void debug(const char *fmt, ...) {
  if (!debug_enable) return;
  char msg[256];
  char *i = msg;
  char *end = i + sizeof(msg);
  i += assert_no_err(snprintf(i, end-i, "(%lx) ", pthread_self()));
  if (i < end) {
    va_list ap;
    va_start(ap, fmt);
    i += assert_no_err(vsnprintf(i, end-i, fmt, ap));
    va_end(ap);
  }
  fprintf(stderr, "%s\n", msg);
}

static void debuga(action a, void *id) {
  debug("[%p] %s", id, action_name[a]);
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
  if (!action_enabled[A_CONNECT]) return;
  uconnection *u = uconnection_new();
  if (cpool_add(cp, u)) {
    debuga(A_CONNECT, u->pn_connection);
    pn_proactor_connect(proactor, u->pn_connection, addr);
  } else {
    uconnection_free(u);
  }
}

void cpool_wake(cpool *cp) {
  if (!action_enabled[A_WAKE]) return;
  uconnection *u = cpool_pick(cp);
  if (u) {
    debuga(A_WAKE, u->pn_connection);
    /* Required locking: the application may not call wake on a freed connection */
    pthread_mutex_lock(&u->lock);
    if (u && u->pn_connection) pn_connection_wake(u->pn_connection);
    pthread_mutex_unlock(&u->lock);
    cpool_unref(u);
  }
}

static void uconnection_on_close(uconnection *u) {
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
  if (!action_enabled[A_LISTEN]) return;
  char a[PN_MAX_ADDR];
  pn_proactor_addr(a, sizeof(a), "", "0");
  ulistener *u = ulistener_new();
  if (lpool_add(lp, u)) {
    debuga(A_LISTEN,  u->pn_listener);
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
  debug("[%p] listening on %s", u->pn_listener, u->addr);
}

static void ulistener_on_close(ulistener *u) {
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
  if (!action_enabled[A_LCLOSE]) return;
  ulistener *u = lpool_pick(lp);
  if (u) {
    debuga(A_LCLOSE, u->pn_listener);
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

void global_connect(global *g) {
  char a[PN_MAX_ADDR];
  lpool_addr(&g->listeners, a, sizeof(a));
  cpool_connect(&g->connections_active, g->proactor, a);
}

/* Return true with given probability */
static bool maybe(double probability) {
  if (probability == 1.0) return true;
  return rand() < (probability * RAND_MAX);
}

/* Run random activities that can be done from any thread. */
static void global_do_stuff(global *g) {
  if (global_get_shutdown(g)) return;
  if (maybe(0.5)) global_connect(g);
  if (maybe(0.3)) lpool_listen(&g->listeners, g->proactor);
  if (maybe(0.5)) cpool_wake(&g->connections_active);
  if (maybe(0.5)) cpool_wake(&g->connections_idle);
  if (maybe(0.1)) lpool_close(&g->listeners);
  if (action_enabled[A_TIMEOUT] && maybe(0.5)) {
    debuga(A_TIMEOUT, g->proactor);
    pn_proactor_set_timeout(g->proactor, rand() % TIMEOUT_MAX);
  }
}

static void* user_thread(void* void_g) {
  debug("user_thread start");
  global *g = (global*)void_g;
  while (!global_get_shutdown(g)) {
    global_do_stuff(g);
    millisleep(rand() % SLEEP_MAX);
  }
  debug("user_thread end");
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
     cpool_connect(&g->connections_active, g->proactor, u->addr); /* Initial connection */
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
  debug("proactor_thread start");
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
  debug("proactor_thread end");
  return NULL;
}

/* Command line arguments */

static const int default_runtime = 1;
static const int default_threads = 8;

void usage(const char **argv, const char **arg, const char **end) {
  fprintf(stderr, "usage: %s [options]\n", argv[0]);
  fprintf(stderr, "  -time TIME: total run-time in seconds (default %d)\n", default_runtime);
  fprintf(stderr, "  -threads THREADS: total number of threads (default %d)\n", default_threads);
  fprintf(stderr, "  -debug: print debug messages\n");
  fprintf(stderr, " ");
  for (int i = 0; i < (int)action_count; ++i) fprintf(stderr, " -%s", action_name[i]);
  fprintf(stderr, ": enable actions\n\n");

  fprintf(stderr, "bad argument: ");
  for (const char **a = argv+1; a < arg; ++a) fprintf(stderr, "%s ", *a);
  fprintf(stderr, ">>> %s <<<", *arg++);
  for (; arg < end; ++arg) fprintf(stderr, " %s", *arg);
  exit(1);
}

int main(int argc, const char* argv[]) {
  const char **arg = argv + 1;
  const char **end = argv + argc;
  int runtime = default_runtime;
  int threads = default_threads;

  while (arg < end) {
    if (!strcmp(*arg, "-time") && ++arg < end) {
      runtime = atoi(*arg);
      if (runtime <= 0) usage(argv, arg, end);
    }
    else if (!strcmp(*arg, "-threads") && ++arg < end) {
      threads = atoi(*arg);
      if (threads <= 0) usage(argv, arg, end);
      if (threads % 2) threads += 1; /* Round up to even: half proactor, half user */
    }
    else if (!strcmp(*arg, "-debug")) {
      debug_enable = true;
    }
    else if (**arg == '-') {
      size_t i = 0;
      while (i < action_count && strcmp((*arg)+1, action_name[i])) ++i;
      if (i == action_count) usage(argv, arg, end);
      action_enabled[i] = true;
    } else {
      break;
    }
    ++arg;
  }
  int i = 0;
  /* If no actions are requested on command line, enable them all */
  while (i < (int)action_count && !action_enabled[i]) ++i;
  if (i == action_count) {
    for (i = 0; i < (int)action_count; ++i) action_enabled[i] = true;
  }

  /* Set up global state, start threads */
  debug("threaderciser start threads=%d, time=%d\n", threads, runtime);
  global g;
  global_init(&g, threads);
  lpool_listen(&g.listeners, g.proactor); /* Start initial listener */

  pthread_t *user_threads = (pthread_t*)calloc(threads/2, sizeof(pthread_t));
  pthread_t *proactor_threads = (pthread_t*)calloc(threads/2, sizeof(pthread_t));
  for (i = 0; i < threads/2; ++i) {
    pthread_create(&user_threads[i], NULL, user_thread, &g);
    pthread_create(&proactor_threads[i], NULL, proactor_thread, &g);
  }
  millisleep(runtime*1000);

  debug("shut down");
  global_set_shutdown(&g, true);
  void *ignore;
  for (i = 0; i < threads/2; ++i) pthread_join(user_threads[i], &ignore);
  pn_proactor_disconnect(g.proactor, NULL);
  for (i = 0; i < threads/2; ++i) pthread_join(proactor_threads[i], &ignore);

  free(user_threads);
  free(proactor_threads);
  global_destroy(&g);
}
