/*
 *
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
 *
 */

#include "../core/log_private.h"
#include "../core/url-internal.h"

#include <proton/condition.h>
#include <proton/connection_driver.h>
#include <proton/engine.h>
#include <proton/listener.h>
#include <proton/message.h>
#include <proton/object.h>
#include <proton/proactor.h>
#include <proton/transport.h>

#include <uv.h>

/* All asserts are cheap and should remain in a release build for debugability */
#undef NDEBUG
#include <assert.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
  libuv functions are thread unsafe, we use a"leader-worker-follower" model as follows:

  - At most one thread at a time is the "leader". The leader runs the UV loop till there
  are events to process and then becomes a "worker"n

  - Concurrent "worker" threads process events for separate connections or listeners.
  When they run out of work they become "followers"

  - A "follower" is idle, waiting for work. When the leader becomes a worker, one follower
  takes over as the new leader.

  Any thread that calls pn_proactor_wait() or pn_proactor_get() can take on any of the
  roles as required at run-time. Monitored sockets (connections or listeners) are passed
  between threads on thread-safe queues.

  Function naming:
  - on_*() - libuv callbacks, called in leader thread via  uv_run().
  - leader_* - only called in leader thread from
  - *_lh - called with the relevant lock held
*/

const char *AMQP_PORT = "5672";
const char *AMQP_PORT_NAME = "amqp";
const char *AMQPS_PORT = "5671";
const char *AMQPS_PORT_NAME = "amqps";

PN_HANDLE(PN_PROACTOR)

/* pn_proactor_t and pn_listener_t are plain C structs with normal memory management.
   CLASSDEF is for identification when used as a pn_event_t context.
*/
PN_STRUCT_CLASSDEF(pn_proactor, CID_pn_proactor)
PN_STRUCT_CLASSDEF(pn_listener, CID_pn_listener)


/* ================ Queues ================ */
static int unqueued;            /* Provide invalid address for _unqueued pointers */

#define QUEUE_DECL(T)                                                   \
  typedef struct T##_queue_t { T##_t *front, *back; } T##_queue_t;      \
                                                                        \
  static T##_t *T##_unqueued = (T##_t*)&unqueued;                       \
                                                                        \
  static void T##_push(T##_queue_t *q, T##_t *x) {                      \
    assert(x->next == T##_unqueued);                                    \
    x->next = NULL;                                                     \
    if (!q->front) {                                                    \
      q->front = q->back = x;                                           \
    } else {                                                            \
      q->back->next = x;                                                \
      q->back =  x;                                                     \
    }                                                                   \
  }                                                                     \
                                                                        \
  static T##_t* T##_pop(T##_queue_t *q) {                               \
    T##_t *x = q->front;                                                \
    if (x) {                                                            \
      q->front = x->next;                                               \
      x->next = T##_unqueued;                                           \
    }                                                                   \
    return x;                                                           \
  }


/* All work structs and UV callback data structs start with a struct_type member  */
typedef enum { T_CONNECTION, T_LISTENER, T_LSOCKET } struct_type;

/* A stream of serialized work for the proactor */
typedef struct work_t {
  /* Immutable */
  struct_type type;
  pn_proactor_t *proactor;

  /* Protected by proactor.lock */
  struct work_t* next;
  bool working;                      /* Owned by a worker thread */
} work_t;

QUEUE_DECL(work)

static void work_init(work_t* w, pn_proactor_t* p, struct_type type) {
  w->proactor = p;
  w->next = work_unqueued;
  w->type = type;
  w->working = true;
}

/* ================ IO ================ */

#define MAXADDR (NI_MAXHOST+NI_MAXSERV)

/* A resolvable address */
typedef struct addr_t {
  char addr[MAXADDR];
  char *host, *port;            /* Point into addr after destructive pni_url_parse */
  uv_getaddrinfo_t getaddrinfo; /* UV getaddrinfo request, contains list of addrinfo */
  struct addrinfo* addrinfo;    /* The current addrinfo being tried */
} addr_t;

/* A single listening socket, a listener can have more than one */
typedef struct lsocket_t {
  struct_type type;             /* Always T_LSOCKET */
  pn_listener_t *parent;
  uv_tcp_t tcp;
} lsocket_t;

PN_STRUCT_CLASSDEF(lsocket, CID_pn_listener_socket)

/* An incoming or outgoing connection. */
typedef struct pconnection_t {
  work_t work;

  /* Only used by owner thread */
  pn_connection_driver_t driver;

  /* Only used by leader */
  uv_tcp_t tcp;
  addr_t addr;

  uv_connect_t connect;         /* Outgoing connection only */
  int connected;      /* 0: not connected, <0: connecting after error, 1 = connected ok */

  lsocket_t *lsocket;         /* Incoming connection only */

  uv_timer_t timer;
  uv_write_t write;
  size_t writing;               /* size of pending write request, 0 if none pending */
  uv_shutdown_t shutdown;

  /* Locked for thread-safe access */
  uv_mutex_t lock;
  bool wake;                    /* pn_connection_wake() was called */
} pconnection_t;


typedef enum { L_UNINIT, L_LISTENING, L_CLOSE, L_CLOSING, L_CLOSED } listener_state;
/* A listener */
struct pn_listener_t {
  work_t work;

  size_t nsockets;
  lsocket_t *sockets;
  lsocket_t prealloc[1];       /* Pre-allocated socket array, allocate larger if needed */

  /* Only used by owner thread */
  pn_event_batch_t batch;
  pn_record_t *attachments;
  void *context;
  size_t backlog;

  /* Only used by leader */
  addr_t addr;

  /* Locked for thread-safe access. uv_listen can't be stopped or cancelled so we can't
   * detach a listener from the UV loop to prevent concurrent access.
   */
  uv_mutex_t lock;
  pn_condition_t *condition;
  pn_collector_t *collector;
  work_queue_t accept;          /* pconnection_t list for accepting */
  listener_state state;
};

struct pn_proactor_t {
  /* Leader thread  */
  uv_cond_t cond;
  uv_loop_t loop;
  uv_async_t async;
  uv_timer_t timer;

  /* Owner thread: proactor collector and batch can belong to leader or a worker */
  pn_collector_t *collector;
  pn_event_batch_t batch;

  /* Protected by lock */
  uv_mutex_t lock;
  work_queue_t worker_q;               /* ready for work, to be returned via pn_proactor_wait()  */
  work_queue_t leader_q;               /* waiting for attention by the leader thread */
  size_t interrupt;             /* pending interrupts */
  pn_millis_t timeout;
  size_t count;                 /* connection/listener count for INACTIVE events */
  bool inactive;
  bool timeout_request;
  bool timeout_elapsed;
  bool has_leader;
  bool batch_working;          /* batch is being processed in a worker thread */
};


/* Notify the leader thread that there is something to do outside of uv_run() */
static inline void notify(pn_proactor_t* p) {
  uv_async_send(&p->async);
}

/* Notify that this work item needs attention from the leader at the next opportunity */
static void work_notify(work_t *w) {
  uv_mutex_lock(&w->proactor->lock);
  /* If the socket is in use by a worker or is already queued then leave it where it is.
     It will be processed in pn_proactor_done() or when the queue it is on is processed.
  */
  if (!w->working && w->next == work_unqueued) {
    work_push(&w->proactor->leader_q, w);
    notify(w->proactor);
  }
  uv_mutex_unlock(&w->proactor->lock);
}

/* Notify the leader of a newly-created work item */
static void work_start(work_t *w) {
  uv_mutex_lock(&w->proactor->lock);
  if (w->next == work_unqueued) {  /* No-op if already queued */
    w->working = false;
    work_push(&w->proactor->leader_q, w);
    notify(w->proactor);
    uv_mutex_unlock(&w->proactor->lock);
  }
}

static void parse_addr(addr_t *addr, const char *str) {
  strncpy(addr->addr, str, sizeof(addr->addr));
  char *scheme, *user, *pass, *path;
  pni_parse_url(addr->addr, &scheme, &user, &pass, &addr->host, &addr->port, &path);
}

static pconnection_t *pconnection(pn_proactor_t *p, pn_connection_t *c, bool server) {
  pconnection_t *pc = (pconnection_t*)calloc(1, sizeof(pconnection_t));
  if (!pc || pn_connection_driver_init(&pc->driver, c, NULL) != 0) {
    return NULL;
  }
  work_init(&pc->work, p,  T_CONNECTION);
  pc->write.data = &pc->work;
  if (server) {
    pn_transport_set_server(pc->driver.transport);
  }
  pn_record_t *r = pn_connection_attachments(pc->driver.connection);
  pn_record_def(r, PN_PROACTOR, PN_VOID);
  pn_record_set(r, PN_PROACTOR, pc);
  pc->addr.host = pc->addr.port = pc->addr.addr; /* Set host/port to "" by default */
  return pc;
}

static pn_event_t *listener_batch_next(pn_event_batch_t *batch);
static pn_event_t *proactor_batch_next(pn_event_batch_t *batch);

static inline pn_proactor_t *batch_proactor(pn_event_batch_t *batch) {
  return (batch->next_event == proactor_batch_next) ?
    (pn_proactor_t*)((char*)batch - offsetof(pn_proactor_t, batch)) : NULL;
}

static inline pn_listener_t *batch_listener(pn_event_batch_t *batch) {
  return (batch->next_event == listener_batch_next) ?
    (pn_listener_t*)((char*)batch - offsetof(pn_listener_t, batch)) : NULL;
}

static inline pconnection_t *batch_pconnection(pn_event_batch_t *batch) {
  pn_connection_driver_t *d = pn_event_batch_connection_driver(batch);
  return d ? (pconnection_t*)((char*)d - offsetof(pconnection_t, driver)) : NULL;
}

static inline work_t *batch_work(pn_event_batch_t *batch) {
  pconnection_t *pc = batch_pconnection(batch);
  if (pc) return &pc->work;
  pn_listener_t *l = batch_listener(batch);
  if (l) return &l->work;
  return NULL;
}

/* Total count of listener and connections for PN_PROACTOR_INACTIVE */
static void leader_count(pn_proactor_t *p, int change) {
  uv_mutex_lock(&p->lock);
  p->count += change;
  if (p->count == 0) {
    p->inactive = true;
    notify(p);
  }
  uv_mutex_unlock(&p->lock);
}

static void pconnection_free(pconnection_t *pc) {
  pn_connection_driver_destroy(&pc->driver);
  free(pc);
}

/* Final close event for for a pconnection_t, closes the timer */
static void on_close_pconnection_final(uv_handle_t *h) {
  pconnection_t *pc = (pconnection_t*)h->data;
  leader_count(pc->work.proactor, -1);
  pconnection_free(pc);
}

static void uv_safe_close(uv_handle_t *h, uv_close_cb cb) {
  if (!uv_is_closing(h)) {
    uv_close(h, cb);
  }
}

static void on_close_pconnection(uv_handle_t *h) {
  pconnection_t *pc = (pconnection_t*)h->data;
  /* Delay the free till the timer handle is also closed */
  uv_timer_stop(&pc->timer);
  uv_safe_close((uv_handle_t*)&pc->timer, on_close_pconnection_final);
}

static void on_close_lsocket(uv_handle_t *h) {
  lsocket_t* ls = (lsocket_t*)h->data;
  pn_listener_t *l = ls->parent;
  uv_mutex_lock(&l->lock);
  if (--l->nsockets == 0) {
    l->state = L_CLOSED;
    pn_collector_put(l->collector, pn_listener__class(), l, PN_LISTENER_CLOSE);
    work_notify(&l->work);
  }
  uv_mutex_unlock(&l->lock);
}

static pconnection_t *get_pconnection(pn_connection_t* c) {
  if (!c) {
    return NULL;
  }
  pn_record_t *r = pn_connection_attachments(c);
  return (pconnection_t*) pn_record_get(r, PN_PROACTOR);
}

static inline void pconnection_bad_connect(pconnection_t *pc, int err) {
  if (!pc->connected) {
    pc->connected = err;        /* Remember first connect error in case they all fail  */
  }
}

static void pconnection_error(pconnection_t *pc, int err, const char* what) {
  assert(err);
  pconnection_bad_connect(pc, err);
  pn_connection_driver_t *driver = &pc->driver;
  pn_connection_driver_bind(driver); /* Bind so errors will be reported */
  if (!pn_condition_is_set(pn_transport_condition(driver->transport))) {
    pn_connection_driver_errorf(driver, uv_err_name(err), "%s %s:%s: %s",
                                what, pc->addr.host, pc->addr.port,
                                uv_strerror(err));
  }
  pn_connection_driver_close(driver);
}

static void listener_close_lh(pn_listener_t* l) {
  if (l->state < L_CLOSE) {
    l->state = L_CLOSE;
    work_notify(&l->work);
  }
}

static void listener_error_lh(pn_listener_t *l, int err, const char* what) {
  assert(err);
  if (!pn_condition_is_set(l->condition)) {
    pn_condition_format(l->condition, uv_err_name(err), "%s %s:%s: %s",
                        what, l->addr.host, l->addr.port,
                        uv_strerror(err));
  }
  listener_close_lh(l);
}

static void listener_error(pn_listener_t *l, int err, const char* what) {
  uv_mutex_lock(&l->lock);
  listener_error_lh(l, err, what);
  uv_mutex_unlock(&l->lock);
}

static int pconnection_init(pconnection_t *pc) {
  int err = 0;
  err = uv_tcp_init(&pc->work.proactor->loop, &pc->tcp);
  if (!err) {
    pc->tcp.data = pc;
    pc->connect.data = pc;
    err = uv_timer_init(&pc->work.proactor->loop, &pc->timer);
    if (!err) {
      pc->timer.data = pc;
    } else {
      uv_close((uv_handle_t*)&pc->tcp, NULL);
    }
  }
  if (!err) {
    leader_count(pc->work.proactor, +1);
  } else {
    pconnection_error(pc, err, "initialization");
  }
  return err;
}

static void try_connect(pconnection_t *pc);

static void on_connect_fail(uv_handle_t *handle) {
  pconnection_t *pc = (pconnection_t*)handle->data;
  /* Create a new TCP socket, the current one is closed */
  int err = uv_tcp_init(&pc->work.proactor->loop, &pc->tcp);
  if (err) {
    pc->connected = err;
    pc->addr.addrinfo = NULL; /* No point in trying anymore, we can't create a socket */
  } else {
    try_connect(pc);
  }
}

/* Outgoing connection */
static void on_connect(uv_connect_t *connect, int err) {
  pconnection_t *pc = (pconnection_t*)connect->data;
  if (!err) {
    pc->connected = 1;
    pn_connection_open(pc->driver.connection);
    work_notify(&pc->work);
    uv_freeaddrinfo(pc->addr.getaddrinfo.addrinfo); /* Done with address info */
  } else {
    pconnection_bad_connect(pc, err);
    uv_close((uv_handle_t*)&pc->tcp, on_connect_fail); /* Try the next addr if there is one */
  }
}

/* Incoming connection ready to be accepted */
static void on_connection(uv_stream_t* server, int err) {
  /* Unlike most on_* functions, this can be called by the leader thread when the listener
   * is ON_WORKER or ON_LEADER, because
   *
   * 1. There's no way to stop libuv from calling on_connection().
   * 2. There can be multiple lsockets per listener.
   *
   * Update the state of the listener and queue it for leader attention.
   */
  lsocket_t *ls = (lsocket_t*)server->data;
  pn_listener_t *l = ls->parent;
  if (err) {
    listener_error(l, err, "on incoming connection");
  } else {
    uv_mutex_lock(&l->lock);
    pn_collector_put(l->collector, lsocket__class(), ls, PN_LISTENER_ACCEPT);
    uv_mutex_unlock(&l->lock);
  }
  work_notify(&l->work);
}

/* Common address resolution for leader_listen and leader_connect */
static int leader_resolve(pn_proactor_t *p, addr_t *addr, bool listen) {
  struct addrinfo hints = { 0 };
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  /* Note this looks contradictory since we disable V4 mapping in bind() but it is
     correct - read the getaddrinfo man page carefully!
  */
  hints.ai_flags = AI_V4MAPPED | AI_ADDRCONFIG;
  if (listen) {
    hints.ai_flags |= AI_PASSIVE | AI_ALL;
  }
  int err = uv_getaddrinfo(&p->loop, &addr->getaddrinfo, NULL,
                           *addr->host ? addr->host : NULL, addr->port, &hints);
  addr->addrinfo = addr->getaddrinfo.addrinfo; /* Start with the first addrinfo */
  return err;
}

/* Try to connect to the current addrinfo. Called by leader and via callbacks for retry.*/
static void try_connect(pconnection_t *pc) {
  struct addrinfo *ai = pc->addr.addrinfo;
  if (!ai) {                    /* End of list, connect fails */
    uv_freeaddrinfo(pc->addr.getaddrinfo.addrinfo);
    pconnection_bad_connect(pc, UV_EAI_NODATA);
    pconnection_error(pc, pc->connected, "connecting to");
    work_notify(&pc->work);
  } else {
    pc->addr.addrinfo = ai->ai_next; /* Advance for next attempt */
    int err = uv_tcp_connect(&pc->connect, &pc->tcp, ai->ai_addr, on_connect);
    if (err) {
      pconnection_bad_connect(pc, err);
      uv_close((uv_handle_t*)&pc->tcp, on_connect_fail); /* Queue up next attempt */
    }
  }
}

static bool leader_connect(pconnection_t *pc) {
  int err = pconnection_init(pc);
  if (!err) err = leader_resolve(pc->work.proactor, &pc->addr, false);
  if (err) {
    pconnection_error(pc, err, "connect resolving");
    return true;
  } else {
    try_connect(pc);
    return false;
  }
}

static int lsocket_init(lsocket_t *ls, pn_listener_t *l, struct addrinfo *ai) {
  ls->type = T_LSOCKET;
  ls->parent = l;
  ls->tcp.data = ls;
  int err = uv_tcp_init(&l->work.proactor->loop, &ls->tcp);
  if (!err) {
    int flags = (ai->ai_family == AF_INET6) ? UV_TCP_IPV6ONLY : 0;
    err = uv_tcp_bind(&ls->tcp, ai->ai_addr, flags);
    if (!err) err = uv_listen((uv_stream_t*)&ls->tcp, l->backlog, on_connection);
    if (err) uv_close((uv_handle_t*)&ls->tcp, NULL);
  }
  return err;
}

#define ARRAY_LEN(A) (sizeof(A)/sizeof(*(A)))

/* Listen on all available addresses */
static void leader_listen_lh(pn_listener_t *l) {
  leader_count(l->work.proactor, +1);
  int err = leader_resolve(l->work.proactor, &l->addr, true);
  if (!err) {
    /* Count addresses, allocate enough space */
    size_t len = 0;
    for (struct addrinfo *ai = l->addr.getaddrinfo.addrinfo; ai; ai = ai->ai_next) {
      ++len;
    }
    assert(len > 0);            /* Guaranteed by getaddrinfo() */
    l->sockets = (len > ARRAY_LEN(l->prealloc)) ? (lsocket_t*)calloc(len, sizeof(lsocket_t)) : l->prealloc;
    /* Find the working addresses */
    l->nsockets = 0;
    int first_err = 0;
    for (struct addrinfo *ai = l->addr.getaddrinfo.addrinfo; ai; ai = ai->ai_next) {
      lsocket_t *ls = &l->sockets[l->nsockets];
      int err2 = lsocket_init(ls, l, ai);
      if (!err2) {
        ++l->nsockets;                    /* Next socket */
      } else if (!first_err) {
        first_err = err2;
      }
    }
    uv_freeaddrinfo(l->addr.getaddrinfo.addrinfo);
    if (l->nsockets == 0) err = first_err;
  }
  /* Always put an OPEN event for symmetry, even if we immediately close with err */
  pn_collector_put(l->collector, pn_listener__class(), l, PN_LISTENER_OPEN);
  if (err) {
    listener_error_lh(l, err, "listening on");
  }
}

static void pn_listener_free(pn_listener_t *l) {
  if (l) {
    if (l->collector) pn_collector_free(l->collector);
    if (l->condition) pn_condition_free(l->condition);
    if (l->attachments) pn_free(l->attachments);
    if (l->sockets && l->sockets != l->prealloc) free(l->sockets);
    assert(!l->accept.front);
    free(l);
  }
}

/* Process a listener, return true if it has events for a worker thread */
static bool leader_process_listener(pn_listener_t *l) {
  /* NOTE: l may be concurrently accessed by on_connection() */
  uv_mutex_lock(&l->lock);
  bool finished = false;
  switch (l->state) {

   case L_UNINIT:
    l->state = L_LISTENING;
    leader_listen_lh(l);
    break;

   case L_LISTENING:
   case L_CLOSING:
    break;

   case L_CLOSE: {
     if (l->nsockets == 0) {    /* Close now */
       l->state = L_CLOSED;
       pn_collector_put(l->collector, pn_listener__class(), l, PN_LISTENER_CLOSE);
     } else {
       l->state = L_CLOSING;
       for (size_t i = 0; i < l->nsockets; ++i) { /* Will close on last socket */
         uv_close((uv_handle_t*)&l->sockets[i].tcp, on_close_lsocket);
       }
     }
     break;
   }

   case L_CLOSED: {
     if (!pn_collector_peek(l->collector) && !l->accept.front) {
       leader_count(l->work.proactor, -1);
       finished = true;
     }
     break;
   }
  }
  uv_mutex_unlock(&l->lock);

  if (finished) {
    pn_listener_free(l);
    return false;
  } else {
    /* Process accepted connections if any */
    pconnection_t *pc;
    while ((pc = (pconnection_t*)work_pop(&l->accept))) {
      int err = pconnection_init(pc);
      if (!err) {
        err = uv_accept((uv_stream_t*)&pc->lsocket->tcp, (uv_stream_t*)&pc->tcp);
      } else {
        listener_error(l, err, "accepting from");
        pconnection_error(pc, err, "accepting from");
      }
      work_start(&pc->work);
    }
    uv_mutex_lock(&l->lock);
    bool has_work = pn_collector_peek(l->collector);
    uv_mutex_unlock(&l->lock);
    return has_work;
  }
}

/* Generate tick events and return millis till next tick or 0 if no tick is required */
static pn_millis_t leader_tick(pconnection_t *pc) {
  uint64_t now = uv_now(pc->timer.loop);
  uint64_t next = pn_transport_tick(pc->driver.transport, now);
  return next ? next - now : 0;
}

static void on_tick(uv_timer_t *timer) {
  pconnection_t *pc = (pconnection_t*)timer->data;
  leader_tick(pc);
  work_notify(&pc->work);
}

static void on_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  pconnection_t *pc = (pconnection_t*)stream->data;
  if (nread >= 0) {
    pn_connection_driver_read_done(&pc->driver, nread);
  } else if (nread == UV_EOF) { /* hangup */
    pn_connection_driver_read_close(&pc->driver);
  } else {
    pconnection_error(pc, nread, "on read from");
  }
  work_notify(&pc->work);
}

static void on_write(uv_write_t* write, int err) {
  pconnection_t *pc = (pconnection_t*)write->data;
  size_t size = pc->writing;
  pc->writing = 0;
  if (err) {
    pconnection_error(pc, err, "on write to");
  } else {
    pn_connection_driver_write_done(&pc->driver, size);
  }
  work_notify(&pc->work);
}

static void on_timeout(uv_timer_t *timer) {
  pn_proactor_t *p = (pn_proactor_t*)timer->data;
  uv_mutex_lock(&p->lock);
  p->timeout_elapsed = true;
  uv_mutex_unlock(&p->lock);
}

/* Read buffer allocation function for uv, just returns the transports read buffer. */
static void alloc_read_buffer(uv_handle_t* stream, size_t size, uv_buf_t* buf) {
  pconnection_t *pc = (pconnection_t*)stream->data;
  pn_rwbytes_t rbuf = pn_connection_driver_read_buffer(&pc->driver);
  *buf = uv_buf_init(rbuf.start, rbuf.size);
}

/* Set the event in the proactor's batch  */
static pn_event_batch_t *proactor_batch_lh(pn_proactor_t *p, pn_event_type_t t) {
  pn_collector_put(p->collector, pn_proactor__class(), p, t);
  p->batch_working = true;
  return &p->batch;
}

static void on_stopping(uv_handle_t* h, void* v) {
  /* Close all the TCP handles */
  if (h->type == UV_TCP) {
    switch (*(struct_type*)h->data) {
     case T_CONNECTION:
      uv_safe_close(h, on_close_pconnection);
     case T_LSOCKET:
      uv_safe_close(h, on_close_lsocket);
     default:
      break;
    }
  }
}

static pn_event_t *log_event(void* p, pn_event_t *e) {
  if (e) {
    pn_logf("[%p]:(%s)", (void*)p, pn_event_type_name(pn_event_type(e)));
  }
  return e;
}

static pn_event_t *listener_batch_next(pn_event_batch_t *batch) {
  pn_listener_t *l = batch_listener(batch);
  uv_mutex_lock(&l->lock);
  pn_event_t *e = pn_collector_next(l->collector);
  uv_mutex_unlock(&l->lock);
  return log_event(l, e);
}

static pn_event_t *proactor_batch_next(pn_event_batch_t *batch) {
  pn_proactor_t *p = batch_proactor(batch);
  assert(p->batch_working);
  return log_event(p, pn_collector_next(p->collector));
}

/* Return the next event batch or NULL if no events are available */
static pn_event_batch_t *get_batch_lh(pn_proactor_t *p) {
  if (!p->batch_working) {       /* Can generate proactor events */
    if (p->inactive) {
      p->inactive = false;
      return proactor_batch_lh(p, PN_PROACTOR_INACTIVE);
    }
    if (p->interrupt > 0) {
      --p->interrupt;
      return proactor_batch_lh(p, PN_PROACTOR_INTERRUPT);
    }
    if (p->timeout_elapsed) {
      p->timeout_elapsed = false;
      return proactor_batch_lh(p, PN_PROACTOR_TIMEOUT);
    }
  }
  for (work_t *w = work_pop(&p->worker_q); w; w = work_pop(&p->worker_q)) {
    assert(w->working);
    switch (w->type) {
     case T_CONNECTION:
      return &((pconnection_t*)w)->driver.batch;
     case T_LISTENER:
      return &((pn_listener_t*)w)->batch;
     default:
      break;
    }
  }
  return NULL;
}

/* Check and reset the wake flag */
static bool check_wake(pconnection_t *pc) {
  uv_mutex_lock(&pc->lock);
  bool wake = pc->wake;
  pc->wake = false;
  uv_mutex_unlock(&pc->lock);
  return wake;
}

/* Process a pconnection, return true if it has events for a worker thread */
static bool leader_process_pconnection(pconnection_t *pc) {
  /* Important to do the following steps in order */
  if (!pc->connected) {
    return leader_connect(pc);
  }
  if (pc->writing) {
    /* We can't do anything while a write request is pending */
    return false;
  }
  if (pn_connection_driver_finished(&pc->driver)) {
    /* Close if the connection is finished */
    uv_safe_close((uv_handle_t*)&pc->tcp, on_close_pconnection);
  } else {
    /* Check for events that can be generated without blocking for IO */
    if (check_wake(pc)) {
      pn_connection_t *c = pc->driver.connection;
      pn_collector_put(pn_connection_collector(c), PN_OBJECT, c, PN_CONNECTION_WAKE);
    }
    pn_millis_t next_tick = leader_tick(pc);
    pn_rwbytes_t rbuf = pn_connection_driver_read_buffer(&pc->driver);
    pn_bytes_t wbuf = pn_connection_driver_write_buffer(&pc->driver);

    /* If we still have no events, make async UV requests */
    if (!pn_connection_driver_has_event(&pc->driver)) {
      int err = 0;
      const char *what = NULL;
      if (!err && next_tick) {
        what = "connection timer start";
        err = uv_timer_start(&pc->timer, on_tick, next_tick, 0);
      }
      if (!err) {
        what = "write";
        if (wbuf.size > 0) {
          uv_buf_t buf = uv_buf_init((char*)wbuf.start, wbuf.size);
          err = uv_write(&pc->write, (uv_stream_t*)&pc->tcp, &buf, 1, on_write);
          if (!err) {
            pc->writing = wbuf.size;
          }
        } else if (pn_connection_driver_write_closed(&pc->driver)) {
          uv_shutdown(&pc->shutdown, (uv_stream_t*)&pc->tcp, NULL);
        }
      }
      if (!err && rbuf.size > 0) {
        what = "read";
        err = uv_read_start((uv_stream_t*)&pc->tcp, alloc_read_buffer, on_read);
      }
      if (err) {
        /* Some IO requests failed, generate the error events */
        pconnection_error(pc, err, what);
      }
    }
  }
  return pn_connection_driver_has_event(&pc->driver);
}

/* Detach a connection from the UV loop so it can be used safely by a worker */
void pconnection_detach(pconnection_t *pc) {
  if (pc->connected && !pc->writing) {           /* Can't detach while a write is pending */
    uv_read_stop((uv_stream_t*)&pc->tcp);
    uv_timer_stop(&pc->timer);
  }
}

/* Process the leader_q and the UV loop, in the leader thread */
static pn_event_batch_t *leader_lead_lh(pn_proactor_t *p, uv_run_mode mode) {
  pn_event_batch_t *batch = NULL;
  for (work_t *w = work_pop(&p->leader_q); w; w = work_pop(&p->leader_q)) {
    assert(!w->working);

    uv_mutex_unlock(&p->lock);  /* Unlock to process each item, may add more items to leader_q */
    bool has_work = false;
    switch (w->type) {
     case T_CONNECTION:
      has_work = leader_process_pconnection((pconnection_t*)w);
      break;
     case T_LISTENER:
      has_work = leader_process_listener((pn_listener_t*)w);
      break;
     default:
      break;
    }
    uv_mutex_lock(&p->lock);

    if (has_work && !w->working && w->next == work_unqueued) {
      if (w->type == T_CONNECTION) {
        pconnection_detach((pconnection_t*)w);
      }
      w->working = true;
      work_push(&p->worker_q, w);
    }
  }
  batch = get_batch_lh(p);      /* Check for work */
  if (!batch) { /* No work, run the UV loop */
    /* Set timeout timer before uv_run */
    if (p->timeout_request) {
      p->timeout_request = false;
      uv_timer_stop(&p->timer);
      if (p->timeout) {
        uv_timer_start(&p->timer, on_timeout, p->timeout, 0);
      }
    }
    uv_mutex_unlock(&p->lock);  /* Unlock to run UV loop */
    uv_run(&p->loop, mode);
    uv_mutex_lock(&p->lock);
    batch = get_batch_lh(p);
  }
  return batch;
}

/**** public API ****/

pn_event_batch_t *pn_proactor_get(struct pn_proactor_t* p) {
  uv_mutex_lock(&p->lock);
  pn_event_batch_t *batch = get_batch_lh(p);
  if (batch == NULL && !p->has_leader) {
    /* Try a non-blocking lead to generate some work */
    p->has_leader = true;
    batch = leader_lead_lh(p, UV_RUN_NOWAIT);
    p->has_leader = false;
    uv_cond_broadcast(&p->cond);   /* Signal followers for possible work */
  }
  uv_mutex_unlock(&p->lock);
  return batch;
}

pn_event_batch_t *pn_proactor_wait(struct pn_proactor_t* p) {
  uv_mutex_lock(&p->lock);
  pn_event_batch_t *batch = get_batch_lh(p);
  while (!batch && p->has_leader) {
    uv_cond_wait(&p->cond, &p->lock); /* Follow the leader */
    batch = get_batch_lh(p);
  }
  if (!batch) {                 /* Become leader */
    p->has_leader = true;
    do {
      batch = leader_lead_lh(p, UV_RUN_ONCE);
    } while (!batch);
    p->has_leader = false;
    uv_cond_broadcast(&p->cond); /* Signal a followers. One takes over, many can work. */
  }
  uv_mutex_unlock(&p->lock);
  return batch;
}

void pn_proactor_done(pn_proactor_t *p, pn_event_batch_t *batch) {
  uv_mutex_lock(&p->lock);
  work_t *w = batch_work(batch);
  if (w) {
    assert(w->working);
    assert(w->next == work_unqueued);
    w->working = false;
    work_push(&p->leader_q, w);
  }
  pn_proactor_t *bp = batch_proactor(batch); /* Proactor events */
  if (bp == p) {
    p->batch_working = false;
  }
  uv_mutex_unlock(&p->lock);
  notify(p);
}

pn_listener_t *pn_event_listener(pn_event_t *e) {
  if (pn_event_class(e) == pn_listener__class()) {
    return (pn_listener_t*)pn_event_context(e);
  } else if (pn_event_class(e) == lsocket__class()) {
    return ((lsocket_t*)pn_event_context(e))->parent;
  } else {
    return NULL;
  }
}

pn_proactor_t *pn_event_proactor(pn_event_t *e) {
  if (pn_event_class(e) == pn_proactor__class()) {
    return (pn_proactor_t*)pn_event_context(e);
  }
  pn_listener_t *l = pn_event_listener(e);
  if (l) {
    return l->work.proactor;
  }
  pn_connection_t *c = pn_event_connection(e);
  if (c) {
    return pn_connection_proactor(pn_event_connection(e));
  }
  return NULL;
}

void pn_proactor_interrupt(pn_proactor_t *p) {
  uv_mutex_lock(&p->lock);
  ++p->interrupt;
  uv_mutex_unlock(&p->lock);
  notify(p);
}

void pn_proactor_set_timeout(pn_proactor_t *p, pn_millis_t t) {
  uv_mutex_lock(&p->lock);
  p->timeout = t;
  p->timeout_request = true;
  uv_mutex_unlock(&p->lock);
  notify(p);
}

int pn_proactor_connect(pn_proactor_t *p, pn_connection_t *c, const char *addr) {
  pconnection_t *pc = pconnection(p, c, false);
  if (pc) {
    parse_addr(&pc->addr, addr);
    work_start(&pc->work);
  } else {
    return PN_OUT_OF_MEMORY;
  }
  return 0;
}

int pn_proactor_listen(pn_proactor_t *p, pn_listener_t *l, const char *addr, int backlog) {
  work_init(&l->work, p, T_LISTENER);
  parse_addr(&l->addr, addr);
  l->backlog = backlog;
  work_start(&l->work);
  return 0;
}

pn_proactor_t *pn_proactor() {
  pn_proactor_t *p = (pn_proactor_t*)calloc(1, sizeof(*p));
  p->collector = pn_collector();
  p->batch.next_event = &proactor_batch_next;
  if (!p->collector) return NULL;
  uv_loop_init(&p->loop);
  uv_mutex_init(&p->lock);
  uv_cond_init(&p->cond);
  uv_async_init(&p->loop, &p->async, NULL);
  uv_timer_init(&p->loop, &p->timer);
  p->timer.data = p;
  return p;
}

void pn_proactor_free(pn_proactor_t *p) {
  uv_timer_stop(&p->timer);
  uv_safe_close((uv_handle_t*)&p->timer, NULL);
  uv_safe_close((uv_handle_t*)&p->async, NULL);
  uv_walk(&p->loop, on_stopping, NULL); /* Close all TCP handles */
  while (uv_loop_alive(&p->loop)) {
    uv_run(&p->loop, UV_RUN_ONCE);          /* Execute a UV close event */
    pn_event_batch_t *batch;
    while ((batch = pn_proactor_get(p))) { /* Drain any resulting proactor events */
      while (pn_event_batch_next(batch))
        ;
      pn_proactor_done(p, batch);
    }
  }
  uv_loop_close(&p->loop);
  uv_mutex_destroy(&p->lock);
  uv_cond_destroy(&p->cond);
  pn_collector_free(p->collector);
  free(p);
}

pn_proactor_t *pn_connection_proactor(pn_connection_t* c) {
  pconnection_t *pc = get_pconnection(c);
  return pc ? pc->work.proactor : NULL;
}

void pn_connection_wake(pn_connection_t* c) {
  /* May be called from any thread */
  pconnection_t *pc = get_pconnection(c);
  if (pc) {
    uv_mutex_lock(&pc->lock);
    pc->wake = true;
    uv_mutex_unlock(&pc->lock);
    work_notify(&pc->work);
  }
}

pn_listener_t *pn_listener(void) {
  pn_listener_t *l = (pn_listener_t*)calloc(1, sizeof(pn_listener_t));
  if (l) {
    l->batch.next_event = listener_batch_next;
    l->collector = pn_collector();
    l->condition = pn_condition();
    l->attachments = pn_record();
    if (!l->condition || !l->collector || !l->attachments) {
      pn_listener_free(l);
      return NULL;
    }
  }
  return l;
}

void pn_listener_close(pn_listener_t* l) {
  /* May be called from any thread */
  uv_mutex_lock(&l->lock);
  listener_close_lh(l);
  uv_mutex_unlock(&l->lock);
}

pn_proactor_t *pn_listener_proactor(pn_listener_t* l) {
  return l ? l->work.proactor : NULL;
}

pn_condition_t* pn_listener_condition(pn_listener_t* l) {
  return l->condition;
}

void *pn_listener_get_context(pn_listener_t *l) {
  return l->context;
}

void pn_listener_set_context(pn_listener_t *l, void *context) {
  l->context = context;
}

pn_record_t *pn_listener_attachments(pn_listener_t *l) {
  return l->attachments;
}

int pn_listener_accept(pn_listener_t *l, pn_connection_t *c) {
  uv_mutex_lock(&l->lock);
  pconnection_t *pc = pconnection(l->work.proactor, c, true);
  if (!pc) {
    return PN_OUT_OF_MEMORY;
  }
  /* Get the socket from the accept event that we are processing */
  pn_event_t *e = pn_collector_prev(l->collector);
  assert(pn_event_type(e) == PN_LISTENER_ACCEPT);
  assert(pn_event_listener(e) == l);
  pc->lsocket = (lsocket_t*)pn_event_context(e);
  pc->connected = 1;            /* Don't need to connect() */
  work_push(&l->accept, &pc->work);
  uv_mutex_unlock(&l->lock);
  work_notify(&l->work);
  return 0;
}
