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

#include "test_tools.h"
#include <proton/condition.h>
#include <proton/connection.h>
#include <proton/event.h>
#include <proton/listener.h>
#include <proton/proactor.h>
#include <proton/transport.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static pn_millis_t timeout = 7*1000; /* timeout for hanging tests */

static const char *localhost = "127.0.0.1"; /* host for connect/listen */

typedef pn_event_t *(*test_handler_fn)(test_t *, pn_event_t*);

/* Proactor and handler that take part in a test */
typedef struct proactor_test_t {
  test_handler_fn handler;
  test_t *t;
  pn_proactor_t *proactor;
  pn_event_batch_t *batch;      /* Batch in progress */
} proactor_test_t;


/* Initialize an array of proactor_test_t */
static void proactor_test_init(proactor_test_t *pts, size_t n, test_t *t) {
  for (proactor_test_t *pt = pts; pt < pts + n; ++pt) {
    if (!pt->t) pt->t = t;
    if (!pt->proactor) pt->proactor = pn_proactor();
    pn_proactor_set_timeout(pt->proactor, timeout);
  }
}

#define PROACTOR_TEST_INIT(A, T) proactor_test_init(A, sizeof(A)/sizeof(*A), (T))

static void proactor_test_free(proactor_test_t *pts, size_t n) {
  for (proactor_test_t *pt = pts; pt < pts + n; ++pt) {
    pn_proactor_free(pt->proactor);
  }
}

#define PROACTOR_TEST_FREE(A) proactor_test_free(A, sizeof(A)/sizeof(*A))

/* Process events on a proactor array until a handler returns an event, or
 * there are no more events available (returns NULL).
 */
static pn_event_t *proactor_test_get(proactor_test_t *pts, size_t n) {
  while (true) {
    bool busy = false;
    for (proactor_test_t *pt = pts; pt < pts + n; ++pt) {
      if (pt->batch == 0) {
        pt->batch = pn_proactor_get(pt->proactor);
      }
      if (pt->batch) {
        busy = true;
        pn_event_t *e;
        while ((e = pn_event_batch_next(pt->batch))) {
          pn_event_t *ret = pt->handler(pt->t, e);
          if (ret) return ret;
        }
        pn_proactor_done(pt->proactor, pt->batch);
        pt->batch = NULL;
      }
    }
    if (!busy) {
      return NULL;
    }
  }
}

/* Run an array of proactors till a handler returns an event. */
static pn_event_t *proactor_test_run(proactor_test_t *pts, size_t n) {
  pn_event_t *e;
  while ((e = proactor_test_get(pts, n)) == NULL)
         ;
  return e;
}


/* Drain and discard outstanding events from an array of proactors */
static void proactor_test_drain(proactor_test_t *pts, size_t n) {
  while (proactor_test_get(pts, n))
         ;
}


#define PROACTOR_TEST_GET(A) proactor_test_get((A), sizeof(A)/sizeof(*A))
#define PROACTOR_TEST_RUN(A) proactor_test_run((A), sizeof(A)/sizeof(*A))
#define PROACTOR_TEST_DRAIN(A) proactor_test_drain((A), sizeof(A)/sizeof(*A))

/* Wait for the next single event, return its type */
static pn_event_type_t wait_next(pn_proactor_t *proactor) {
  pn_event_batch_t *events = pn_proactor_wait(proactor);
  pn_event_type_t etype = pn_event_type(pn_event_batch_next(events));
  pn_proactor_done(proactor, events);
  return etype;
}

/* Test that interrupt and timeout events cause pn_proactor_wait() to return. */
static void test_interrupt_timeout(test_t *t) {
  pn_proactor_t *p = pn_proactor();
  TEST_CHECK(t, pn_proactor_get(p) == NULL); /* idle */
  pn_proactor_interrupt(p);
  TEST_ETYPE_EQUAL(t, PN_PROACTOR_INTERRUPT, wait_next(p));
  TEST_CHECK(t, pn_proactor_get(p) == NULL); /* idle */
  pn_proactor_set_timeout(p, 1); /* very short timeout */
  TEST_ETYPE_EQUAL(t, PN_PROACTOR_TIMEOUT, wait_next(p));
  pn_proactor_free(p);
}

/* Common handler for simple client/server interactions,  */
static pn_event_t *common_handler(test_t *t, pn_event_t *e) {
  pn_connection_t *c = pn_event_connection(e);
  pn_listener_t *l = pn_event_listener(e);

  switch (pn_event_type(e)) {

    /* Stop on these events */
   case PN_LISTENER_CLOSE:
   case PN_LISTENER_OPEN:
   case PN_PROACTOR_INACTIVE:
   case PN_PROACTOR_TIMEOUT:
   case PN_TRANSPORT_CLOSED:
    return e;

   case PN_LISTENER_ACCEPT:
    pn_listener_accept(l, pn_connection());
    return NULL;

   case PN_CONNECTION_REMOTE_OPEN:
    pn_connection_open(c);      /* Return the open (no-op if already open) */
    return NULL;

   case PN_CONNECTION_REMOTE_CLOSE:
    pn_connection_close(c);     /* Return the close */
    return NULL;

    /* Ignored these events */
   case PN_CONNECTION_INIT:
   case PN_CONNECTION_BOUND:
   case PN_CONNECTION_LOCAL_OPEN:
   case PN_CONNECTION_LOCAL_CLOSE:
   case PN_TRANSPORT:
   case PN_TRANSPORT_ERROR:
   case PN_TRANSPORT_HEAD_CLOSED:
   case PN_TRANSPORT_TAIL_CLOSED:
    return NULL;

   default:
    TEST_ERRORF(t, "unexpected event %s", pn_event_type_name(pn_event_type(e)));
    return NULL;                   /* Fail the test but keep going */
  }
}

/* close a connection when it is remote open */
static pn_event_t *open_close_handler(test_t *t, pn_event_t *e) {
  switch (pn_event_type(e)) {
   case PN_CONNECTION_REMOTE_OPEN:
    pn_connection_close(pn_event_connection(e));
    return NULL;          /* common_handler will finish on TRANSPORT_CLOSED */
   default:
    return common_handler(t, e);
  }
}

/* Simple client/server connection with 2 proactors */
static void test_client_server(test_t *t) {
  proactor_test_t pts[] ={ { open_close_handler }, { common_handler } };
  PROACTOR_TEST_INIT(pts, t);
  pn_proactor_t *client = pts[0].proactor, *server = pts[1].proactor;
  test_port_t port = test_port(localhost);
  pn_proactor_listen(server, pn_listener(), port.host_port, 4);
  TEST_EVENT_TYPE(t, PN_LISTENER_OPEN, PROACTOR_TEST_RUN(pts));
  pn_proactor_connect(client, pn_connection(), port.host_port);
  TEST_EVENT_TYPE(t, PN_TRANSPORT_CLOSED, PROACTOR_TEST_RUN(pts));
  sock_close(port.sock);
  PROACTOR_TEST_FREE(pts);
}

/* Return on connection open, close and return on wake */
static pn_event_t *open_wake_handler(test_t *t, pn_event_t *e) {
  switch (pn_event_type(e)) {
   case PN_CONNECTION_REMOTE_OPEN:
    return e;
   case PN_CONNECTION_WAKE:
    pn_connection_close(pn_event_connection(e));
    return e;
   default:
    return common_handler(t, e);
  }
}

/* Test waking up a connection that is idle */
static void test_connection_wake(test_t *t) {
  proactor_test_t pts[] =  { { open_wake_handler }, { common_handler } };
  PROACTOR_TEST_INIT(pts, t);
  pn_proactor_t *client = pts[0].proactor, *server = pts[1].proactor;
  test_port_t port = test_port(localhost);          /* Hold a port */
  pn_proactor_listen(server, pn_listener(), port.host_port, 4);
  TEST_EVENT_TYPE(t, PN_LISTENER_OPEN, PROACTOR_TEST_RUN(pts));
  pn_connection_t *c = pn_connection();
  pn_proactor_connect(client, c, port.host_port);
  TEST_EVENT_TYPE(t, PN_CONNECTION_REMOTE_OPEN, PROACTOR_TEST_RUN(pts));
  TEST_CHECK(t, pn_proactor_get(client) == NULL); /* Should be idle */
  pn_connection_wake(c);
  TEST_EVENT_TYPE(t, PN_CONNECTION_WAKE, PROACTOR_TEST_RUN(pts));
  TEST_EVENT_TYPE(t, PN_TRANSPORT_CLOSED, PROACTOR_TEST_RUN(pts));
  sock_close(port.sock);
  PROACTOR_TEST_FREE(pts);
}

/* Test that INACTIVE event is generated when last connections/listeners closes. */
static void test_inactive(test_t *t) {
  proactor_test_t pts[] =  { { open_wake_handler }, { common_handler } };
  PROACTOR_TEST_INIT(pts, t);
  pn_proactor_t *client = pts[0].proactor, *server = pts[1].proactor;
  test_port_t port = test_port(localhost);          /* Hold a port */

  pn_listener_t *l = pn_listener();
  pn_proactor_listen(server, l, port.host_port,  4);
  TEST_EVENT_TYPE(t, PN_LISTENER_OPEN, PROACTOR_TEST_RUN(pts));
  pn_connection_t *c = pn_connection();
  pn_proactor_connect(client, c, port.host_port);
  TEST_EVENT_TYPE(t, PN_CONNECTION_REMOTE_OPEN, PROACTOR_TEST_RUN(pts));
  pn_connection_wake(c);
  TEST_EVENT_TYPE(t, PN_CONNECTION_WAKE, PROACTOR_TEST_RUN(pts));
  /* expect TRANSPORT_CLOSED from client and server, INACTIVE from client */
  TEST_EVENT_TYPE(t, PN_TRANSPORT_CLOSED, PROACTOR_TEST_RUN(pts));
  TEST_EVENT_TYPE(t, PN_TRANSPORT_CLOSED, PROACTOR_TEST_RUN(pts));
  TEST_EVENT_TYPE(t, PN_PROACTOR_INACTIVE, PROACTOR_TEST_RUN(pts));
  /* server won't be INACTIVE until listener is closed */
  TEST_CHECK(t, pn_proactor_get(server) == NULL);
  pn_listener_close(l);
  TEST_EVENT_TYPE(t, PN_LISTENER_CLOSE, PROACTOR_TEST_RUN(pts));
  TEST_EVENT_TYPE(t, PN_PROACTOR_INACTIVE, PROACTOR_TEST_RUN(pts));

  sock_close(port.sock);
  PROACTOR_TEST_FREE(pts);
}

/* Tests for error handling */
static void test_errors(test_t *t) {
  proactor_test_t pts[] =  { { open_wake_handler }, { common_handler } };
  PROACTOR_TEST_INIT(pts, t);
  pn_proactor_t *client = pts[0].proactor, *server = pts[1].proactor;
  test_port_t port = test_port(localhost);          /* Hold a port */

  /* Invalid connect/listen parameters */
  pn_connection_t *c = pn_connection();
  pn_proactor_connect(client, c, "127.0.0.1:xxx");
  TEST_EVENT_TYPE(t, PN_TRANSPORT_CLOSED, PROACTOR_TEST_RUN(pts));
  TEST_CHECK_COND(t, "xxx", pn_transport_condition(pn_connection_transport(c)));
  TEST_EVENT_TYPE(t, PN_PROACTOR_INACTIVE, PROACTOR_TEST_RUN(pts));

  pn_listener_t *l = pn_listener();
  pn_proactor_listen(server, l, "127.0.0.1:xxx", 1);
  TEST_EVENT_TYPE(t, PN_LISTENER_OPEN, PROACTOR_TEST_RUN(pts));
  TEST_EVENT_TYPE(t, PN_LISTENER_CLOSE, PROACTOR_TEST_RUN(pts));
  TEST_CHECK_COND(t, "xxx", pn_listener_condition(l));
  TEST_EVENT_TYPE(t, PN_PROACTOR_INACTIVE, PROACTOR_TEST_RUN(pts));

  /* Connect with no listener */
  c = pn_connection();
  pn_proactor_connect(client, c, port.host_port);
  if (TEST_EVENT_TYPE(t, PN_TRANSPORT_CLOSED, PROACTOR_TEST_RUN(pts))) {
    TEST_CHECK_COND(t, "connection refused", pn_transport_condition(pn_connection_transport(c)));
    TEST_EVENT_TYPE(t, PN_PROACTOR_INACTIVE, PROACTOR_TEST_RUN(pts));
    sock_close(port.sock);
    PROACTOR_TEST_FREE(pts);
  }
}

static inline const char *event_listener_desc(pn_event_t *e) {
  return pn_condition_get_description(pn_listener_condition(pn_event_listener(e)));
}

/* Test that we can control listen/select on ipv6/v4 and listen on both by default */
static void test_ipv4_ipv6(test_t *t) {
  proactor_test_t pts[] ={ { open_close_handler }, { common_handler } };
  PROACTOR_TEST_INIT(pts, t);
  pn_proactor_t *client = pts[0].proactor, *server = pts[1].proactor;

  /* Listen on all interfaces for IPv6 only. If this fails, skip IPv6 tests */
  test_port_t port6 = test_port("[::]");
  pn_proactor_listen(server, pn_listener(), port6.host_port, 4);
  TEST_EVENT_TYPE(t, PN_LISTENER_OPEN, PROACTOR_TEST_RUN(pts));
  sock_close(port6.sock);
  pn_event_t *e = PROACTOR_TEST_GET(pts);
  bool has_ipv6 = (pn_event_type(e) != PN_LISTENER_CLOSE);
  if (!has_ipv6) {
    TEST_LOGF(t, "skip IPv6 tests: %s", event_listener_desc(e));
  }
  PROACTOR_TEST_DRAIN(pts);

  /* Listen on all interfaces for IPv4 only. */
  test_port_t port4 = test_port("0.0.0.0");
  pn_proactor_listen(server, pn_listener(), port4.host_port, 4);
  TEST_EVENT_TYPE(t, PN_LISTENER_OPEN, PROACTOR_TEST_RUN(pts));
  sock_close(port4.sock);
  e = PROACTOR_TEST_GET(pts);
  if (pn_event_type(e) == PN_LISTENER_CLOSE) {
    TEST_ERRORF(t, "listener error: %s",  event_listener_desc(e));
  }
  PROACTOR_TEST_DRAIN(pts);

  /* Empty address listens on both IPv4 and IPv6 on all interfaces */
  test_port_t port = test_port("");
  pn_proactor_listen(server, pn_listener(), port.host_port, 4);
  TEST_EVENT_TYPE(t, PN_LISTENER_OPEN, PROACTOR_TEST_RUN(pts));
  sock_close(port.sock);
  e = PROACTOR_TEST_GET(pts);
  if (pn_event_type(e) == PN_LISTENER_CLOSE) {
    TEST_ERRORF(t, "listener error: %s",  event_listener_desc(e));
  }
  PROACTOR_TEST_DRAIN(pts);

#define EXPECT_CONNECT(TP, HOST) do {                                   \
    pn_proactor_connect(client, pn_connection(), test_port_use_host(&(TP), (HOST))); \
    pn_event_t *e = TEST_EVENT_TYPE(t, PN_TRANSPORT_CLOSED, PROACTOR_TEST_RUN(pts)); \
    if (e) TEST_CHECK_NO_COND(t, pn_transport_condition(pn_event_transport(e))); \
    PROACTOR_TEST_DRAIN(pts);                                           \
  } while(0)

#define EXPECT_FAIL(TP, HOST) do {                                      \
    pn_proactor_connect(client, pn_connection(), test_port_use_host(&(TP), (HOST))); \
    pn_event_t *e = TEST_EVENT_TYPE(t, PN_TRANSPORT_CLOSED, PROACTOR_TEST_RUN(pts)); \
    if (e) TEST_CHECK_COND(t, "refused", pn_transport_condition(pn_event_transport(e))); \
    PROACTOR_TEST_DRAIN(pts);                                           \
  } while(0)

  EXPECT_CONNECT(port4, "127.0.0.1"); /* v4->v4 */
  EXPECT_CONNECT(port4, "");          /* local->v4*/

  EXPECT_CONNECT(port, "127.0.0.1"); /* v4->all */
  EXPECT_CONNECT(port, "");          /* local->all */

  if (has_ipv6) {
    EXPECT_CONNECT(port6, "[::]"); /* v6->v6 */
    EXPECT_CONNECT(port6, "");     /* local->v6 */
    EXPECT_CONNECT(port, "[::1]"); /* v6->all */

    EXPECT_FAIL(port6, "127.0.0.1"); /* fail v4->v6 */
    EXPECT_FAIL(port4, "[::1]");     /* fail v6->v4 */
  }

  PROACTOR_TEST_FREE(pts);
}


int main(int argc, char **argv) {
  int failed = 0;
  RUN_ARGV_TEST(failed, t, test_inactive(&t));
  RUN_ARGV_TEST(failed, t, test_interrupt_timeout(&t));
  RUN_ARGV_TEST(failed, t, test_errors(&t));
  RUN_ARGV_TEST(failed, t, test_client_server(&t));
  RUN_ARGV_TEST(failed, t, test_connection_wake(&t));
  RUN_ARGV_TEST(failed, t, test_ipv4_ipv6(&t));
  return failed;
}
