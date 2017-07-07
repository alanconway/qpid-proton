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

/*
   An intermediary that "streams" messages from senders to receivers.

   This example demonstrates how an intermediary can forward large messages
   without holding the entire message in memory.
*/

#include "thread.h"

#include <proton/engine.h>
#include <proton/listener.h>
#include <proton/proactor.h>
#include <proton/sasl.h>
#include <proton/transport.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Use small limits so we can see buffering effects with modest-sized data. */

/* Note the default frame max for proton is "unlimited", you must set the max
   frame size *and* the session capacity to get a finite incoming session buffer.
*/
#define FRAME 16*1024           /* Max frame size */
#define INCOMING_SESSION_CAP 16*FRAME /* Session buffer limit for session flow control */
#define OUTGOING_SESSION_CAP 16*FRAME /* Many endpoints won't set a limit, no more than this */
#define BUFFER 4*FRAME           /* Internal buffer used to transfer messages */
#define FLOW_WINDOW 100          /* Message-count flow control */

/* Generate unique delivery tags  */
static int global_dtag = 0;
pthread_mutex_t dtag_lock = PTHREAD_MUTEX_INITIALIZER; 

/* Each "address" can be used by at most one sender and one receiver at a time. */
typedef struct address_t {
  char name[256];               /* Used as the AMQP address */
  pn_link_t *sender, *receiver;
  bool sender_blocked, receiver_blocked; /* Need wakeup for sender/receiver */
  pthread_mutex_t lock;         /* Address is used concurrently by sender/receiver */
  char data[BUFFER];
  size_t size;                  /* Size of data in buffer */
  bool started;                 /* Initial credit has been issued. */
  bool end_of_message;          /* Final data received for current message, but not yet sent. */
  size_t session_out;           /* Outgoing session limit */

  struct address_t *next;
} address_t;

static void address_init(address_t *a, const char* name, address_t *next) {
  memset(a, 0, sizeof(*a));
  pthread_mutex_init(&a->lock, NULL);
  strncpy(a->name, name, sizeof(a->name));
  a->sender = a->receiver = NULL;
  a->next = next;
}

static void address_destroy(address_t *a) {
  pthread_mutex_destroy(&a->lock);
}

/* Thread safe set of addresses */
typedef struct addresses_t {
  pthread_mutex_t lock;
  address_t *addresses;
} addresses_t;

void addresses_init(addresses_t *addresses) {
  pthread_mutex_init(&addresses->lock, NULL);
  addresses->addresses = NULL;
}

void addresses_destroy(addresses_t *addresses) {
  while (addresses->addresses) {
    address_t *a = addresses->addresses;
    addresses->addresses = a->next;
    address_destroy(a);
    free(a);
  }
  pthread_mutex_destroy(&addresses->lock);
}

/** Get or create the named address. */
address_t* addresses_get(addresses_t *addresses, const char* name) {
  pthread_mutex_lock(&addresses->lock);
  address_t *a;
  for (a = addresses->addresses; a && strcmp(a->name, name) != 0; a = a->next)
    ;
  if (!a) {
    a = (address_t*)calloc(1, sizeof(*a));
    address_init(a, name, addresses->addresses);
    addresses->addresses = a;
  }
  pthread_mutex_unlock(&addresses->lock);
  return a;
}

/* Server state */
typedef struct streamer_t {
  pn_proactor_t *proactor;
  size_t threads;
  const char *container_id;     /* AMQP container-id */
  addresses_t addresses;
  bool finished;
} streamer_t;

static int exit_code = 0;

void streamer_stop(streamer_t *streamer) {
  /* Interrupt the proactor to stop the working threads. */
  streamer->finished = true;
  pn_proactor_interrupt(streamer->proactor);
}

#define fatal(...) do {                                 \
    fprintf(stderr, "%s:%d: ", __FILE__, __LINE__);     \
    fprintf(stderr, __VA_ARGS__);                       \
    fprintf(stderr, "\n");                              \
  } while(0)

static address_t *link_address(pn_link_t *l) {
  address_t *a = (address_t*)pn_link_get_context(l);
  if (!a) fatal("unkown link");
  return a;
}

#define check_condition(e, cond) do {                                   \
    if (pn_condition_is_set(cond)) {                                    \
      fatal("%s: %s: %s\n", pn_event_type_name(pn_event_type(e)),       \
            pn_condition_get_name(cond), pn_condition_get_description(cond)); \
    }                                                                   \
  } while(0)

static void link_down(pn_link_t *l) {
  address_t *a = link_address(l);
  pthread_mutex_lock(&a->lock);
  if (pn_link_is_sender(l)) {
    a->sender = NULL;
  } else {
    a->receiver = NULL;
    a->started = false;
  }
  pthread_mutex_unlock(&a->lock);
  pn_link_close(l);
  pn_link_free(l);
}

static inline size_t min(size_t a, size_t b) { return a < b ? a : b; }

static inline size_t buffer_free(size_t size, size_t max) { return max > size ? max - size : 0; }

/* Receive data from current message if we can, if not the sender will WAKE us when we can */
static void address_receive(address_t *a) {
  pn_delivery_t *d = pn_link_current(a->receiver);
  pthread_mutex_lock(&a->lock);
  if (!a->started) {
    pn_link_flow(a->receiver, FLOW_WINDOW);
    a->started = true;
  } else if (d && pn_delivery_readable(d) && !a->end_of_message) {
      bool wake = false;
      while (pn_delivery_pending(d) > 0 && a->size < BUFFER) {
        size_t size = min(BUFFER - a->size, pn_delivery_pending(d));
        ssize_t recv = pn_link_recv(a->receiver, a->data + a->size, size);
        if (recv >= 0) {
          a->size += recv;
          wake = recv;
        } else {
          fatal("pn_link_recv %s: %s" , a->name, pn_code(recv));
        }
      }
      if (pn_delivery_pending(d) == 0) {
        if (!pn_delivery_partial(d)) { /* End of message */
          pn_link_advance(a->receiver);
          pn_delivery_update(d, PN_ACCEPTED);
          pn_delivery_settle(d);
          pn_link_flow(a->receiver, 1);
          a->end_of_message = true;
          wake = true;
        }
      } else {                  /* No buffer space, need wakeup when there is some */
        a->receiver_blocked = true;
      }
      if (wake && a->sender_blocked) {
        a->sender_blocked = false;
        pn_connection_wake(pn_session_connection(pn_link_session(a->sender)));
      }
  }
  pthread_mutex_unlock(&a->lock);
}

static void address_send(address_t *a) {
  pn_delivery_t *d = pn_link_current(a->sender);
  bool wake = false;

  pthread_mutex_lock(&a->lock);
  if (a->size) {       /* There is something to send */
    /* Start a new delivery if there isn't one in progress */
    if (!d) {
      pthread_mutex_lock(&dtag_lock);
      int dtag = global_dtag++;
      pthread_mutex_unlock(&dtag_lock);
      d = pn_delivery(a->sender, pn_dtag((char*)&dtag, sizeof(dtag)));
    }
    /* Send as much as possible without exceeding outgoing session limit */
    size_t pending = pn_session_outgoing_bytes(pn_link_session(a->sender));
    ssize_t size = min(buffer_free(pending, a->session_out), a->size);
    ssize_t sent = pn_link_send(a->sender, a->data, size);
    if (sent >= 0) {
      memmove(a->data, a->data + sent, a->size - sent);
      a->size -= sent;
      wake = sent;
    } else {
      fatal("pn_link_send %s: %s" , a->name, pn_code(sent));
    }
  } else {                      /* Nothing to send, need wakeup when there is data */
    a->sender_blocked = true;
  }
  /* Check if the message is complete */
  if (a->end_of_message && a->size == 0) {
    pn_delivery_settle(d);
    a->end_of_message = false;
    wake = true;
  }
  if (wake && a->receiver_blocked) {
    a->receiver_blocked = false;
    pn_connection_wake(pn_session_connection(pn_link_session(a->receiver)));
  }
  pthread_mutex_unlock(&a->lock);
}

static void handle(streamer_t* s, pn_event_t* e) {
  pn_connection_t *c = pn_event_connection(e);

  switch (pn_event_type(e)) {

   case PN_LISTENER_OPEN:
    fprintf(stderr, "listening\n");
    fflush(stdout);
    break;

   case PN_LISTENER_ACCEPT:
    pn_listener_accept(pn_event_listener(e), pn_connection());
    break;

   case PN_CONNECTION_INIT:
    pn_connection_set_container(c, s->container_id);
    break;

   case PN_CONNECTION_BOUND: {
     pn_transport_t *t = pn_connection_transport(c);
     /* Small frame size */
     pn_transport_set_max_frame(t, FRAME);
     /* Turn off security */
     pn_transport_require_auth(t, false);
     pn_sasl_allowed_mechs(pn_sasl(t), "ANONYMOUS");
     break;
   }
   case PN_CONNECTION_REMOTE_OPEN: {
     pn_connection_open(pn_event_connection(e)); /* Complete the open */
     break;
   }
   case PN_CONNECTION_WAKE: {
     /* Check all links to see if we can send or receive to/from their addresses */
     for (pn_link_t *l = pn_link_head(c, 0); l != NULL; l = pn_link_next(l, 0)) {
       address_t *a = link_address(l);
       if (a) {
         if (pn_link_is_receiver(l)) {
           address_receive(a);
         } else {
           address_send(a);
         }
       }
     }
     break;
   }

   case PN_SESSION_REMOTE_OPEN:
    pn_session_set_incoming_capacity(pn_event_session(e), INCOMING_SESSION_CAP);
    pn_session_open(pn_event_session(e));
    break;

   case PN_LINK_REMOTE_OPEN: {
     pn_link_t *l = pn_event_link(e);
     address_t *a = NULL;
     if (pn_link_is_sender(l)) {
       a = addresses_get(&s->addresses, pn_terminus_get_address(pn_link_remote_source(l)));
       if (a->sender) fatal("%s already has a sender", a->name);
       a->sender = l;
       size_t max_frame = pn_transport_get_max_frame(pn_event_transport(e));
       size_t window = pn_session_get_outgoing_window(pn_event_session(e));
       a->session_out = min(max_frame * window, OUTGOING_SESSION_CAP);
       fprintf(stderr, "outgoing session limit for %s: %luK (requested %luK)\n",
               a->name,
               (unsigned long)a->session_out/1024,
               (unsigned long) (max_frame * window)/1024);
     } else {
       a = addresses_get(&s->addresses, pn_terminus_get_address(pn_link_remote_target(l)));
       if (a->receiver) fatal("%s already has a receiver", a->name);
       a->receiver = l;
     }
     pn_link_set_context(l, a);
     pn_link_open(l);
     if (a->sender && a->receiver) {
       /* Wake the receiver to start the message flow. */
       pn_connection_wake(pn_session_connection(pn_link_session(a->receiver)));
     }
     break;
   }
   case PN_DELIVERY: {
       address_t *a = link_address(pn_event_link(e));
       if (a) address_receive(a);
    break;
   }
   case PN_LINK_FLOW: {
     address_t *a = link_address(pn_event_link(e));
     if (a) address_send(a);
    break;
   }
   case PN_TRANSPORT_CLOSED: 
    check_condition(e, pn_transport_condition(pn_event_transport(e)));
    for (pn_link_t *l = pn_link_head(pn_event_connection(e), 0); l != NULL; l = pn_link_next(l, 0))
      link_down(l);
    break;

   case PN_CONNECTION_REMOTE_CLOSE:
    check_condition(e, pn_connection_remote_condition(pn_event_connection(e)));
    pn_connection_close(pn_event_connection(e));
    break;

   case PN_SESSION_REMOTE_CLOSE:
    check_condition(e, pn_session_remote_condition(pn_event_session(e)));
    for (pn_link_t *l = pn_link_head(pn_event_connection(e), 0); l != NULL; l = pn_link_next(l, 0)) {
      if (pn_link_session(l) == pn_event_session(e))
        link_down(l);
    }
    pn_session_close(pn_event_session(e));
    pn_session_free(pn_event_session(e));
    break;

   case PN_LINK_REMOTE_CLOSE: {
     check_condition(e, pn_link_remote_condition(pn_event_link(e)));
     link_down(pn_event_link(e));
     break;
   }
   case PN_LISTENER_CLOSE:
    check_condition(e, pn_listener_condition(pn_event_listener(e)));
    break;

   case PN_PROACTOR_INACTIVE:   /* listener and all connections closed */
    streamer_stop(s);
    break;

   case PN_PROACTOR_INTERRUPT:
    pn_proactor_interrupt(s->proactor); /* Pass along the interrupt to the other threads */
    break;

   default:
    break;
  }
}

static void* streamer_thread(void *void_streamer) {
  streamer_t *streamer = (streamer_t*)void_streamer;
  do {
    pn_event_batch_t *events = pn_proactor_wait(streamer->proactor);
    pn_event_t *e;
    while ((e = pn_event_batch_next(events))) {
      handle(streamer, e);
    }
    pn_proactor_done(streamer->proactor, events);
  } while(!streamer->finished);
  return NULL;
}

int main(int argc, char **argv) {
  streamer_t streamer = {0};
  streamer.proactor = pn_proactor();
  addresses_init(&streamer.addresses);
  streamer.container_id = argv[0];
  streamer.threads = 4;
  int i = 1;
  const char *host = (argc > i) ? argv[i++] : "";
  const char *port = (argc > i) ? argv[i++] : "amqp";

  /* Listen on addr */
  char addr[PN_MAX_ADDR];
  pn_proactor_addr(addr, sizeof(addr), host, port);
  pn_proactor_listen(streamer.proactor, pn_listener(), addr, 16);

  /* Start n-1 threads */
  pthread_t* threads = (pthread_t*)calloc(sizeof(pthread_t), streamer.threads);
  for (size_t i = 0; i < streamer.threads-1; ++i) {
    pthread_create(&threads[i], NULL, streamer_thread, &streamer);
  }
  streamer_thread(&streamer);            /* Use the main thread too. */
  /* Join the other threads */
  for (size_t i = 0; i < streamer.threads-1; ++i) {
    pthread_join(threads[i], NULL);
  }
  pn_proactor_free(streamer.proactor);
  addresses_destroy(&streamer.addresses);
  free(threads);
  return exit_code;
}
