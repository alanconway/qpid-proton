#ifndef PROACTOR_PNI_EPOLL_HPP
#define PROACTOR_PNI_EPOLL_HPP

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

/* FIXME aconway 2018-05-22: doc simplified arm/update/disarm semantics */

/** @file
 *
 * Thread safe epoll ONESHOT mode.
 *
 * - an fd  is *armed* if it is enabled for events in epoll.
 * - an fd  returned by pni_epoll_wait() is *disarmed*, it will not be returned by
 *   pni_epoll_wait() again until after a call to pni_epoll_arm()
 * - pn_epoll_update() changes event flags for an *armed* fd, it is a no-op on an unarmed fd
 *
 * NOTE: This is what epoll ONESHOT is intended for, but it is not thread safe
 * if the event status of an fd is modified concurrent with epoll_wait()
 * returning that fd. epoll can't tell the difference between the processing
 * thread "re-arming" an fd and some other thread "updating" the event
 * status. This API distinguishes between pni_epoll_arm() and pni_epoll_udate()
 *
 */

#include <proton/type_compat.h>
#include <sys/epoll.h>

typedef struct pni_epoll_t pni_epoll_t;

/* Struct to register with each epoll fd */
typedef struct pni_epoll_data_t {
  /* FIXME aconway 2018-05-17: refactor better with epoll.c */
  /* FIXME aconway 2018-05-17: embed in pocket */
  int fd;
  uint32_t events;              /* events enabled when armed */
  bool armed:1;
} pni_epoll_data_t;

static inline int pni_epoll_data_fd(pni_epoll_data_t *ed) { return ed->fd; }

void pni_epoll_data_init(pni_epoll_data_t *ed, int fd, uint32_t events);

pni_epoll_t *pni_epoll(void);

void pni_epoll_free(pni_epoll_t* ep);


/* FIXME aconway 2018-05-22: doc */
int pni_epoll_arm(pni_epoll_t *ep, pni_epoll_data_t *ed);
int pni_epoll_update(pni_epoll_t *ep, pni_epoll_data_t *ed);
int pni_epoll_disarm(pni_epoll_t *ep, pni_epoll_data_t *ed);

/* @return
 * -  0: timeout
 * -  1: ev_out->data.ptr is an epoll_data_t
 * -  -1: error, see errno
 */
int pni_epoll_wait(pni_epoll_t *ep, struct epoll_event *ev_out, int timeout);

#endif // PROACTOR_PNI_EPOLL_HPP
