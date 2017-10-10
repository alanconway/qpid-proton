#ifndef TESTS_TEST_PORT_H
#define TESTS_TEST_PORT_H

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

#include "../platform/platform.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Some very simple platform-secifics to acquire an unused socket */
#if defined(_WIN32)

#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET sock_t;
void sock_close(sock_t sock) { closesocket(sock); }
// pni_snprintf not exported.  We can live with a simplified version
// for this test's limited use. Abort if that assumption is wrong.
#define pni_snprintf pnitst_snprintf
static int pnitst_snprintf(char *buf, size_t count, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int n = _vsnprintf(buf, count, fmt, ap);
  va_end(ap);
  if (count == 0 || n < 0) {
    perror("proton internal failure on Windows test snprintf");
    abort();
  }
  // Windows and C99 are in agreement.
  return n;
}

#else  /* POSIX */
# include <sys/types.h>
# include <sys/socket.h>
# include <netinet/in.h>
# include <unistd.h>
typedef int sock_t;
void sock_close(sock_t sock) { close(sock); }
#endif

#define TEST_PORT_MAX_STR 1060
/* Combines a sock_t with the int and char* versions of the port for convenience */
typedef struct test_port_t {
  sock_t sock;
  int port;                     /* port as integer */
  char str[TEST_PORT_MAX_STR];	/* port as string */
  char host_port[TEST_PORT_MAX_STR]; /* host:port string */
} test_port_t;

/* Modifies tp->host_port to use host, returns the new tp->host_port */
const char *test_port_use_host(test_port_t *tp, const char *host) {
  pni_snprintf(tp->host_port, sizeof(tp->host_port), "%s:%d", host, tp->port);
  return tp->host_port;
}

/* Create a socket and bind(INADDR_LOOPBACK:0) to get a free port.
   Use SO_REUSEADDR so other processes can bind and listen on this port.
   Use host to create the host_port address string.
*/
test_port_t test_port(const char* host) {
  test_port_t tp = {0};
  tp.sock = socket(AF_INET, SOCK_STREAM, 0);
  if (tp.sock < 0) { perror("socket"); abort(); }
  int on = 1;
  int err = setsockopt(tp.sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&on, sizeof(on));
  if (err) { perror("setsockopt"); abort(); }
  struct sockaddr_in addr = {0};
  addr.sin_family = AF_INET;    /* set the type of connection to TCP/IP */
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;            /* bind to port 0 */
  err = bind(tp.sock, (struct sockaddr*)&addr, sizeof(addr));
  if (err) { perror("bind"); abort(); }
  socklen_t len = sizeof(addr);
  err = getsockname(tp.sock, (struct sockaddr*)&addr, &len); /* Get the bound port */
  if (err) { perror("getsockname"); abort(); }
  tp.port = ntohs(addr.sin_port);
  pni_snprintf(tp.str, sizeof(tp.str), "%d", tp.port);
  test_port_use_host(&tp, host);
  return tp;
}

#endif // TESTS_TEST_PORT_H
