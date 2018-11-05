#ifndef PROTON_MESSAGING_HANDLER_HPP
#define PROTON_MESSAGING_HANDLER_HPP

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

#include "./fwd.hpp"
#include "./internal/export.hpp"

/// @file
/// @copybrief proton::messaging_handler

namespace proton {


/// Handler for Proton messaging events.
///
/// Subclass and override the event-handling member functions.
///
/// **Thread-safety**: A thread-safe handler can use the objects
/// passed as arguments, and other objects belonging to the same
/// proton::connection.  It *must not* use objects belonging to a
/// different connection. See @ref mt_page and proton::work_queue for
/// safe ways to communicate between connections.  Thread-safe
/// handlers can also be used in single-threaded code.
///
/// **Single-threaded only**: An application is single-threaded if it
/// calls container::run() exactly once, and only makes make proton
/// calls from handler functions. Single-threaded handler functions
/// can use objects belonging to another connection, but *must* call
/// connection::wake() on the other connection before returning. Such
/// a handler is not thread-safe.
///
/// ### Connection life-cycle and automatic re-connect
///
/// on_connection_start() is the first event for any connection.
///
/// on_connection_open() means the remote peer has sent an AMQP open.
/// For a client, this means the connection is fully open.  A server
/// should respond with connection::open() or reject the request with
/// connection::close()
///
/// on_transport_error() is called when the connection is disconnected
/// unexpectedly. If an automatic reconnect has been scheduled (see
/// reconnect_options) then connection::reconnect_pending() will be
/// true.
///
/// @note on_connection_open() will be called again after a successful
/// automatic re-connect.  Each open @ref session, @ref sender and
/// @ref receiver will also be automatically re-opened. On success,
/// on_sender_open() or on_receiver_open() are called, on failure
/// on_sender_error() or on_receiver_error().
///
/// on_connection_error() means the remote peer has indicated an error,
/// it is always followed by on_connection_close()
///
/// on_connection_close() means the remote peer has closed the
/// connection. Servers should respond with connection::close().
/// on_connection_close() is not called if the transport fails before
/// receiving a remote close.
///
/// on_transport_close() is always the final event for a connection,
/// and is always called regardless of how the connection closed or
/// whether there are errors.
///
/// @note Closing a connection with the special error condition
/// `amqp:connection-forced`is treated as a disconnect - it triggers
/// automatic re-connect or on_transport_error()/on_transport_close(),
/// not on_connection_close().
///
/// @see reconnect_options
///
class
PN_CPP_CLASS_EXTERN messaging_handler {
  public:
    PN_CPP_EXTERN messaging_handler();
    PN_CPP_EXTERN virtual ~messaging_handler();

    /// The container event loop is starting.
    ///
    /// This is the first event received after calling
    /// `container::run()`.
    PN_CPP_EXTERN virtual void on_container_start(container&);

    /// The container event loop is stopping.
    ///
    /// This is the last event received before the container event
    /// loop stops.
    PN_CPP_EXTERN virtual void on_container_stop(container&);

    /// A message is received.
    PN_CPP_EXTERN virtual void on_message(delivery&, message&);

    /// A message can be sent.
    PN_CPP_EXTERN virtual void on_sendable(sender&);

    /// The underlying network transport is open
    PN_CPP_EXTERN virtual void on_transport_open(transport&);

    /// The underlying network transport has closed.
    /// This is the final event for a connection, there will be
    /// no more events or re-connect attempts.
    PN_CPP_EXTERN virtual void on_transport_close(transport&);

    /// The underlying network transport has disconnected unexpectedly.
    ///
    /// If an automatic re-connect has been scheduled then
    /// connection::reconnect_pending() will be true.
    /// Calling connection::close() from on_transport_error()
    /// will cancel any pending reconnect.
    PN_CPP_EXTERN virtual void on_transport_error(transport&);

    /// **Unsettled API** - Called before the connection is opened.
    /// Use for initial setup, e.g. to open senders or receivers.
    PN_CPP_EXTERN virtual void on_connection_start(connection&);

    /// The remote peer opened the connection.
    /// Called for the initial open, and also after each successful re-connect if
    /// @ref reconnect_options are set.
    PN_CPP_EXTERN virtual void on_connection_open(connection&);

    /// The remote peer closed the connection.
    PN_CPP_EXTERN virtual void on_connection_close(connection&);

    /// The remote peer closed the connection with an error condition.
    PN_CPP_EXTERN virtual void on_connection_error(connection&);

    /// The remote peer opened the session.
    PN_CPP_EXTERN virtual void on_session_open(session&);

    /// The remote peer closed the session.
    PN_CPP_EXTERN virtual void on_session_close(session&);

    /// The remote peer closed the session with an error condition.
    PN_CPP_EXTERN virtual void on_session_error(session&);

    /// The remote peer opened the link.
    PN_CPP_EXTERN virtual void on_receiver_open(receiver&);

    /// The remote peer detached the link.
    PN_CPP_EXTERN virtual void on_receiver_detach(receiver&);

    /// The remote peer closed the link.
    PN_CPP_EXTERN virtual void on_receiver_close(receiver&);

    /// The remote peer closed the link with an error condition.
    PN_CPP_EXTERN virtual void on_receiver_error(receiver&);

    /// The remote peer opened the link.
    PN_CPP_EXTERN virtual void on_sender_open(sender&);

    /// The remote peer detached the link.
    PN_CPP_EXTERN virtual void on_sender_detach(sender&);

    /// The remote peer closed the link.
    PN_CPP_EXTERN virtual void on_sender_close(sender&);

    /// The remote peer closed the link with an error condition.
    PN_CPP_EXTERN virtual void on_sender_error(sender&);

    /// The receiving peer accepted a transfer.
    PN_CPP_EXTERN virtual void on_tracker_accept(tracker&);

    /// The receiving peer rejected a transfer.
    PN_CPP_EXTERN virtual void on_tracker_reject(tracker&);

    /// The receiving peer released a transfer.
    PN_CPP_EXTERN virtual void on_tracker_release(tracker&);

    /// The receiving peer settled a transfer.
    PN_CPP_EXTERN virtual void on_tracker_settle(tracker&);

    /// The sending peer settled a transfer.
    PN_CPP_EXTERN virtual void on_delivery_settle(delivery&);

    /// **Unsettled API** - The receiving peer has requested a drain of
    /// remaining credit.
    PN_CPP_EXTERN virtual void on_sender_drain_start(sender&);

    /// **Unsettled API** - The credit outstanding at the time of the
    /// drain request has been consumed or returned.
    PN_CPP_EXTERN virtual void on_receiver_drain_finish(receiver&);

    /// **Unsettled API** - An event that can be triggered from
    /// another thread.
    ///
    /// This event is triggered by a call to `connection::wake()`.  It
    /// is used to notify the application that something needs
    /// attention.
    ///
    /// **Thread-safety** - The application handler and the triggering
    /// thread must use some form of thread-safe state or
    /// communication to tell the handler what it needs to do.  See
    /// `proton::work_queue` for an easier way to execute code safely
    /// in the handler thread.
    ///
    /// @note Spurious calls to `on_connection_wake()` can occur
    /// without any application call to `connection::wake()`.
    PN_CPP_EXTERN virtual void on_connection_wake(connection&);

    /// Fallback error handling.
    PN_CPP_EXTERN virtual void on_error(const error_condition&);
};

} // proton

#endif // PROTON_MESSAGING_HANDLER_HPP
