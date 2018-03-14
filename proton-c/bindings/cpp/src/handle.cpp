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

#include <proton/connection.hpp>
#include <proton/delivery.hpp>
#include <proton/handle.hpp>
#include <proton/link.hpp>
#include <proton/receiver.hpp>
#include <proton/sender.hpp>
#include <proton/session.hpp>
#include <proton/tracker.hpp>
#include "proton_bits.hpp"

namespace proton {

template <class T> T handle<T>::get() const { return internal::factory<T>::wrap(ptr_); }

// Explicit instantiations for the linker
template PN_CPP_EXTERN connection handle<connection>::get() const;
template PN_CPP_EXTERN session handle<session>::get() const;
template PN_CPP_EXTERN link handle<link>::get() const;
template PN_CPP_EXTERN sender handle<sender>::get() const;
template PN_CPP_EXTERN receiver handle<receiver>::get() const;
template PN_CPP_EXTERN delivery handle<delivery>::get() const;
template PN_CPP_EXTERN tracker handle<tracker>::get() const;

// FIXME aconway 2018-03-13: bring listener into the fold with a work_queue?
// Not really needed since close() is thread safe, but for symmetry.
}
