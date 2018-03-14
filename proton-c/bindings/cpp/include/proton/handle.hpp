#ifndef PROTON_HANDLE_HPP
#define PROTON_HANDLE_HPP

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

#include "./internal/comparable.hpp"
#include "./internal/export.hpp"
#include "./work_queue.hpp"

/// @file
/// @copybriefg proton::handle

namespace proton {

// A handle identifies a proton object. Handles can safely be passed and stored
// in non-proton threads, unlike proton objects.
template <class T> class handle : public internal::comparable<handle<T> > {
  public:
    handle() : ptr_(0) {}
    handle(const T& x) : ptr_(x.pn_object()) {}
    // Default copy, move, assign, dtor

    /// Get the proton object identified by the handle.
    ///
    /// **Thread safety** this function can only be called in the objects
    /// thread context, or in a functor that is added to the objects
    /// @ref work_queue
    PN_CPP_EXTERN T get() const;

    /// Get the @ref work_queue associated with the object.
    ///
    /// **Thread safety** safe to call in any thread as long as the object exists.
    class work_queue& work_queue() const { return work_queue::get(ptr_); }

    bool operator!() const { return !ptr_; }
#if PN_CPP_HAS_EXPLICIT_CONVERSIONS
    explicit operator bool() const { return ptr_; }
#endif
  friend bool operator==(const handle& a, const handle& b) { return a.ptr_ == b.ptr_; }
  friend bool operator<(const handle& a, const handle& b) { return a.ptr_ < b.ptr_; }

  protected:
    typename T::pn_type* ptr_;
};

template <class T> handle<T> make_handle(const T& x) { return handle<T>(x); }

// FIXME aconway 2018-03-13: should interoperate with returned<T>

} // proton

#endif  /*!PROTON_HANDLE_HPP*/
