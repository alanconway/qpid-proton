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

#include "options.hpp"

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/error_condition.hpp>
#include <proton/handle.hpp>
#include <proton/listen_handler.hpp>
#include <proton/listener.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/receiver_options.hpp>
#include <proton/work_queue.hpp>

#include <deque>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <string>
#include <thread>

// A server that accepts messages, processes them in application threads,
// then accepts the message when processing is done.
//
// The code is split into an application class, which processes and accepts
// messages, and proton handler classes which do the proton work.
// The application uses a proton::handle<proton::delivery> to identify
// deliveries that need to be accepted, and a proton::work_queue to queue
// the actual accept on the correct proton thread.

// The application class that processes messages.
// The application implements its own thread-safe queue and runs in its own threads.
class application {
    typedef std::pair<proton::message, proton::handle<proton::delivery> > item;
    std::deque<item> work_;
    std::mutex lock_;
    std::condition_variable full_;

    // Get the next item, thread safe.
    item next() {
        std::unique_lock<std::mutex> l(lock_);
        while (work_.empty()) full_.wait(l);
        item i = work_.front();
        work_.pop_front();
        return i;
    }

    // Process an item (message) and send an accept via proton
    void process(const proton::message& m, proton::handle<proton::delivery> d) {
        std::cout << "process item " << m.body() << std::endl; // Dummy processing
        // The accept cannot be done directly in this thread, so we use a work_queue
        // to send it to the correct thread.
        d.work_queue().add([=]() { d.get().accept(); });
    }

  public:

    // Thread safe: called in proton threads to queue messages for processing
    void deliver(const proton::message& m, proton::handle<proton::delivery> d) {
        std::cout << "queue " << m.body() << " for processing" << std::endl;
        std::lock_guard<std::mutex> g(lock_);
        work_.push_back(item(m, d));
        full_.notify_one();
    }

    // Run an application thread to get items and process them.
    void run() {
        while(true) {
            item i = next();
            process(i.first, i.second);
        }
    }
};

// Proton handler that passes delivered messages to an application instance.
class connection_handler : public proton::messaging_handler {
    application& app_;

  public:
    connection_handler(application& app) : app_(app) {}

    // handler methods

    void on_receiver_open(proton::receiver &r) override {
        // Important to disable auto_accept so that proton won't accept for us,
        // we want to delay the accept till the application has processed the message.
        r.open(proton::receiver_options().auto_accept(false));
    }

    void on_message(proton::delivery &d, proton::message &m) override {
        // Pass the message and delivery to the application
        app_.deliver(m, d);
    }

    void on_error(const proton::error_condition& e) override {
        std::cerr << "error: " << e.what() << std::endl;
    }

    void on_transport_close(proton::transport&) override {
        delete this;            // All done
    }
};

class listen_handler : public proton::listen_handler {
    application& app_;

  public:

    listen_handler(application& app) : app_(app) {}

    proton::connection_options on_accept(proton::listener&) override {
        return proton::connection_options().handler(*new connection_handler(app_));
    }

    void on_open(proton::listener& l) override {
        std::cout << "listening on " << l.port() << std::endl;
    }

    void on_error(proton::listener&, const std::string& s) override {
        std::cerr << "listen error: " << s << std::endl;
        throw std::runtime_error(s);
    }
};

int main(int argc, char **argv) {
    std::string address("");
    int threads = std::thread::hardware_concurrency();
    example::options opts(argc, argv);

    opts.add_value(address, 'a', "address", "listen on URL", "URL");
    opts.add_value(threads, 't', "threads", "number of threads", "THREADS");

    try {
        opts.parse();
        application app;
        proton::container container(argv[0]);
        listen_handler lh(app);
        container.listen(address, lh);

        // Split the threads between proton and application.
        int n = (threads + 1) / 2;
        std::cout << "starting " << n*2 << " threads" << std:: endl << std::flush;
        std::vector<std::thread> threads;
        for (; n > 0; --n) threads.push_back(std::thread([&]() { app.run(); }));
        container.run(n);
        return 0;
    } catch (const example::bad_option& e) {
        std::cout << opts << std::endl << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "shutdown: " << e.what() << std::endl;
    }

    // FIXME aconway 2018-03-13: no prevision for shutting down.
    return 1;
}
