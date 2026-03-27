// Copyright (C) 2026 Kumo inc. and its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include <memory>
#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <cassert>

namespace taco {
    ///////////////////////////////////////////////////////////////////////////////////////////////
    ///         DoublyBufferedData
    /// ------------------------------------------------------------------------------------------
    /// This data structure makes read() almost lock-free by making modify()
    /// *much* slower. It's very suitable for implementing LoadBalancers which
    /// have a lot of concurrent read-only ops from many threads and occasional
    /// modifications of data. As a side effect, this data structure can store
    /// a thread-local data for user.
    ///
    /// read(): begin with a thread-local mutex locked then read the foreground
    /// instance which will not be changed before the mutex is unlocked. Since the
    /// mutex is only locked by modify() with an empty critical section, the
    /// function is almost lock-free.
    ///
    /// modify(): modify background instance which is not used by any read(), flip
    /// foreground and background, lock thread-local mutexes one by one to make
    /// sure all existing read() finish and later read() see new foreground,
    /// then modify background(foreground before flip) again.
    ///////////////////////////////////////////////////////////////////////////////////////////////

    template<typename T, typename MUTEX = std::mutex, typename RWMUTEX = std::shared_mutex>
    class DoublyBufferedData {
    public:
        class ScopedPtr {
        public:
            ScopedPtr() = default;

            ~ScopedPtr() = default;
            /// disable copy
            ScopedPtr(const ScopedPtr &) = delete;
            ScopedPtr &operator=(const ScopedPtr &) = delete;

            const T *get() const {
                return data_;
            }

            const T &operator*() const {
                return *data_;
            }

            const T *operator->() const {
                return data_;
            }

            static void create(const T *d, RWMUTEX &rw_mutex, ScopedPtr &ptr) {
                ScopedPtr sp(d, rw_mutex);
                ptr = std::move(sp);
            }

            ScopedPtr(ScopedPtr &&ptr) noexcept {
                data_ = ptr.data_;
                ptr.data_ = nullptr;
                rl_ = std::move(ptr.rl_);
            }

            void operator=(ScopedPtr &&ptr) noexcept {
                data_ = ptr.data_;
                ptr.data_ = nullptr;
                rl_ = std::move(ptr.rl_);
            }

        private:
            ScopedPtr(const T *d, RWMUTEX &rw_mutex) : data_(d), rl_(rw_mutex) {
            }

        private:
            const T *data_{nullptr};
            std::shared_lock<RWMUTEX> rl_;
        };

        DoublyBufferedData();

        ~DoublyBufferedData();

        /// Put foreground instance into ptr. The instance will not be changed until
        /// ptr is destructed.
        /// This function is not blocked by read() and modify() in other threads.
        /// Returns 0 on success, -1 otherwise.
        int read(ScopedPtr *ptr);

        /// modify background and foreground instances. fn(T&, ...) will be called
        /// twice. modify() from different threads are exclusive from each other.
        /// NOTE: Call same series of fn to different equivalent instances should
        /// result in equivalent instances, otherwise foreground and background
        /// instance will be inconsistent.
        template<typename Fn, typename... Args>
        size_t modify(Fn &fn, const Args &... args);

        /// fn(T& background, const T& foreground, ...) will be called to background
        /// and foreground instances respectively.
        template<typename Fn, typename... Args>
        size_t modify_with_fore_ground(Fn &fn, const Args &... args);

    private:
        template<typename Fn, typename... Args>
        struct WithFG0 {
            WithFG0(Fn &f, T *d, const Args &... a)
                : fn(f), data(d), args_tuple(a...) {
            }

            size_t operator()(T &bg) {
                return std::apply([this, &bg](auto &&... unpacked_args) {
                    return fn(bg, (const T &) data[&bg == data], unpacked_args...);
                }, args_tuple);
            }

        private:
            Fn &fn;
            T *data;
            std::tuple<const Args &...> args_tuple;
        };

        template<typename Fn, typename Arg1>
        struct WithFG1 {
            WithFG1(Fn &f, T *d, const Arg1 &a1) : fn(f), data(d), arg1(a1) {
            }

            size_t operator()(T &bg) {
                return fn(bg, (const T &) data[&bg == data], arg1);
            }

        private:
            Fn &fn;
            T *data;
            const Arg1 &arg1;
        };

        template<typename Fn, typename Arg1, typename Arg2>
        struct WithFG2 {
            WithFG2(Fn &f, T *d, const Arg1 &a1, const Arg2 &a2) : fn(f), data(d), arg1(a1), arg2(a2) {
            }

            size_t operator()(T &bg) {
                return fn(bg, (const T &) data[&bg == data], arg1, arg2);
            }

        private:
            Fn &fn;
            T *data;
            const Arg1 &arg1;
            const Arg2 &arg2;
        };

        const T *unsafe_read(int index) const {
            return data_ + index;
        }

        /// Foreground and background void.
        T data_[2];

        /// Index of foreground instance.
        std::atomic<int> index_;

        /// Foreground and background rwlock
        /// 0/1 fore/bg lock
        RWMUTEX _rwlock[2];
        // Sequence modifications.
        MUTEX modify_mutex_;
    };

    template<typename T, typename MUTEX, typename RWMUTEX>
    DoublyBufferedData<T, MUTEX, RWMUTEX>::DoublyBufferedData() : index_(0) {
        /// Initialize _data for some POD types. This is essential for pointer
        /// types because they should be read() as nullptr before any modify().
        if (std::is_integral<T>::value || std::is_floating_point<T>::value || std::is_pointer<T>::value
            || std::is_member_function_pointer<T>::value) {
            data_[0] = T();
            data_[1] = T();
        }
    }

    template<typename T, typename MUTEX, typename RWMUTEX>
    DoublyBufferedData<T, MUTEX, RWMUTEX>::~DoublyBufferedData() {
    }

    template<typename T, typename MUTEX, typename RWMUTEX>
    int DoublyBufferedData<T, MUTEX, RWMUTEX>::read(typename DoublyBufferedData<T, MUTEX, RWMUTEX>::ScopedPtr *ptr) {
        int fg_index = index_.load(std::memory_order_acquire);
        ScopedPtr::create(unsafe_read(fg_index), _rwlock[fg_index], *ptr);
        return 0;
    }

    template<typename T, typename MUTEX, typename RWMUTEX>
    template<typename Fn, typename... Args>
    size_t DoublyBufferedData<T, MUTEX, RWMUTEX>::modify(Fn &fn, const Args &... args) {
        /// _modify_mutex sequences modifications. Using a separate mutex rather
        /// than _wrappers_mutex is to avoid blocking threads calling
        /// AddWrapper() or RemoveWrapper() too long. Most of the time, modifications
        /// are done by one thread, contention should be negligible.
        std::unique_lock<MUTEX> lk(modify_mutex_);
        int bg_index = !index_.load(std::memory_order_relaxed);
        /// background instance is not accessed by other threads, being safe to
        /// modify.
        const size_t ret = fn(data_[bg_index], args...);
        if (!ret) {
            return 0;
        }

        /// Publish, flip background and foreground.
        /// The release fence matches with the acquire fence in unsafe_read() to
        /// make readers which just begin to read the new foreground instance see
        /// all changes made in fn.
        index_.store(bg_index, std::memory_order_release);
        bg_index = !bg_index;

        /// Wait until all threads finishes current reading. When they begin next
        /// read, they should see updated _index.
        std::unique_lock<RWMUTEX> bg_lock(_rwlock[bg_index]);

        const size_t ret2 = fn(data_[bg_index], args...);
        assert(ret2 == ret);
        return ret2;
    }

    template<typename T, typename MUTEX, typename RWMUTEX>
    template<typename Fn, typename... Args>
    size_t DoublyBufferedData<T, MUTEX, RWMUTEX>::modify_with_fore_ground(Fn &fn, const Args &... args) {
        WithFG0<Fn, Args...> c(fn, data_, args...);
        return modify(c);
    }
} // namespace taco
