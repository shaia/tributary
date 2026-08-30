#pragma once
//
// Consumer thread placement.
//
// Both of these are best-effort and report failure rather than throwing: a
// pipeline that refuses to start because it could not set a thread name is
// worse than one that starts unnamed. Pinning is the one that matters -- the
// tail-latency argument for this whole design assumes the consumer does not
// migrate between cores, and an unpinned consumer on a busy box will.

#include "../traits.hpp"

#include <string>
#include <string_view>

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <windows.h>
#  include <processthreadsapi.h>
#elif defined(__linux__)
#  include <pthread.h>
#  include <sched.h>
#elif defined(__APPLE__)
#  include <pthread.h>
#endif

namespace tributary {
inline namespace TRIBUTARY_ABI {
namespace detail {

// Names the calling thread. Shows up in top, perf, gdb, and Task Manager, which
// is the difference between "one of these 40 threads is hot" and knowing which.
inline bool set_thread_name(std::string_view name) noexcept {
#if defined(_WIN32)
    // SetThreadDescription is Windows 10 1607+; resolve dynamically so the
    // library still loads on older targets.
    using set_desc_fn = HRESULT(WINAPI*)(HANDLE, PCWSTR);
    // The double cast is deliberate: GetProcAddress returns FARPROC, and a
    // direct reinterpret_cast to a differently-shaped function pointer trips
    // -Wcast-function-type. Going through void* is the documented Win32 idiom.
    // NOLINTNEXTLINE(bugprone-casting-through-void)
    static const auto fn = reinterpret_cast<set_desc_fn>(reinterpret_cast<void*>(
        ::GetProcAddress(::GetModuleHandleW(L"kernel32.dll"), "SetThreadDescription")));
    if (fn == nullptr) return false;
    std::wstring wide(name.begin(), name.end());  // names are ASCII by convention
    return SUCCEEDED(fn(::GetCurrentThread(), wide.c_str()));
#elif defined(__linux__)
    // pthread_setname_np truncates at 16 bytes including the terminator, and
    // fails outright if given more, so truncate rather than silently not naming.
    std::string s(name.substr(0, 15));
    return ::pthread_setname_np(::pthread_self(), s.c_str()) == 0;
#elif defined(__APPLE__)
    std::string s(name);
    return ::pthread_setname_np(s.c_str()) == 0;
#else
    (void)name;
    return false;
#endif
}

// Pins the calling thread to one logical CPU.
inline bool pin_to_cpu(int cpu) noexcept {
    if (cpu < 0) return false;
#if defined(_WIN32)
    // A DWORD_PTR affinity mask covers one processor group, so this reaches the
    // first 64 logical CPUs. Beyond that Windows needs
    // SetThreadGroupAffinity and a group number; machines that large are also
    // the ones where you want explicit NUMA placement anyway.
    if (cpu >= 64) return false;
    const DWORD_PTR mask = static_cast<DWORD_PTR>(1) << cpu;
    return ::SetThreadAffinityMask(::GetCurrentThread(), mask) != 0;
#elif defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<unsigned>(cpu), &set);
    return ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set) == 0;
#else
    // macOS exposes no portable hard affinity: thread_policy_set with
    // THREAD_AFFINITY_POLICY is only an advisory hint to the scheduler, and on
    // Apple silicon it is ignored entirely. Reporting failure is more honest
    // than pretending it worked.
    (void)cpu;
    return false;
#endif
}

}  // namespace detail
}  // namespace TRIBUTARY_ABI
}  // namespace tributary
