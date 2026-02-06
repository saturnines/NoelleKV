#ifndef LYGUS_TIMING_H
#define LYGUS_TIMING_H

#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * Lygus Timing Utilities
     *
     * High-resolution timestamps for latency measurement.
     * Uses CLOCK_MONOTONIC to avoid issues with time adjustments.
     */

    /**
     * Get current timestamp in nanoseconds (monotonic)
     *
     * Use CLOCK_MONOTONIC so measurements aren't affected by NTP adjustments.
     *
     * @return Nanoseconds since arbitrary epoch (monotonic)
     */
    static inline uint64_t lygus_now_ns(void) {
        struct timespec ts;

#ifdef CLOCK_MONOTONIC
        clock_gettime(CLOCK_MONOTONIC, &ts);
#else
        // Fallback for systems without CLOCK_MONOTONIC
        clock_gettime(CLOCK_REALTIME, &ts);
#endif

        return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    }

    /**
     * Get current timestamp in microseconds (monotonic)
     *
     * @return Microseconds since arbitrary epoch
     */
    static inline uint64_t lygus_now_us(void) {
        return lygus_now_ns() / 1000;
    }

    /**
     * Convert nanoseconds to microseconds
     *
     * @param ns Nanoseconds
     * @return Microseconds (truncated)
     */
    static inline uint32_t lygus_ns_to_us(uint64_t ns) {
        return (uint32_t)(ns / 1000);
    }

    /**
     * Convert nanoseconds to milliseconds
     *
     * @param ns Nanoseconds
     * @return Milliseconds (truncated)
     */
    static inline uint32_t lygus_ns_to_ms(uint64_t ns) {
        return (uint32_t)(ns / 1000000);
    }

#ifdef __cplusplus
}
#endif

#endif // LYGUS_TIMING_H