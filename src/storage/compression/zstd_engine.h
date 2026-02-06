#ifndef LYGUS_ZSTD_ENGINE_H
#define LYGUS_ZSTD_ENGINE_H

#include <stddef.h>
#include <stdint.h>
#include <string.h> // Fixes the memcpy error
#include <stdlib.h> // Fixes malloc/free

#ifdef __cplusplus
extern "C" {
#endif

    // ============================================================================
    // stub zstd engine
    // ============================================================================


    typedef struct lygus_zstd_ctx {
        int level;
    } lygus_zstd_ctx_t;


    static inline lygus_zstd_ctx_t* lygus_zstd_create(int level) {
        lygus_zstd_ctx_t* ctx = malloc(sizeof(lygus_zstd_ctx_t));
        if (ctx) ctx->level = level;
        return ctx;
    }

    static inline void lygus_zstd_destroy(lygus_zstd_ctx_t* ctx) {
        free(ctx);
    }


    static inline size_t lygus_zstd_compress(lygus_zstd_ctx_t* ctx,
                                             void* dst, size_t dst_cap,
                                             const void* src, size_t src_len) {
        (void)ctx;
        // If output buffer is too small, return 0 (error)
        if (dst_cap < src_len) return 0;

        memcpy(dst, src, src_len);
        return src_len;
    }


    static inline size_t lygus_zstd_decompress(lygus_zstd_ctx_t* ctx,
                                               void* dst, size_t dst_cap,
                                               const void* src, size_t src_len) {
        (void)ctx;
        if (dst_cap < src_len) return 0;

        memcpy(dst, src, src_len);
        return src_len;
    }

#ifdef __cplusplus
}
#endif

#endif // LYGUS_ZSTD_ENGINE_H