#include "block_format.h"
#include "../../util/varint.h"
#include "../../util/crc32c.h"
#include "../compression/zstd_engine.h"
#include <string.h>
#include <assert.h>

// ============================================================================
// Entry Implementation
// ============================================================================

ssize_t wal_entry_size(wal_entry_type_t type, uint64_t index, uint64_t term,
                       size_t klen, size_t vlen)
{
    // Validate type
    if (type < WAL_ENTRY_PUT || type > WAL_ENTRY_SNAP_MARK) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Check size limits
    if (klen > WAL_ENTRY_MAX_KEY_SIZE) {
        return LYGUS_ERR_KEY_TOO_LARGE;
    }
    if (vlen > WAL_ENTRY_MAX_VALUE_SIZE) {
        return LYGUS_ERR_VAL_TOO_LARGE;
    }

    // Calculate total size
    size_t total = 0;
    
    total += 1;                       // type byte
    total += varint_size(index);      // index varint
    total += varint_size(term);       // term varint
    total += varint_size(klen);       // klen varint
    total += varint_size(vlen);       // vlen varint
    total += klen;                    // key bytes
    total += vlen;                    // value bytes
    total += 4;                       // crc32c

    return (ssize_t)total;
}

ssize_t wal_entry_encode(wal_entry_type_t type, uint64_t index, uint64_t term,
                         const void *key, size_t klen,
                         const void *val, size_t vlen,
                         uint8_t *out, size_t out_len)
{
    // Validate inputs
    if (!out) {
        return LYGUS_ERR_INVALID_ARG;
    }
    if (type < WAL_ENTRY_PUT || type > WAL_ENTRY_SNAP_MARK) {
        return LYGUS_ERR_INVALID_ARG;
    }
    if (klen > 0 && !key) {
        return LYGUS_ERR_INVALID_ARG;
    }
    if (vlen > 0 && !val) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Calculate needed size
    ssize_t needed = wal_entry_size(type, index, term, klen, vlen);
    if (needed < 0) {
        return needed;
    }

    // Check buffer capacity
    if (out_len < (size_t)needed) {
        return LYGUS_ERR_INCOMPLETE;
    }

    size_t pos = 0;

    // Write type byte
    out[pos++] = (uint8_t)type;

    // Encode varints
    ssize_t ret;
    ret = varint_encode(index, &out[pos], out_len - pos);
    if (ret < 0) return ret;
    pos += (size_t)ret;

    ret = varint_encode(term, &out[pos], out_len - pos);
    if (ret < 0) return ret;
    pos += (size_t)ret;

    ret = varint_encode(klen, &out[pos], out_len - pos);
    if (ret < 0) return ret;
    pos += (size_t)ret;

    ret = varint_encode(vlen, &out[pos], out_len - pos);
    if (ret < 0) return ret;
    pos += (size_t)ret;

    // Copy key
    if (klen > 0) {
        memcpy(&out[pos], key, klen);
        pos += klen;
    }

    // Copy value
    if (vlen > 0) {
        memcpy(&out[pos], val, vlen);
        pos += vlen;
    }

    // Compute CRC over everything so far
    uint32_t crc = crc32c(out, pos);

    // Write CRC (little endian)
    out[pos++] = (crc >> 0) & 0xFF;
    out[pos++] = (crc >> 8) & 0xFF;
    out[pos++] = (crc >> 16) & 0xFF;
    out[pos++] = (crc >> 24) & 0xFF;

    assert(pos == (size_t)needed);
    return (ssize_t)pos;
}

ssize_t wal_entry_decode(const uint8_t *buf, size_t buf_len, wal_entry_t *entry)
{
    // Validate inputs
    if (!buf || !entry) {
        return LYGUS_ERR_INVALID_ARG;
    }
    if (buf_len == 0) {
        return LYGUS_ERR_INCOMPLETE;
    }

    size_t pos = 0;

    // Read type byte
    uint8_t type = buf[pos++];
    if (type < WAL_ENTRY_PUT || type > WAL_ENTRY_SNAP_MARK) {
        return LYGUS_ERR_MALFORMED;
    }

    // Decode index
    uint64_t index;
    ssize_t ret = varint_decode(&buf[pos], buf_len - pos, &index);
    if (ret < 0) return ret;
    pos += ret;

    // Decode term
    uint64_t term;
    ret = varint_decode(&buf[pos], buf_len - pos, &term);
    if (ret < 0) return ret;
    pos += ret;

    // Decode klen
    uint64_t klen;
    ret = varint_decode(&buf[pos], buf_len - pos, &klen);
    if (ret < 0) return ret;
    pos += ret;

    // Decode vlen
    uint64_t vlen;
    ret = varint_decode(&buf[pos], buf_len - pos, &vlen);
    if (ret < 0) return ret;
    pos += ret;

    // Validate lengths
    if (klen > WAL_ENTRY_MAX_KEY_SIZE) {
        return LYGUS_ERR_KEY_TOO_LARGE;
    }
    if (vlen > WAL_ENTRY_MAX_VALUE_SIZE) {
        return LYGUS_ERR_VAL_TOO_LARGE;
    }

    // Check remaining buffer has key + value + CRC
    if (buf_len - pos < klen + vlen + 4) {
        return LYGUS_ERR_INCOMPLETE;
    }

    // Extract key pointer
    const uint8_t *key = (klen > 0) ? &buf[pos] : NULL;
    pos += klen;

    // Extract value pointer
    const uint8_t *val = (vlen > 0) ? &buf[pos] : NULL;
    pos += vlen;

    // Read stored CRC (little-endian)
    uint32_t stored_crc = ((uint32_t)buf[pos + 0] << 0)
                        | ((uint32_t)buf[pos + 1] << 8)
                        | ((uint32_t)buf[pos + 2] << 16)
                        | ((uint32_t)buf[pos + 3] << 24);
    pos += 4;

    // Compute CRC over entry data (everything except CRC itself)
    uint32_t computed_crc = crc32c(buf, pos - 4);

    // Verify CRC
    if (stored_crc != computed_crc) {
        return LYGUS_ERR_CORRUPT;
    }

    // Fill entry structure
    entry->type = (wal_entry_type_t)type;
    entry->index = index;
    entry->term = term;
    entry->key = key;
    entry->klen = (size_t)klen;
    entry->val = val;
    entry->vlen = (size_t)vlen;
    entry->crc = stored_crc;

    // Initialize location fields to 0 (caller must set if needed)
    entry->segment_num = 0;
    entry->block_offset = 0;
    entry->entry_offset = 0;

    return (ssize_t)pos;
}

// ============================================================================
// Block Implementation
// ============================================================================

ssize_t wal_block_compress(const uint8_t *raw_data, size_t raw_len,
                           uint64_t seq_no, void *zctx,
                           wal_block_hdr_t *out_hdr,
                           uint8_t *out_data, size_t out_cap)
{
    // Validate inputs
    if (!raw_data || !out_hdr || !out_data || !zctx) {
        return LYGUS_ERR_INVALID_ARG;
    }
    if (raw_len > WAL_BLOCK_SIZE) {
        return LYGUS_ERR_INVALID_ARG;
    }
    if (out_cap < WAL_BLOCK_SIZE + 1024) {  // Need headroom for compression metadata
        return LYGUS_ERR_INCOMPLETE;
    }

    // Attempt compression
    lygus_zstd_ctx_t *zstd_ctx = (lygus_zstd_ctx_t *)zctx;
    size_t comp_len = lygus_zstd_compress(zstd_ctx, out_data, out_cap, raw_data, raw_len);

    uint16_t flags = 0;
    size_t final_len;
    const uint8_t *final_data;

    // Check if compression was beneficial
    if (comp_len == 0 || comp_len >= raw_len) {
        // Compression failed or made data larger - store uncompressed
        flags |= WAL_FLAG_UNCOMPRESSED;
        memcpy(out_data, raw_data, raw_len);
        final_len = raw_len;
        final_data = raw_data;
    } else {
        // Compression succeeded
        final_len = comp_len;
        final_data = out_data;
    }

    // Compute CRC over final payload
    uint32_t crc = crc32c(final_data, final_len);

    // Fill header (except header CRC)
    out_hdr->magic = WAL_BLOCK_MAGIC;
    out_hdr->version = WAL_BLOCK_VERSION;
    out_hdr->flags = flags;
    out_hdr->seq_no = seq_no;
    out_hdr->raw_len = (uint32_t)raw_len;
    out_hdr->comp_len = (uint32_t)final_len;
    out_hdr->crc32c = crc;

    // Compute header CRC over first 28 bytes (everything except hdr_crc32c)
    out_hdr->hdr_crc32c = crc32c((const uint8_t *)out_hdr, WAL_BLOCK_HDR_CRC_OFFSET);

    return (ssize_t)final_len;
}

ssize_t wal_block_decompress(const wal_block_hdr_t *hdr,
                             const uint8_t *comp_data,
                             void *zctx,
                             uint8_t *out_raw, size_t out_cap)
{
    if (!hdr || !comp_data || !out_raw || !zctx) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_block_validate_header(hdr);
    if (ret < 0) {
        return ret;
    }

    if (out_cap < hdr->raw_len) {
        return LYGUS_ERR_INCOMPLETE;
    }

    // Verify payload CRC - use header as source of truth
    uint32_t computed_crc = crc32c(comp_data, hdr->comp_len);
    if (computed_crc != hdr->crc32c) {
        return LYGUS_ERR_CORRUPT;
    }

    if (hdr->flags & WAL_FLAG_UNCOMPRESSED) {
        if (hdr->comp_len != hdr->raw_len) {
            return LYGUS_ERR_MALFORMED;
        }
        memcpy(out_raw, comp_data, hdr->raw_len);
        return (ssize_t)hdr->raw_len;
    } else {
        lygus_zstd_ctx_t *zstd_ctx = (lygus_zstd_ctx_t *)zctx;
        size_t decompressed = lygus_zstd_decompress(zstd_ctx, out_raw, out_cap,
                                                     comp_data, hdr->comp_len);

        if (decompressed == 0) {
            return LYGUS_ERR_DECOMPRESS;
        }

        if (decompressed != hdr->raw_len) {
            return LYGUS_ERR_MALFORMED;
        }

        return (ssize_t)decompressed;
    }
}

int wal_block_validate_header(const wal_block_hdr_t *hdr)
{
    if (!hdr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // check header CRC first
    uint32_t computed_hdr_crc = crc32c((const uint8_t *)hdr, WAL_BLOCK_HDR_CRC_OFFSET);
    if (computed_hdr_crc != hdr->hdr_crc32c) {
        return LYGUS_ERR_CORRUPT;
    }

    // Check magic number
    if (hdr->magic != WAL_BLOCK_MAGIC) {
        return LYGUS_ERR_BAD_BLOCK;
    }

    // Check version
    if (hdr->version != WAL_BLOCK_VERSION) {
        return LYGUS_ERR_BAD_BLOCK;
    }

    // Check raw_len is reasonable
    if (hdr->raw_len > WAL_BLOCK_SIZE) {
        return LYGUS_ERR_BAD_BLOCK;
    }

    // Check comp_len is reasonable
    if (hdr->comp_len > WAL_BLOCK_SIZE + 1024) {  // Allow some overhead
        return LYGUS_ERR_BAD_BLOCK;
    }

    // Check flags are valid
    uint16_t valid_flags = WAL_FLAG_LAST_BLOCK | WAL_FLAG_UNCOMPRESSED;
    if ((hdr->flags & ~valid_flags) != 0) {
        return LYGUS_ERR_BAD_BLOCK;
    }

    return LYGUS_OK;
}