#ifndef DELTA_HTTP_HEADERS_H
#define DELTA_HTTP_HEADERS_H

#include <stddef.h>
#include <stdint.h>

#if defined(_MSC_VER)
#define DELTA_HEADERS_CDECL __cdecl
#elif defined(__i386__)
#define DELTA_HEADERS_CDECL __attribute__((cdecl))
#else
#define DELTA_HEADERS_CDECL
#endif

#ifdef __cplusplus
extern "C" {
#endif

enum {
    DELTA_HEADERS_ABI_VERSION = 1,
    DELTA_HEADERS_REGISTER_ACCEPTED = 0,
    DELTA_HEADERS_REGISTER_INVALID = 1,
    DELTA_HEADERS_REGISTER_DUPLICATE = 2,
    DELTA_HEADERS_REGISTER_CAPACITY = 3,
    DELTA_HEADERS_COMPLETE_CLAIMED = 0,
    DELTA_HEADERS_COMPLETE_IGNORED = 1,
    DELTA_HEADERS_STATUS_SUCCESS = 0
};

/** Borrowed bytes, valid only during the receiving call. Length counts bytes. */
typedef struct ByteSlice {
    const uint8_t *data;
    size_t len;
} ByteSlice;

/** Begin copies method/URI before returning; zero means work was accepted. */
typedef uint32_t (DELTA_HEADERS_CDECL *HeaderBegin)(
    uint64_t context, uint64_t request, ByteSlice method, ByteSlice uri);

/** Cooperative cancellation, only after accepted begin and pending retirement. */
typedef void (DELTA_HEADERS_CDECL *HeaderCancel)(uint64_t context, uint64_t request);

/** Exactly once after the last native owner; managed workers retain their own state. */
typedef void (DELTA_HEADERS_CDECL *HeaderReleased)(uint64_t context);

/** All callbacks are required, promptly returning, non-unwinding, process-lifetime.
 * Set struct_size to sizeof(HeaderCallbacks). Registration copies the descriptor.
 * Context zero and duplicate live/retiring context IDs are rejected.
 * Host images export their own C register/unregister/complete wrappers and call
 * acquire internally. This rlib exports no Rust allocations across images.
 */
typedef struct HeaderCallbacks {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t context_id;
    HeaderBegin begin;
    HeaderCancel cancel;
    HeaderReleased released;
} HeaderCallbacks;

#ifdef __cplusplus
}
#endif

#endif