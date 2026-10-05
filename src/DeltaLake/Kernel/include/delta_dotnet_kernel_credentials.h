#ifndef DELTA_DOTNET_KERNEL_CREDENTIALS_H
#define DELTA_DOTNET_KERNEL_CREDENTIALS_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include "delta_kernel_ffi.h"

/**
 * Counted, borrowed UTF-8 option pair for the additive engine constructor.
 */
typedef struct KernelCredentialOptionV1 {
  const uint8_t *key_utf8;
  uint64_t key_len;
  const uint8_t *value_utf8;
  uint64_t value_len;
} KernelCredentialOptionV1;

/**
 * Borrowed request descriptor. The callback must copy it before returning.
 */
typedef struct KernelCredentialRequestV1 {
  uint32_t abi_version;
  uint32_t struct_size;
  uint64_t context_id;
  uint64_t request_id;
  uint32_t credential_kind;
  uint32_t timeout_ms;
  uint32_t minimum_lifetime_ms;
  uint32_t flags;
  uint64_t reserved[2];
} KernelCredentialRequestV1;

/**
 * Immutable registration descriptor, copied only after validation.
 */
typedef struct KernelCredentialRegistrationV1 {
  uint32_t abi_version;
  uint32_t struct_size;
  uint64_t context_id;
  uint32_t credential_kind;
  uint32_t acquisition_timeout_ms;
  uint32_t max_token_bytes;
  uint32_t flags;
  uint32_t (*begin)(const struct KernelCredentialRequestV1*);
  void (*cancel)(uint64_t, uint64_t, uint32_t);
  void (*released)(uint64_t);
  uint64_t reserved[2];
} KernelCredentialRegistrationV1;

/**
 * Borrowed completion descriptor; accepted bytes are copied during completion.
 */
typedef struct KernelCredentialResultV1 {
  uint32_t abi_version;
  uint32_t struct_size;
  uint32_t status;
  uint32_t credential_kind;
  const uint8_t *token_utf8;
  uint64_t token_len;
  int64_t expires_unix_ms;
  uint64_t reserved[2];
} KernelCredentialResultV1;

/**
 * Nonblocking managed request admission callback.
 */
typedef uint32_t (*KernelCredentialBegin)(const struct KernelCredentialRequestV1*);

/**
 * Nonblocking cooperative cancellation callback.
 */
typedef void (*KernelCredentialCancel)(uint64_t, uint64_t, uint32_t);

/**
 * Final callback, after all native consumer and acquisition leases retire.
 */
typedef void (*KernelCredentialReleased)(uint64_t);

/**
 * Creates a provider-backed default engine in the same image as all stock exports.
 * Zero worker threads selects two owned runtime workers; zero blocking threads uses
 * Tokio's default. The registration must be activated and still attachable.
 *
 * # Safety
 * URI/options must describe readable, borrowed UTF-8 for this call. The allocator
 * must remain valid through every engine clone and stock operation using it.
 */
ExternResultHandleSharedExternEngine kernel_engine_new_with_credential_v1(KernelStringSlice table_uri,
                                                                          const struct KernelCredentialOptionV1 *options,
                                                                          uint64_t option_count,
                                                                          uint64_t context_id,
                                                                          AllocateErrorFn allocate_error,
                                                                          uint32_t worker_threads,
                                                                          uint32_t max_blocking_threads);

/**
 * Returns the credential wire ABI version, independently of the stock kernel ABI.
 */
uint32_t kernel_credential_abi_version(void);

/**
 * Reserves a nonreused context ID; writes zero on failure.
 *
 * # Safety
 * `context_id` must point to writable u64 storage for this call.
 */
uint32_t kernel_credential_context_new(uint64_t *context_id);

/**
 * Publishes a validated, dormant callback registration.
 *
 * # Safety
 * The descriptor header and, for a supported header, full descriptor must be readable.
 * Callbacks must remain callable until Released and must not unwind or block.
 */
uint32_t kernel_credential_register(const struct KernelCredentialRegistrationV1 *registration);

/**
 * Enables callbacks after the caller publishes its static callback roots.
 */
uint32_t kernel_credential_activate(uint64_t context_id);

/**
 * Stops new engine attachments without revoking existing consumers.
 */
uint32_t kernel_credential_unregister(uint64_t context_id);

/**
 * Claims a pending request and copies its bounded payload synchronously.
 * Unknown, retired and duplicate keys return IGNORED without reading `result`.
 *
 * # Safety
 * A live key requires a readable descriptor header/full supported descriptor and
 * readable token bytes for the validated count. All memory is borrowed for this call.
 */
uint32_t kernel_credential_complete(uint64_t context_id,
                                    uint64_t request_id,
                                    const struct KernelCredentialResultV1 *result);

#endif  /* DELTA_DOTNET_KERNEL_CREDENTIALS_H */
