#include "delta_dotnet_kernel_credentials.h"

_Static_assert(sizeof(KernelCredentialRequestV1) == 56, "request size");
_Static_assert(sizeof(KernelCredentialResultV1) == 56, "result size");
_Static_assert(sizeof(KernelCredentialRegistrationV1) == 72, "registration size");
_Static_assert(sizeof(KernelCredentialOptionV1) == 32, "option size");
_Static_assert(_Alignof(KernelCredentialRegistrationV1) == 8, "native alignment");
_Static_assert(offsetof(KernelCredentialRequestV1, context_id) == 8, "context offset");
_Static_assert(offsetof(KernelCredentialRequestV1, request_id) == 16, "request offset");
_Static_assert(offsetof(KernelCredentialRequestV1, credential_kind) == 24, "kind offset");
_Static_assert(offsetof(KernelCredentialRequestV1, timeout_ms) == 28, "timeout offset");
_Static_assert(offsetof(KernelCredentialRequestV1, minimum_lifetime_ms) == 32, "lifetime offset");
_Static_assert(offsetof(KernelCredentialResultV1, token_utf8) == 16, "token offset");
_Static_assert(offsetof(KernelCredentialResultV1, token_len) == 24, "token count offset");
_Static_assert(offsetof(KernelCredentialResultV1, expires_unix_ms) == 32, "expiry offset");
_Static_assert(offsetof(KernelCredentialRegistrationV1, begin) == 32, "begin offset");
_Static_assert(offsetof(KernelCredentialRegistrationV1, cancel) == 40, "cancel offset");
_Static_assert(offsetof(KernelCredentialRegistrationV1, released) == 48, "released offset");
_Static_assert(offsetof(KernelCredentialRegistrationV1, reserved) == 56, "reserved offset");

uint32_t verify_credential_exports(void) {
    uint64_t context_id = 0;
    uint32_t version = kernel_credential_abi_version();
    if (version != 1 || kernel_credential_context_new(&context_id) != 0) {
        return 1;
    }
    return kernel_credential_unregister(context_id);
}