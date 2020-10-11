/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "yaksi.h"
#include "yaksu.h"
#include <assert.h>

int yaksa_iacc(const void *inbuf, uintptr_t insize, void *outbuf, uintptr_t outcount,
               yaksa_type_t type, uintptr_t outoffset, yaksa_op_t op, yaksa_info_t info,
               yaksa_request_t * request)
{
    int rc = YAKSA_SUCCESS;

    assert(yaksu_atomic_load(&yaksi_is_initialized));

    if (outcount == 0) {
        *request = YAKSA_REQUEST__NULL;
        goto fn_exit;
    }

    yaksi_type_s *yaksi_type;
    rc = yaksi_type_get(type, &yaksi_type);
    YAKSU_ERR_CHECK(rc, fn_fail);

    if (yaksi_type->size == 0) {
        *request = YAKSA_REQUEST__NULL;
        goto fn_exit;
    }

    yaksi_request_s *yaksi_request;
    rc = yaksi_request_create(&yaksi_request);
    YAKSU_ERR_CHECK(rc, fn_fail);

    yaksi_info_s *yaksi_info;
    yaksi_info = (yaksi_info_s *) info;

    uintptr_t actual_unpack_bytes;
    rc = yaksi_iacc_unpack(inbuf, insize, outbuf, outcount, yaksi_type,
                           outoffset, &actual_unpack_bytes, op, yaksi_info, yaksi_request);
    YAKSU_ERR_CHECK(rc, fn_fail);

    assert(actual_unpack_bytes == insize);

    if (yaksu_atomic_load(&yaksi_request->cc)) {
        *request = yaksi_request->id;
    } else {
        rc = yaksi_request_free(yaksi_request);
        YAKSU_ERR_CHECK(rc, fn_fail);

        *request = YAKSA_REQUEST__NULL;
    }

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
