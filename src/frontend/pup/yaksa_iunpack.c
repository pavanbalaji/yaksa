/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "yaksi.h"
#include "yaksu.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

int yaksa_iunpack(const void *inbuf, uintptr_t insize, void *outbuf, uintptr_t outcount,
                  yaksa_type_t type, uintptr_t outoffset, uintptr_t * actual_unpack_bytes,
                  yaksa_info_t info, yaksa_request_t * request)
{
    int rc = YAKSA_SUCCESS;

    assert(yaksu_atomic_load(&yaksi_is_initialized));

    yaksa_context_t ctx_id;
    yaksu_handle_t obj_id;
    YAKSI_TYPE_DECODE(type, ctx_id, obj_id);

    yaksi_context_s *ctx;
    rc = yaksu_handle_pool_elem_get(yaksi_global.context_handle_pool, ctx_id, (const void **) &ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    if (outcount == 0) {
        *actual_unpack_bytes = 0;

        yaksi_request_s *req;
        rc = yaksi_request_create(ctx, &req);
        YAKSU_ERR_CHECK(rc, fn_fail);

        *request = req->id;
        goto fn_exit;
    }

    yaksi_type_s *yaksi_type;
    rc = yaksi_type_get(type, &yaksi_type);
    YAKSU_ERR_CHECK(rc, fn_fail);

    if (yaksi_type->size == 0) {
        *actual_unpack_bytes = 0;

        yaksi_request_s *req;
        rc = yaksi_request_create(ctx, &req);
        YAKSU_ERR_CHECK(rc, fn_fail);

        *request = req->id;
        goto fn_exit;
    }

    yaksi_request_s *yaksi_request;
    yaksi_request = NULL;
    rc = yaksi_request_create(ctx, &yaksi_request);
    YAKSU_ERR_CHECK(rc, fn_fail);

    yaksi_info_s *yaksi_info;
    yaksi_info = (yaksi_info_s *) info;
    rc = yaksi_iunpack(inbuf, insize, outbuf, outcount, yaksi_type, outoffset, actual_unpack_bytes,
                       yaksi_info, yaksi_request);
    YAKSU_ERR_CHECK(rc, fn_fail);

    *request = yaksi_request->id;

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
