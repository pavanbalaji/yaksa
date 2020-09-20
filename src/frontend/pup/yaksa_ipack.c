/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "yaksi.h"
#include "yaksu.h"
#include <assert.h>

int yaksa_ipack(const void *inbuf, uintptr_t incount, yaksa_type_t type, uintptr_t inoffset,
                void *outbuf, uintptr_t max_pack_bytes, uintptr_t * actual_pack_bytes,
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

    if (incount == 0) {
        *actual_pack_bytes = 0;

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
        *actual_pack_bytes = 0;

        yaksi_request_s *req;
        rc = yaksi_request_create(ctx, &req);
        YAKSU_ERR_CHECK(rc, fn_fail);

        *request = req->id;
        goto fn_exit;
    }

    yaksi_request_s *yaksi_request;
    rc = yaksi_request_create(ctx, &yaksi_request);
    YAKSU_ERR_CHECK(rc, fn_fail);

    yaksi_info_s *yaksi_info;
    yaksi_info = (yaksi_info_s *) info;
    rc = yaksi_ipack(inbuf, incount, yaksi_type, inoffset, outbuf, max_pack_bytes,
                     actual_pack_bytes, yaksi_info, yaksi_request);
    YAKSU_ERR_CHECK(rc, fn_fail);

    *request = yaksi_request->id;

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
