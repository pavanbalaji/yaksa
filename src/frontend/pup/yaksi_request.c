/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "yaksi.h"
#include "yaksu.h"
#include <assert.h>

int yaksi_request_create(yaksi_context_s * ctx, yaksi_request_s ** request)
{
    int rc = YAKSA_SUCCESS;
    yaksi_request_s *req;
    yaksu_handle_t obj_id;

    req = (yaksi_request_s *) malloc(sizeof(yaksi_request_s));
    YAKSU_ERR_CHKANDJUMP(!req, rc, YAKSA_ERR__OUT_OF_MEM, fn_fail);

    rc = yaksu_handle_pool_elem_alloc(ctx->request_handle_pool, &obj_id, req);
    YAKSU_ERR_CHECK(rc, fn_fail);

    assert(obj_id < ((yaksa_request_t) 1 << YAKSI_REQUEST_OBJECT_ID_BITS));
    YAKSI_REQUEST_ENCODE(req->id, ctx->id, obj_id);

    yaksu_atomic_store(&req->cc, 0);

    rc = yaksur_request_create_hook(req);
    YAKSU_ERR_CHECK(rc, fn_fail);

    *request = req;

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksi_request_free(yaksi_request_s * request)
{
    int rc = YAKSA_SUCCESS;
    yaksu_handle_t obj_id;
    yaksa_context_t ctx_id;
    yaksi_context_s *ctx;

    YAKSI_REQUEST_DECODE(request->id, ctx_id, obj_id);

    rc = yaksur_request_free_hook(request);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_elem_get(yaksi_global.context_handle_pool, ctx_id, (const void **) &ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_elem_free(ctx->request_handle_pool, obj_id);
    YAKSU_ERR_CHECK(rc, fn_fail);

    free(request);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksi_request_get(yaksa_request_t request, struct yaksi_request_s **yaksi_request)
{
    int rc = YAKSA_SUCCESS;
    yaksu_handle_t obj_id;
    yaksa_context_t ctx_id;
    yaksi_context_s *ctx;

    YAKSI_REQUEST_DECODE(request, ctx_id, obj_id);

    rc = yaksu_handle_pool_elem_get(yaksi_global.context_handle_pool, ctx_id, (const void **) &ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_elem_get(ctx->request_handle_pool, obj_id,
                                    (const void **) yaksi_request);
    YAKSU_ERR_CHECK(rc, fn_fail);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
