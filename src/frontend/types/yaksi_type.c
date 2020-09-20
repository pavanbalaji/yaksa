/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "yaksi.h"
#include "yaksu.h"
#include <assert.h>

int yaksi_type_handle_alloc(yaksi_context_s * ctx, yaksi_type_s * type, yaksa_type_t * handle)
{
    int rc = YAKSA_SUCCESS;
    yaksu_handle_t obj_id;

    rc = yaksu_handle_pool_elem_alloc(ctx->type_handle_pool, &obj_id, type);
    YAKSU_ERR_CHECK(rc, fn_fail);

    YAKSI_TYPE_ENCODE(*handle, ctx->id, obj_id);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksi_type_handle_dealloc(yaksa_type_t handle, yaksi_type_s ** type)
{
    int rc = YAKSA_SUCCESS;
    yaksu_handle_t obj_id;
    yaksa_context_t ctx_id;
    yaksi_context_s *ctx;

    YAKSI_TYPE_DECODE(handle, ctx_id, obj_id);

    rc = yaksu_handle_pool_elem_get(yaksi_global.context_handle_pool, ctx_id, (const void **) &ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_elem_get(ctx->type_handle_pool, obj_id, (const void **) type);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_elem_free(ctx->type_handle_pool, obj_id);
    YAKSU_ERR_CHECK(rc, fn_fail);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksi_type_get(yaksa_type_t handle, yaksi_type_s ** type)
{
    int rc = YAKSA_SUCCESS;
    yaksu_handle_t obj_id;
    yaksa_context_t ctx_id;
    yaksi_context_s *ctx;

    YAKSI_TYPE_DECODE(handle, ctx_id, obj_id);

    rc = yaksu_handle_pool_elem_get(yaksi_global.context_handle_pool, ctx_id, (const void **) &ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_elem_get(ctx->type_handle_pool, obj_id, (const void **) type);
    YAKSU_ERR_CHECK(rc, fn_fail);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
