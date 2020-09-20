/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "yaksi.h"
#include "yaksu.h"
#include <stdlib.h>
#include <assert.h>

int yaksa_type_get_predefined(yaksa_context_t context, unsigned int type_seed, yaksa_type_t * type)
{
    int rc = YAKSA_SUCCESS;
    yaksi_context_s *ctx;

    assert(yaksu_atomic_load(&yaksi_is_initialized));

    rc = yaksu_handle_pool_elem_get(yaksi_global.context_handle_pool, context,
                                    (const void **) &ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    *type = ctx->predef_type[type_seed];

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksa_type_get_predefined_seed(yaksa_type_t type, unsigned int *type_seed)
{
    int rc = YAKSA_SUCCESS;

    assert(yaksu_atomic_load(&yaksi_is_initialized));

    yaksu_handle_t obj_id;
    yaksa_context_t ctx_id;
    YAKSI_TYPE_DECODE(type, ctx_id, obj_id);

    assert(obj_id < YAKSI_TYPE__MAX_PREDEFINED_TYPES);

    *type_seed = (unsigned int) obj_id;

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
