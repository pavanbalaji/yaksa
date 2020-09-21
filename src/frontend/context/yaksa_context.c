/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "yaksi.h"
#include "yaksu.h"
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>
#include <limits.h>
#include <pthread.h>

#define ALLOC_TYPE_HANDLE(ctx, tmp_type_, TYPE, rc, fn_fail)            \
    do {                                                                \
        tmp_type_ = (yaksi_type_s *) malloc(sizeof(yaksi_type_s));      \
        YAKSU_ERR_CHKANDJUMP(!tmp_type_, rc, YAKSA_ERR__OUT_OF_MEM, fn_fail); \
        yaksu_atomic_store(&tmp_type_->refcount, 1);                    \
                                                                        \
        rc = yaksi_type_handle_alloc(ctx, tmp_type_, &ctx->predef_type[YAKSA_TYPE__##TYPE]); \
        YAKSU_ERR_CHECK(rc, fn_fail);                                   \
    } while (0)

#define ASSIGN_TYPE_HANDLE(ctx, OLDTYPE, NEWTYPE, rc, fn_fail)          \
    do {                                                                \
        yaksi_type_s *tmp_type_;                                        \
                                                                        \
        rc = yaksi_type_get(ctx->predef_type[YAKSA_TYPE__##OLDTYPE], &tmp_type_); \
        YAKSU_ERR_CHECK(rc, fn_fail);                                   \
        yaksu_atomic_incr(&tmp_type_->refcount);                        \
                                                                        \
        rc = yaksi_type_handle_alloc(ctx, tmp_type_, &ctx->predef_type[YAKSA_TYPE__##NEWTYPE]); \
        YAKSU_ERR_CHECK(rc, fn_fail);                                   \
    } while (0)

#define INT_MATCH_HANDLE(ctx, ctype, TYPE, rc, fn_fail)                 \
    do {                                                                \
        if (sizeof(ctype) == 1) {                                       \
            ASSIGN_TYPE_HANDLE(ctx, INT8_T, TYPE, rc, fn_fail);         \
        } else if (sizeof(ctype) == 2) {                                \
            ASSIGN_TYPE_HANDLE(ctx, INT16_T, TYPE, rc, fn_fail);        \
        } else if (sizeof(ctype) == 4) {                                \
            ASSIGN_TYPE_HANDLE(ctx, INT32_T, TYPE, rc, fn_fail);        \
        } else if (sizeof(ctype) == 8) {                                \
            ASSIGN_TYPE_HANDLE(ctx, INT64_T, TYPE, rc, fn_fail);        \
        } else {                                                        \
            assert(0);                                                  \
        }                                                               \
    } while (0)

#define INIT_BUILTIN_TYPE(ctx, c_type, TYPE, rc, fn_fail)       \
    do {                                                        \
        yaksi_type_s *tmp_type_;                                \
        ALLOC_TYPE_HANDLE(ctx, tmp_type_, TYPE, rc, fn_fail);   \
                                                                \
        tmp_type_->u.builtin.handle = YAKSA_TYPE__##TYPE;       \
        tmp_type_->kind = YAKSI_TYPE_KIND__BUILTIN;             \
        tmp_type_->tree_depth = 0;                              \
                                                                \
        tmp_type_->size = sizeof(c_type);                       \
        struct {                                                \
            c_type x;                                           \
            char y;                                             \
        } z;                                                    \
        tmp_type_->alignment = sizeof(z) - sizeof(c_type);      \
        tmp_type_->extent = sizeof(c_type);                     \
        tmp_type_->lb = 0;                                      \
        tmp_type_->ub = sizeof(c_type);                         \
        tmp_type_->true_lb = 0;                                 \
        tmp_type_->true_ub = sizeof(c_type);                    \
                                                                \
        tmp_type_->is_contig = true;                            \
        tmp_type_->num_contig = 1;                              \
                                                                \
        yaksur_type_create_hook(tmp_type_);                     \
    } while (0)

#define INIT_BUILTIN_PAIRTYPE(ctx, c_type1, c_type2, c_type, TYPE, rc, fn_fail) \
    do {                                                                \
        yaksi_type_s *tmp_type_;                                        \
        ALLOC_TYPE_HANDLE(ctx, tmp_type_, TYPE, rc, fn_fail);           \
                                                                        \
        c_type z;                                                       \
        bool element_is_contig;                                         \
        if ((char *) &z.y - (char *) &z == sizeof(c_type1))             \
            element_is_contig = true;                                   \
        else                                                            \
            element_is_contig = false;                                  \
                                                                        \
        tmp_type_->u.builtin.handle = YAKSA_TYPE__##TYPE;               \
        tmp_type_->kind = YAKSI_TYPE_KIND__BUILTIN;                     \
        tmp_type_->tree_depth = 0;                                      \
                                                                        \
        tmp_type_->size = sizeof(c_type1) + sizeof(c_type2);            \
        struct {                                                        \
            c_type1 x;                                                  \
            char y;                                                     \
        } z1;                                                           \
        struct {                                                        \
            c_type2 x;                                                  \
            char y;                                                     \
        } z2;                                                           \
        tmp_type_->alignment = YAKSU_MAX(sizeof(z1) - sizeof(c_type1), sizeof(z2) - sizeof(c_type2)); \
        tmp_type_->extent = sizeof(c_type);                             \
        tmp_type_->lb = 0;                                              \
        tmp_type_->ub = sizeof(c_type);                                 \
        tmp_type_->true_lb = 0;                                         \
        if (element_is_contig)                                          \
            tmp_type_->true_ub = tmp_type_->size;                       \
        else                                                            \
            tmp_type_->true_ub = sizeof(c_type);                        \
                                                                        \
        if (tmp_type_->size == tmp_type_->extent) {                     \
            tmp_type_->is_contig = true;                                \
        } else {                                                        \
            tmp_type_->is_contig = false;                               \
        }                                                               \
        tmp_type_->num_contig = 1 + !element_is_contig;                 \
                                                                        \
        yaksur_type_create_hook(tmp_type_);                             \
    } while (0)

yaksi_global_s yaksi_global = { 0 };

yaksu_atomic_int yaksi_is_initialized = YAKSU_ATOMIC_VAR_INIT(0);
static pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;

static int one_time_init(void)
{
    int rc = YAKSA_SUCCESS;

    pthread_mutex_lock(&init_mutex);

    if (yaksu_atomic_incr(&yaksi_is_initialized)) {
        goto fn_exit;
    }

    rc = yaksu_handle_pool_alloc(&yaksi_global.context_handle_pool);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksur_init_hook();
    YAKSU_ERR_CHECK(rc, fn_fail);

  fn_exit:
    pthread_mutex_unlock(&init_mutex);
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksa_context_create(yaksa_info_t info, yaksa_context_t * context)
{
    int rc = YAKSA_SUCCESS;
    yaksi_context_s *ctx;

    /************************************************************************/
    /* Initialize global structures */
    /************************************************************************/
    rc = one_time_init();
    YAKSU_ERR_CHECK(rc, fn_fail);


    /************************************************************************/
    /* Allocate and populate the context */
    /************************************************************************/
    ctx = (yaksi_context_s *) malloc(sizeof(yaksi_context_s));
    YAKSU_ERR_CHKANDJUMP(!ctx, rc, YAKSA_ERR__OUT_OF_MEM, fn_fail);

    rc = yaksu_handle_pool_elem_alloc(yaksi_global.context_handle_pool, &ctx->id, ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksur_context_create_hook(ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    *context = (yaksa_context_t) ctx->id;

    /* populate the context */
    rc = yaksu_handle_pool_alloc(&ctx->type_handle_pool);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_alloc(&ctx->request_handle_pool);
    YAKSU_ERR_CHECK(rc, fn_fail);

    /* predefined types */
    ALLOC_TYPE_HANDLE(ctx, ctx->predef_type_null, NULL, rc, fn_fail);

    ctx->predef_type_null->u.builtin.handle = YAKSA_TYPE__NULL;
    ctx->predef_type_null->kind = YAKSI_TYPE_KIND__BUILTIN;
    ctx->predef_type_null->tree_depth = 0;
    ctx->predef_type_null->size = 0;
    ctx->predef_type_null->alignment = 1;
    ctx->predef_type_null->extent = 0;
    ctx->predef_type_null->lb = 0;
    ctx->predef_type_null->ub = 0;
    ctx->predef_type_null->true_lb = 0;
    ctx->predef_type_null->true_ub = 0;

    ctx->predef_type_null->is_contig = true;
    ctx->predef_type_null->num_contig = 0;
    yaksur_type_create_hook(ctx->predef_type_null);

    INIT_BUILTIN_TYPE(ctx, _Bool, _BOOL, rc, fn_fail);

    INIT_BUILTIN_TYPE(ctx, char, CHAR, rc, fn_fail);
    ASSIGN_TYPE_HANDLE(ctx, CHAR, SIGNED_CHAR, rc, fn_fail);
    ASSIGN_TYPE_HANDLE(ctx, CHAR, UNSIGNED_CHAR, rc, fn_fail);
    INIT_BUILTIN_TYPE(ctx, wchar_t, WCHAR_T, rc, fn_fail);

    INIT_BUILTIN_TYPE(ctx, int8_t, INT8_T, rc, fn_fail);
    INIT_BUILTIN_TYPE(ctx, int16_t, INT16_T, rc, fn_fail);
    INIT_BUILTIN_TYPE(ctx, int32_t, INT32_T, rc, fn_fail);
    INIT_BUILTIN_TYPE(ctx, int64_t, INT64_T, rc, fn_fail);
    ASSIGN_TYPE_HANDLE(ctx, INT8_T, UINT8_T, rc, fn_fail);
    ASSIGN_TYPE_HANDLE(ctx, INT16_T, UINT16_T, rc, fn_fail);
    ASSIGN_TYPE_HANDLE(ctx, INT32_T, UINT32_T, rc, fn_fail);
    ASSIGN_TYPE_HANDLE(ctx, INT64_T, UINT64_T, rc, fn_fail);

    INT_MATCH_HANDLE(ctx, int, INT, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, unsigned, UNSIGNED, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, short, SHORT, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, unsigned short, UNSIGNED_SHORT, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, long, LONG, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, unsigned long, UNSIGNED_LONG, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, long long, LONG_LONG, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, unsigned long long, UNSIGNED_LONG_LONG, rc, fn_fail);

    INT_MATCH_HANDLE(ctx, int_fast8_t, INT_FAST8_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, int_fast16_t, INT_FAST16_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, int_fast32_t, INT_FAST32_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, int_fast64_t, INT_FAST64_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uint_fast8_t, UINT_FAST8_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uint_fast16_t, UINT_FAST16_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uint_fast32_t, UINT_FAST32_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uint_fast64_t, UINT_FAST64_T, rc, fn_fail);

    INT_MATCH_HANDLE(ctx, int_least8_t, INT_LEAST8_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, int_least16_t, INT_LEAST16_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, int_least32_t, INT_LEAST32_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, int_least64_t, INT_LEAST64_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uint_least8_t, UINT_LEAST8_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uint_least16_t, UINT_LEAST16_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uint_least32_t, UINT_LEAST32_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uint_least64_t, UINT_LEAST64_T, rc, fn_fail);

    ASSIGN_TYPE_HANDLE(ctx, UINT8_T, BYTE, rc, fn_fail);

    INT_MATCH_HANDLE(ctx, intmax_t, INTMAX_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uintmax_t, UINTMAX_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, size_t, SIZE_T, rc, fn_fail);

    INT_MATCH_HANDLE(ctx, intptr_t, INTPTR_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, uintptr_t, UINTPTR_T, rc, fn_fail);
    INT_MATCH_HANDLE(ctx, ptrdiff_t, PTRDIFF_T, rc, fn_fail);

    INIT_BUILTIN_TYPE(ctx, float, FLOAT, rc, fn_fail);
    INIT_BUILTIN_TYPE(ctx, double, DOUBLE, rc, fn_fail);
    INIT_BUILTIN_TYPE(ctx, long double, LONG_DOUBLE, rc, fn_fail);

    INIT_BUILTIN_PAIRTYPE(ctx, float, float, yaksi_c_complex_s, C_COMPLEX, rc, fn_fail);
    INIT_BUILTIN_PAIRTYPE(ctx, double, double, yaksi_c_double_complex_s, C_DOUBLE_COMPLEX, rc,
                          fn_fail);
    INIT_BUILTIN_PAIRTYPE(ctx, long double, long double, yaksi_c_long_double_complex_s,
                          C_LONG_DOUBLE_COMPLEX, rc, fn_fail);
    INIT_BUILTIN_PAIRTYPE(ctx, float, int, yaksi_float_int_s, FLOAT_INT, rc, fn_fail);
    INIT_BUILTIN_PAIRTYPE(ctx, double, int, yaksi_double_int_s, DOUBLE_INT, rc, fn_fail);
    INIT_BUILTIN_PAIRTYPE(ctx, long, int, yaksi_long_int_s, LONG_INT, rc, fn_fail);
    /* *INDENT-OFF* */
    INIT_BUILTIN_PAIRTYPE(ctx, int, int, yaksi_2int_s, 2INT, rc, fn_fail);
    /* *INDENT-ON* */
    INIT_BUILTIN_PAIRTYPE(ctx, short, int, yaksi_short_int_s, SHORT_INT, rc, fn_fail);
    INIT_BUILTIN_PAIRTYPE(ctx, long double, int, yaksi_long_double_int_s, LONG_DOUBLE_INT, rc,
                          fn_fail);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

static int one_time_finalize(void)
{
    int rc = YAKSA_SUCCESS;

    pthread_mutex_lock(&init_mutex);

    if (yaksu_atomic_decr(&yaksi_is_initialized) > 1) {
        goto fn_exit;
    }

    /* finalize the backend */
    rc = yaksur_finalize_hook();
    YAKSU_ERR_CHECK(rc, fn_fail);

  fn_exit:
    pthread_mutex_unlock(&init_mutex);
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksa_context_free(yaksa_context_t context)
{
    int rc = YAKSA_SUCCESS;
    yaksi_context_s *ctx;

    rc = yaksu_handle_pool_elem_get(yaksi_global.context_handle_pool, context,
                                    (const void **) &ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    /* free the builtin datatypes */
    for (int i = 0; i < YAKSI_TYPE__MAX_PREDEFINED_TYPES; i++) {
        yaksi_type_s *tmp;

        rc = yaksi_type_handle_dealloc(ctx->predef_type[i], &tmp);
        YAKSU_ERR_CHECK(rc, fn_fail);

        rc = yaksi_type_free(tmp);
        YAKSU_ERR_CHECK(rc, fn_fail);
    }

    rc = yaksur_context_free_hook(ctx);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_free(ctx->type_handle_pool);
    YAKSU_ERR_CHECK(rc, fn_fail);

    rc = yaksu_handle_pool_free(ctx->request_handle_pool);
    YAKSU_ERR_CHECK(rc, fn_fail);

    /* free global structures */
    rc = one_time_finalize();
    YAKSU_ERR_CHECK(rc, fn_fail);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
