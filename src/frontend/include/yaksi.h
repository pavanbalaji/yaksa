/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#ifndef YAKSI_H_INCLUDED
#define YAKSI_H_INCLUDED

#include <stdio.h>
#include <stdbool.h>
#include <sys/uio.h>
#include <yuthash.h>
#include "yaksa_config.h"
#include "yaksa.h"
#include "yaksu.h"
#include "yaksur_pre.h"

#if !defined ATTRIBUTE
#if defined HAVE_GCC_ATTRIBUTE
#define ATTRIBUTE(a_) __attribute__(a_)
#else /* MPL_HAVE_GCC_ATTRIBUTE */
#define ATTRIBUTE(a_)
#endif /* MPL_HAVE_GCC_ATTRIBUTE */
#endif /* ATTRIBUTE */

#define YAKSI_ENV_DEFAULT_NESTING_LEVEL  (3)

extern yaksu_atomic_int yaksi_is_initialized;

typedef enum {
    YAKSI_TYPE_KIND__BUILTIN,
    YAKSI_TYPE_KIND__CONTIG,
    YAKSI_TYPE_KIND__DUP,
    YAKSI_TYPE_KIND__RESIZED,
    YAKSI_TYPE_KIND__HVECTOR,
    YAKSI_TYPE_KIND__BLKHINDX,
    YAKSI_TYPE_KIND__HINDEXED,
    YAKSI_TYPE_KIND__STRUCT,
    YAKSI_TYPE_KIND__SUBARRAY,
} yaksi_type_kind_e;

struct yaksi_type_s;
struct yaksi_request_s;

/*
 * The type handle is divided into the following parts:
 *
 *    32 bits -- context id
 *    32 bits -- type object id
 */
#define YAKSI_TYPE_CONTEXT_ID_BITS              (32)
#define YAKSI_TYPE_OBJECT_ID_BITS               (32)

#define YAKSI_TYPE_ENCODE(handle, ctx_id, obj_id)                       \
    do {                                                                \
        (handle) = ((ctx_id) << YAKSI_TYPE_OBJECT_ID_BITS) + (obj_id);  \
    } while (0)

#define YAKSI_TYPE_DECODE(handle, ctx_id, obj_id)                       \
    do {                                                                \
        (ctx_id) = ((handle) >> YAKSI_TYPE_OBJECT_ID_BITS);             \
        (obj_id) = ((handle) << YAKSI_TYPE_CONTEXT_ID_BITS >> YAKSI_TYPE_CONTEXT_ID_BITS); \
    } while (0)

/*
 * The request handle is divided into the following parts:
 *
 *    32 bits -- context id
 *    32 bits -- request object id
 */
#define YAKSI_REQUEST_CONTEXT_ID_BITS              (32)
#define YAKSI_REQUEST_OBJECT_ID_BITS               (32)

#define YAKSI_REQUEST_ENCODE(handle, ctx_id, obj_id)                    \
    do {                                                                \
        (handle) = ((ctx_id) << YAKSI_REQUEST_OBJECT_ID_BITS) + (obj_id); \
    } while (0)

#define YAKSI_REQUEST_DECODE(handle, ctx_id, obj_id)                    \
    do {                                                                \
        (ctx_id) = ((handle) >> YAKSI_REQUEST_OBJECT_ID_BITS);          \
        (obj_id) = ((handle) << YAKSI_REQUEST_CONTEXT_ID_BITS >> YAKSI_REQUEST_CONTEXT_ID_BITS); \
    } while (0)

/* global variables */
typedef struct {
    yaksu_handle_pool_s context_handle_pool;
} yaksi_global_s;
extern yaksi_global_s yaksi_global;

typedef struct yaksi_context_s {
    yaksu_handle_t id;

    yaksu_handle_pool_s type_handle_pool;
    yaksa_type_t predef_type[YAKSI_TYPE__MAX_PREDEFINED_TYPES];

    /* specially store the null request object for convenience of
     * access */
    struct yaksi_type_s *predef_type_null;

    yaksu_handle_pool_s request_handle_pool;

    /* give some private space for the backend to store content */
    yaksur_context_s backend;

    UT_hash_handle hh;
} yaksi_context_s;

typedef struct yaksi_type_s {
    yaksu_atomic_int refcount;

    yaksi_type_kind_e kind;
    int tree_depth;

    uint8_t alignment;
    uintptr_t size;
    uintptr_t extent;
    intptr_t lb;
    intptr_t ub;
    intptr_t true_lb;
    intptr_t true_ub;

    /* "is_contig" is set to true only when an array of this type is
     * contiguous, not just one element */
    bool is_contig;
    uintptr_t num_contig;

    union {
        struct {
            int count;
            int blocklength;
            intptr_t stride;
            struct yaksi_type_s *child;
        } hvector;
        struct {
            int count;
            int blocklength;
            intptr_t *array_of_displs;
            struct yaksi_type_s *child;
        } blkhindx;
        struct {
            int count;
            int *array_of_blocklengths;
            intptr_t *array_of_displs;
            struct yaksi_type_s *child;
        } hindexed;
        struct {
            int count;
            int *array_of_blocklengths;
            intptr_t *array_of_displs;
            struct yaksi_type_s **array_of_types;
        } str;
        struct {
            struct yaksi_type_s *child;
        } resized;
        struct {
            int count;
            struct yaksi_type_s *child;
        } contig;
        struct {
            int ndims;
            struct yaksi_type_s *primary;
        } subarray;
        struct {
            struct yaksi_type_s *child;
        } dup;
        struct {
            yaksa_type_t handle;
        } builtin;
    } u;

    /* give some private space for the backend to store content */
    yaksur_type_s backend;
} yaksi_type_s;

typedef struct yaksi_request_s {
    yaksu_handle_t id;
    yaksu_atomic_int cc;        /* completion counter */

    /* give some private space for the backend to store content */
    yaksur_request_s backend;

    UT_hash_handle hh;
} yaksi_request_s;

typedef struct yaksi_info_s {
    yaksur_info_s backend;
} yaksi_info_s;


/* pair types */
typedef struct {
    float x;
    int y;
} yaksi_float_int_s;

typedef struct {
    double x;
    int y;
} yaksi_double_int_s;

typedef struct {
    long x;
    int y;
} yaksi_long_int_s;

typedef struct {
    int x;
    int y;
} yaksi_2int_s;

typedef struct {
    short x;
    int y;
} yaksi_short_int_s;

typedef struct {
    long double x;
    int y;
} yaksi_long_double_int_s;

typedef struct {
    float x;
    float y;
} yaksi_c_complex_s;

typedef struct {
    double x;
    double y;
} yaksi_c_double_complex_s;

typedef struct {
    long double x;
    long double y;
} yaksi_c_long_double_complex_s;


/* post headers come after the type declarations */
#include "yaksur_post.h"


/* function declarations come at the very end */
int yaksi_type_create_hvector(int count, int blocklength, intptr_t stride, yaksi_type_s * intype,
                              yaksi_type_s ** outtype);
int yaksi_type_create_contig(int count, yaksi_type_s * intype, yaksi_type_s ** outtype);
int yaksi_type_create_dup(yaksi_type_s * intype, yaksi_type_s ** outtype);
int yaksi_type_create_hindexed(int count, const int *array_of_blocklengths,
                               const intptr_t * array_of_displacements, yaksi_type_s * intype,
                               yaksi_type_s ** outtype);
int yaksi_type_create_hindexed_block(int count, int blocklength,
                                     const intptr_t * array_of_displacements, yaksi_type_s * intype,
                                     yaksi_type_s ** outtype);
int yaksi_type_create_resized(yaksi_type_s * intype, intptr_t lb, uintptr_t extent,
                              yaksi_type_s ** outtype);
int yaksi_type_create_struct(int count, const int *array_of_blocklengths,
                             const intptr_t * array_of_displacements,
                             yaksi_type_s ** array_of_intypes, yaksi_type_s ** outtype);
int yaksi_type_create_subarray(int ndims, const int *array_of_sizes, const int *array_of_subsizes,
                               const int *array_of_starts, yaksa_subarray_order_e order,
                               yaksi_type_s * intype, yaksi_type_s ** outtype);
int yaksi_type_free(yaksi_type_s * type);

int yaksi_ipack(const void *inbuf, uintptr_t incount, yaksi_type_s * type, uintptr_t inoffset,
                void *outbuf, uintptr_t max_pack_bytes, uintptr_t * actual_pack_bytes,
                yaksi_info_s * info, yaksi_request_s * request);
int yaksi_ipack_backend(const void *inbuf, void *outbuf, uintptr_t count, yaksi_type_s * type,
                        yaksi_info_s * info, yaksi_request_s * request);
int yaksi_ipack_element(const void *inbuf, yaksi_type_s * type, uintptr_t inoffset, void *outbuf,
                        uintptr_t max_pack_bytes, uintptr_t * actual_pack_bytes,
                        yaksi_info_s * info, yaksi_request_s * request);

int yaksi_iunpack(const void *inbuf, uintptr_t insize, void *outbuf, uintptr_t outcount,
                  yaksi_type_s * type, uintptr_t outoffset, uintptr_t * actual_unpack_bytes,
                  yaksi_info_s * info, yaksi_request_s * request);
int yaksi_iunpack_backend(const void *inbuf, void *outbuf, uintptr_t outcount, yaksi_type_s * type,
                          yaksi_info_s * info, yaksi_request_s * request);
int yaksi_iunpack_element(const void *inbuf, uintptr_t insize, void *outbuf, yaksi_type_s * type,
                          uintptr_t outoffset, uintptr_t * actual_unpack_bytes,
                          yaksi_info_s * info, yaksi_request_s * request);

int yaksi_iov_len(uintptr_t count, yaksi_type_s * type, uintptr_t * iov_len);
int yaksi_iov(const char *buf, uintptr_t count, yaksi_type_s * type, uintptr_t iov_offset,
              struct iovec *iov, uintptr_t max_iov_len, uintptr_t * actual_iov_len);

int yaksi_flatten_size(yaksi_type_s * type, uintptr_t * flattened_type_size);

/* type pool */
int yaksi_type_handle_alloc(yaksi_context_s * ctx, yaksi_type_s * type, yaksa_type_t * handle);
int yaksi_type_handle_dealloc(yaksa_type_t handle, yaksi_type_s ** type);
int yaksi_type_get(yaksa_type_t type, yaksi_type_s ** yaksi_type);

/* request pool */
int yaksi_request_create(yaksi_context_s * ctx, yaksi_request_s ** request);
int yaksi_request_free(yaksi_request_s * request);
int yaksi_request_get(yaksa_request_t request, yaksi_request_s ** yaksi_request);

#endif /* YAKSI_H_INCLUDED */
