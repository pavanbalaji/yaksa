/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include <assert.h>
#include "yaksa.h"
#include "yaksi.h"
#include "yaksu.h"
#include "yaksuri.h"

static int get_ptr_attr(const void *buf, yaksur_ptr_attr_s * ptrattr, yaksuri_gpudriver_id_e * id)
{
    int rc = YAKSA_SUCCESS;

    /* Each GPU backend can claim "ownership" of the input buffer */
    for (*id = YAKSURI_GPUDRIVER_ID__UNSET; *id < YAKSURI_GPUDRIVER_ID__LAST; (*id)++) {
        if (*id == YAKSURI_GPUDRIVER_ID__UNSET)
            continue;

        if (yaksuri_global.gpudriver[*id].info) {
            rc = yaksuri_global.gpudriver[*id].info->get_ptr_attr(buf, ptrattr);
            YAKSU_ERR_CHECK(rc, fn_fail);

            if (ptrattr->type == YAKSUR_PTR_TYPE__GPU ||
                ptrattr->type == YAKSUR_PTR_TYPE__REGISTERED_HOST)
                break;
        }
    }

    if (*id == YAKSURI_GPUDRIVER_ID__LAST) {
        *id = YAKSURI_GPUDRIVER_ID__UNSET;
        ptrattr->type = YAKSUR_PTR_TYPE__UNREGISTERED_HOST;
    }

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

/********************************************************************************/
/*
 * Understanding yaksa requests --
 *
 * Every user pup operation is associated with a request.  Each
 * request has zero or more subrequests.  Each subrequest has zero or
 * more chunks.  The user only sees the request.  The subrequests and
 * chunks are completely internal to yaksa.
 *
 * The frontend breaks down a single pup operation (associated with a
 * single request) into multiple suboperations.  Each such
 * suboperation can have a subrequest associated with it.  The
 * frontend makes a separate yaksur_ipack or yaksur_iunpack for each
 * suboperation.  So, these functions might get called multiple times
 * with the same user request.
 *
 * There are three types of operations associated with each request:
 *
 *   1. Blocking operations: these operations complete immediately.
 *      They do not need us to maintain any resources or to maintain
 *      any state.  These operations still have a user request
 *      associated with them, but the request does not have any
 *      internal subrequests.
 *
 *   2. Direct operations: these are nonblocking operations, so we do
 *      need to maintain some state, but they do not require any
 *      additional resources (such as temporary buffers), so the state
 *      is small.  Each direct operation has one subrequest associated
 *      with it, but the subrequest does not have any chunks attached
 *      to it.
 *
 *   3. Indirect operations: these are nonblocking operations that
 *      need additional resources (such as temporary buffers).  We
 *      maintain a full progress engine for such operations because
 *      depending on the available resources, we might issue them
 *      partially.  Each indirect operation has one subrequest
 *      associated with it, and the subrequest has some non-zero
 *      number of chunks attached to it.
 *
 * Note that a single request can have some subrequests associated
 * with direct operations and some subrequests associated with
 * indirect operations.  But a single request cannot have some
 * blocking and some nonblocking operations.
 */
/********************************************************************************/

static int init_request(const void *inbuf, void *outbuf, yaksi_type_s * type,
                        yaksi_info_s * info, yaksi_request_s * request, yaksuri_puptype_e puptype)
{
    int rc = YAKSA_SUCCESS;
    yaksuri_request_s *request_backend = (yaksuri_request_s *) request->backend.priv;

    if (request_backend->kind == YAKSURI_REQUEST_KIND__UNSET) {
        yaksuri_gpudriver_id_e inbuf_gpudriver, outbuf_gpudriver;
        yaksur_ptr_attr_s inattr, outattr;

        rc = get_ptr_attr((const char *) inbuf + type->true_lb, &inattr, &inbuf_gpudriver);
        YAKSU_ERR_CHECK(rc, fn_fail);

        rc = get_ptr_attr(outbuf, &outattr, &outbuf_gpudriver);
        YAKSU_ERR_CHECK(rc, fn_fail);

        /* figure out the kind of operation this is */
        if (inattr.type == YAKSUR_PTR_TYPE__GPU) {
            if (outattr.type == YAKSUR_PTR_TYPE__GPU) {
                /* D2D operation */
                if (inattr.device == outattr.device) {
                    if (puptype == YAKSURI_PUPTYPE__PACK) {
                        request_backend->kind = YAKSURI_REQUEST_KIND__PACK_D2D_SINGLE;
                    } else {
                        request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_D2D_SINGLE;
                    }
                } else {
                    bool ipc_enabled;
                    rc = yaksuri_global.gpudriver[inbuf_gpudriver].info->check_p2p_comm(inattr.device,
                                                                                        outattr.device,
                                                                                        &ipc_enabled);
                    YAKSU_ERR_CHECK(rc, fn_fail);

                    if (ipc_enabled) {
                        if (puptype == YAKSURI_PUPTYPE__PACK) {
                            request_backend->kind = YAKSURI_REQUEST_KIND__PACK_D2D_IPC;
                        } else {
                            request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_D2D_IPC;
                        }
                    } else {
                        if (puptype == YAKSURI_PUPTYPE__PACK) {
                            request_backend->kind = YAKSURI_REQUEST_KIND__PACK_D2D_STAGED;
                        } else {
                            request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_D2D_STAGED;
                        }
                    }
                }
            } else if (outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) {
                    /* D2RH operation */
                if (puptype == YAKSURI_PUPTYPE__PACK) {
                    request_backend->kind = YAKSURI_REQUEST_KIND__PACK_D2RH;
                } else {
                    request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_D2RH;
                }
            } else if (outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
                /* D2URH operation */
                if (puptype == YAKSURI_PUPTYPE__PACK) {
                    request_backend->kind = YAKSURI_REQUEST_KIND__PACK_D2URH;
                } else {
                    request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_D2URH;
                }
            }
        } else if (inattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) {
            if (outattr.type == YAKSUR_PTR_TYPE__GPU) {
                /* RH2D operation */
                if (puptype == YAKSURI_PUPTYPE__PACK) {
                    request_backend->kind = YAKSURI_REQUEST_KIND__PACK_RH2D;
                } else {
                    request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_RH2D;
                }
            } else if (outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST ||
                       outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
                /* H2H operation */
                if (puptype == YAKSURI_PUPTYPE__PACK) {
                    request_backend->kind = YAKSURI_REQUEST_KIND__PACK_H2H;
                } else {
                    request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_H2H;
                }
            }
        } else if (inattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
            if (outattr.type == YAKSUR_PTR_TYPE__GPU) {
                /* URH2D operation */
                if (puptype == YAKSURI_PUPTYPE__PACK) {
                    request_backend->kind = YAKSURI_REQUEST_KIND__PACK_URH2D;
                } else {
                    request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_URH2D;
                }
            } else if (outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST ||
                       outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
                /* H2H operation */
                if (puptype == YAKSURI_PUPTYPE__PACK) {
                    request_backend->kind = YAKSURI_REQUEST_KIND__PACK_H2H;
                } else {
                    request_backend->kind = YAKSURI_REQUEST_KIND__UNPACK_H2H;
                }
            }
        }

        request_backend->inattr = inattr;
        request_backend->outattr = outattr;

        if (inbuf_gpudriver == YAKSURI_GPUDRIVER_ID__UNSET &&
            outbuf_gpudriver == YAKSURI_GPUDRIVER_ID__UNSET) {
            request_backend->gpudriver_id = YAKSURI_GPUDRIVER_ID__UNSET;
        } else if (inbuf_gpudriver != YAKSURI_GPUDRIVER_ID__UNSET) {
            request_backend->id = inbuf_gpudriver;
        } else if (outbuf_gpudriver != YAKSURI_GPUDRIVER_ID__UNSET) {
            request_backend->id = outbuf_gpudriver;
        }

        request_backend->subreqs = NULL;
    }

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

static int ipup(const void *inbuf, void *outbuf, uintptr_t count, yaksi_type_s * type,
                yaksi_info_s * info, yaksi_request_s * request, yaksuri_puptype_e puptype)
{
    int rc = YAKSA_SUCCESS;
    yaksuri_request_s *request_backend = (yaksuri_request_s *) request->backend.priv;

    rc = init_request(inbuf, outbuf, type, info, request, puptype);
    YAKSU_ERR_CHECK(rc, fn_fail);

    /********************************************************************************/
    /* Blocking operations */
    /********************************************************************************/
    bool is_supported;
    rc = yaksuri_seq_pup_is_supported(type, &is_supported);
    YAKSU_ERR_CHECK(rc, fn_fail);

    if (!is_supported) {
        rc = YAKSA_ERR__NOT_SUPPORTED;
        goto fn_exit;
    } else if (request_backend->kind == YAKSURI_REQUEST_KIND__PACK_H2H) {
        rc = yaksuri_seq_ipack(inbuf, outbuf, count, type, info);
        YAKSU_ERR_CHECK(rc, fn_fail);
        goto fn_exit;
    } else if (request_backend->kind == YAKSURI_REQUEST_KIND__UNPACK_H2H) {
        rc = yaksuri_seq_iunpack(inbuf, outbuf, count, type, info);
        YAKSU_ERR_CHECK(rc, fn_fail);
        goto fn_exit;
    }

    /********************************************************************************/
    /* Nonblocking operations */
    /********************************************************************************/

    /* if we are here, one of the GPU backends should have claimed the
     * buffers */
    yaksuri_gpudriver_id_e id;
    id = request_backend->gpudriver_id;
    assert(id != YAKSURI_GPUDRIVER_ID__UNSET);
    assert(yaksuri_global.gpudriver[id].info);

    /* if the GPU backend cannot support this type, return */
    bool is_supported;
    rc = yaksuri_global.gpudriver[id].info->pup_is_supported(type, &is_supported);
    YAKSU_ERR_CHECK(rc, fn_fail);

    if (!is_supported) {
        rc = YAKSA_ERR__NOT_SUPPORTED;
    } else {
        rc = yaksuri_progress_enqueue(inbuf, outbuf, count, type, info, request);
        YAKSU_ERR_CHECK(rc, fn_fail);
    }

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksur_ipack(const void *inbuf, void *outbuf, uintptr_t count, yaksi_type_s * type,
                 yaksi_info_s * info, yaksi_request_s * request)
{
    int rc = YAKSA_SUCCESS;

    rc = ipup(inbuf, outbuf, count, type, info, request, YAKSURI_PUPTYPE__PACK);
    YAKSU_ERR_CHECK(rc, fn_fail);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksur_iunpack(const void *inbuf, void *outbuf, uintptr_t count, yaksi_type_s * type,
                   yaksi_info_s * info, yaksi_request_s * request)
{
    int rc = YAKSA_SUCCESS;

    rc = ipup(inbuf, outbuf, count, type, info, request, YAKSURI_PUPTYPE__UNPACK);
    YAKSU_ERR_CHECK(rc, fn_fail);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
