/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <limits.h>
#include <pthread.h>
#include "yaksa.h"
#include "yaksi.h"
#include "yaksu.h"
#include "yaksuri.h"

typedef struct progress_elem_s {
    yaksi_request_s *request;
    UT_hash_handle hh;
} progress_elem_s;

static progress_elem_s *progress_list = NULL;
static pthread_mutex_t progress_mutex = PTHREAD_MUTEX_INITIALIZER;

#define TMPBUF_SLAB_SIZE  (16 * 1024 * 1024)

/* the dequeue function is not thread safe, as it is always called
 * from within the progress engine */
static int progress_dequeue(progress_elem_s * elem)
{
    int rc = YAKSA_SUCCESS;

    assert(progress_head);

    if (progress_head == elem && progress_tail == elem) {
        progress_head = progress_tail = NULL;
    } else if (progress_head == elem) {
        progress_head = progress_head->next;
    } else {
        progress_elem_s *tmp;
        for (tmp = progress_head; tmp->next; tmp = tmp->next)
            if (tmp->next == elem)
                break;
        assert(tmp->next);
        tmp->next = tmp->next->next;
        if (tmp->next == NULL)
            progress_tail = tmp;
    }

    yaksu_atomic_decr(&elem->request->cc);
    free(elem);

    return rc;
}

static void enqueue_subreq(yaksi_request_s * request, yaksuri_subreq_s *subreq)
{
    yaksu_atomic_incr(&request->cc);

    pthread_mutex_lock(&progress_mutex);
    DL_APPEND(request->subreqs, subreq);

    progress_elem_s *elem;
    HASH_FIND_PTR(progress_list, &request, elem);
    if (elem == NULL) {
        elem = (progress_elem_s *) malloc(sizeof(progress_elem_s));
        elem->request = request;
        HASH_ADD_PTR(progress_list, request, elem);
    }

    pthread_mutex_unlock(&progress_mutex);
}

int yaksuri_progress_enqueue_direct(yaksi_request_s * request, void *event)
{
    int rc = YAKSA_SUCCESS;

    yaksuri_subreq_s *subreq;
    subreq = (yaksuri_subreq_s *) malloc(sizeof(yaksuri_subreq_s));

    subreq->kind = YAKSURI_SUBREQ_KIND__DIRECT;
    subreq->u.direct.event = event;

    enqueue_subreq(request, subreq);

    return rc;
}

int yaksuri_progress_enqueue_indirect(const void *inbuf, void *outbuf, uintptr_t count, yaksi_type_s * type,
                                      yaksi_info_s * info, yaksi_request_s * request,
                                      yaksur_ptr_attr_s inattr, yaksur_ptr_attr_s outattr,
                                      yaksuri_puptype_e puptype)
{
    int rc = YAKSA_SUCCESS;

    /* if we need to go through the progress engine, make sure we only
     * take on types, where at least one count of the type fits into
     * our temporary buffers. */
    if (type->size > TMPBUF_SLAB_SIZE) {
        rc = YAKSA_ERR__NOT_SUPPORTED;
        goto fn_exit;
    }

    yaksuri_subreq_s *subreq;
    subreq = (yaksuri_subreq_s *) malloc(sizeof(yaksuri_subreq_s));

    if (type->contig) {
        if (inattr.type == YAKSUR_PTR_TYPE__GPU) {
            subreq->kind = YAKSURI_SUBREQ_KIND__COPY_D2URH;
            subreq->u.copy_d2urh.rh = NULL;
            subreq->u.copy_d2urh.events = NULL;
        } else if (outattr.type == YAKSUR_PTR_TYPE__GPU) {
            subreq->kind = YAKSURI_SUBREQ_KIND__COPY_URH2D;
            subreq->u.copy_urh2d.rh = NULL;
            subreq->u.copy_urh2d.events = NULL;
        }
    } else if (puptype == YAKSURI_PUPTYPE__PACK) {
        if (inattr.type == YAKSUR_PTR_TYPE__GPU) {
            if (outattr.type == YAKSUR_PTR_TYPE__GPU) {
                subreq->kind = YAKSURI_SUBREQ_KIND__PACK_D2D_IPC;
                subreq->u.pack_d2d_ipc.src_d = NULL;
                subreq->u.pack_d2d_ipc.events = NULL;
            } else if (outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) {
                subreq->kind = YAKSURI_SUBREQ_KIND__PACK_D2RH;
                subreq->u.pack_d2rh.src_d = NULL;
                subreq->u.pack_d2rh.events = NULL;
            } else if (outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
                subreq->kind = YAKSURI_SUBREQ_KIND__PACK_D2URH;
                subreq->u.pack_d2urh.src_d = NULL;
                subreq->u.pack_d2urh.rh = NULL;
                subreq->u.pack_d2urh.events = NULL;
            }
        } else if (outattr.type == YAKSUR_PTR_TYPE__GPU) {
            if (inattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) {
                subreq->kind = YAKSURI_SUBREQ_KIND__PACK_RH2D;
                subreq->u.pack_rh2d.rh = NULL;
                subreq->u.pack_rh2d.events = NULL;
            } else if (inattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
                subreq->kind = YAKSURI_SUBREQ_KIND__PACK_URH2D;
                subreq->u.pack_urh2d.rh = NULL;
                subreq->u.pack_urh2d.events = NULL;
            }
        }
    } else if (puptype == YAKSURI_PUPTYPE__UNPACK) {
        if (inattr.type == YAKSUR_PTR_TYPE__GPU) {
            if (outattr.type == YAKSUR_PTR_TYPE__GPU) {
                subreq->kind = YAKSURI_SUBREQ_KIND__UNPACK_D2D_IPC;
                subreq->u.unpack_d2d_ipc.dst_d = NULL;
                subreq->u.unpack_d2d_ipc.events = NULL;
            } else if (outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) {
                subreq->kind = YAKSURI_SUBREQ_KIND__UNPACK_D2RH;
                subreq->u.unpack_d2rh.rh = NULL;
                subreq->u.unpack_d2rh.events = NULL;
            } else if (outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
                subreq->kind = YAKSURI_SUBREQ_KIND__UNPACK_D2URH;
                subreq->u.unpack_d2urh.rh = NULL;
                subreq->u.unpack_d2urh.events = NULL;
            }
        } else if (outattr.type == YAKSUR_PTR_TYPE__GPU) {
            if (inattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) {
                subreq->kind = YAKSURI_SUBREQ_KIND__UNPACK_RH2D;
                subreq->u.unpack_rh2d.dst_d = NULL;
                subreq->u.unpack_rh2d.events = NULL;
            } else if (inattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
                subreq->kind = YAKSURI_SUBREQ_KIND__UNPACK_URH2D;
                subreq->u.unpack_urh2d.rh = NULL;
                subreq->u.unpack_urh2d.dst_d = NULL;
                subreq->u.unpack_urh2d.events = NULL;
            }
        }
    }

    subreq->state.inbuf = inbuf;
    subreq->state.outbuf = outbuf;
    subreq->state.count = count;
    subreq->state.type = type;
    subreq->state.info = info;
    subreq->state.offset = 0;

    enqueue_subreq(request, subreq);

  fn_exit:
    return rc;
}

static int alloc_subop(progress_subop_s ** subop)
{
    int rc = YAKSA_SUCCESS;
    progress_elem_s *elem = progress_head;
    uintptr_t gpu_tmpbuf_offset = 0, host_tmpbuf_offset = 0;
    uintptr_t nelems = UINTPTR_MAX;
    yaksuri_request_s *request_backend = (yaksuri_request_s *) elem->request->backend.priv;
    yaksuri_gpudriver_id_e id = request_backend->gpudriver_id;
    bool need_gpu_tmpbuf = false, need_host_tmpbuf = false;
    int devid = INT_MIN;

    if ((elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST)) {
        need_host_tmpbuf = true;
    }

    if (elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
        elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) {
        bool is_enabled;
        rc = yaksuri_global.gpudriver[id].info->check_p2p_comm(elem->pup.inattr.device,
                                                               elem->pup.outattr.device,
                                                               &is_enabled);
        YAKSU_ERR_CHECK(rc, fn_fail);

        if (!is_enabled) {
            need_host_tmpbuf = true;
        }
    }

    if ((elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU)) {
        need_gpu_tmpbuf = true;
        devid = elem->pup.inattr.device;
    }

    if ((elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) ||
        (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
         elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
         elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU)) {
        need_gpu_tmpbuf = true;
        devid = elem->pup.outattr.device;
    }

    assert(need_host_tmpbuf || need_gpu_tmpbuf);

    *subop = NULL;

    /* figure out if we actually have enough buffer space */
    if (need_gpu_tmpbuf) {
        uintptr_t d_nelems;
        if (yaksuri_global.gpudriver[id].device[devid].slab_head_offset == 0 &&
            yaksuri_global.gpudriver[id].device[devid].slab_tail_offset == 0) {
            d_nelems = TMPBUF_SLAB_SIZE / elem->pup.type->size;
            gpu_tmpbuf_offset = yaksuri_global.gpudriver[id].device[devid].slab_tail_offset;
        } else if (yaksuri_global.gpudriver[id].device[devid].slab_tail_offset >
                   yaksuri_global.gpudriver[id].device[devid].slab_head_offset) {
            uintptr_t count =
                (TMPBUF_SLAB_SIZE -
                 yaksuri_global.gpudriver[id].device[devid].slab_tail_offset) /
                elem->pup.type->size;
            if (count) {
                d_nelems = count;
                gpu_tmpbuf_offset = yaksuri_global.gpudriver[id].device[devid].slab_tail_offset;
            } else {
                d_nelems =
                    yaksuri_global.gpudriver[id].device[devid].slab_head_offset /
                    elem->pup.type->size;
                gpu_tmpbuf_offset = 0;
            }
        } else {
            d_nelems =
                (yaksuri_global.gpudriver[id].device[devid].slab_head_offset -
                 yaksuri_global.gpudriver[id].device[devid].slab_tail_offset) /
                elem->pup.type->size;
            gpu_tmpbuf_offset = yaksuri_global.gpudriver[id].device[devid].slab_tail_offset;
        }

        if (nelems > d_nelems)
            nelems = d_nelems;
    }

    if (need_host_tmpbuf) {
        uintptr_t h_nelems;
        if (yaksuri_global.gpudriver[id].host.slab_head_offset == 0 &&
            yaksuri_global.gpudriver[id].host.slab_tail_offset == 0) {
            h_nelems = TMPBUF_SLAB_SIZE / elem->pup.type->size;
            host_tmpbuf_offset = yaksuri_global.gpudriver[id].host.slab_tail_offset;
        } else if (yaksuri_global.gpudriver[id].host.slab_tail_offset >
                   yaksuri_global.gpudriver[id].host.slab_head_offset) {
            uintptr_t count =
                (TMPBUF_SLAB_SIZE -
                 yaksuri_global.gpudriver[id].host.slab_tail_offset) / elem->pup.type->size;
            if (count) {
                h_nelems = count;
                host_tmpbuf_offset = yaksuri_global.gpudriver[id].host.slab_tail_offset;
            } else {
                h_nelems =
                    yaksuri_global.gpudriver[id].host.slab_head_offset / elem->pup.type->size;
                host_tmpbuf_offset = 0;
            }
        } else {
            h_nelems =
                (yaksuri_global.gpudriver[id].host.slab_head_offset -
                 yaksuri_global.gpudriver[id].host.slab_tail_offset) / elem->pup.type->size;
            host_tmpbuf_offset = yaksuri_global.gpudriver[id].host.slab_tail_offset;
        }

        if (nelems > h_nelems)
            nelems = h_nelems;
    }

    if (nelems > elem->pup.count - elem->pup.completed_count - elem->pup.issued_count)
        nelems = elem->pup.count - elem->pup.completed_count - elem->pup.issued_count;

    /* if we don't have enough space, return */
    if (nelems == 0) {
        goto fn_exit;
    }


    /* allocate the actual buffer space */
    if (need_gpu_tmpbuf) {
        if (yaksuri_global.gpudriver[id].device[devid].slab == NULL) {
            yaksuri_global.gpudriver[id].device[devid].slab =
                yaksuri_global.gpudriver[id].info->gpu_malloc(TMPBUF_SLAB_SIZE, id);
        }
        yaksuri_global.gpudriver[id].device[devid].slab_tail_offset =
            gpu_tmpbuf_offset + nelems * elem->pup.type->size;
    }

    if (need_host_tmpbuf) {
        if (yaksuri_global.gpudriver[id].host.slab == NULL) {
            yaksuri_global.gpudriver[id].host.slab =
                yaksuri_global.gpudriver[id].info->host_malloc(TMPBUF_SLAB_SIZE);
        }
        yaksuri_global.gpudriver[id].host.slab_tail_offset =
            host_tmpbuf_offset + nelems * elem->pup.type->size;
    }


    /* allocate the subop */
    *subop = (progress_subop_s *) malloc(sizeof(progress_subop_s));

    (*subop)->count_offset = elem->pup.completed_count + elem->pup.issued_count;
    (*subop)->count = nelems;
    if (need_gpu_tmpbuf)
        (*subop)->gpu_tmpbuf =
            (void *) ((char *) yaksuri_global.gpudriver[id].device[devid].slab + gpu_tmpbuf_offset);
    else
        (*subop)->gpu_tmpbuf = NULL;

    if (need_host_tmpbuf)
        (*subop)->host_tmpbuf =
            (void *) ((char *) yaksuri_global.gpudriver[id].host.slab + host_tmpbuf_offset);
    else
        (*subop)->host_tmpbuf = NULL;

    (*subop)->interm_event = NULL;
    (*subop)->event = NULL;
    (*subop)->next = NULL;

    if (elem->pup.subop_tail == NULL) {
        assert(elem->pup.subop_head == NULL);
        elem->pup.subop_head = elem->pup.subop_tail = *subop;
    } else {
        elem->pup.subop_tail->next = *subop;
    }

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

static int free_subop(progress_subop_s * subop)
{
    int rc = YAKSA_SUCCESS;
    progress_elem_s *elem = progress_head;
    yaksuri_request_s *request_backend = (yaksuri_request_s *) elem->request->backend.priv;
    yaksuri_gpudriver_id_e id = request_backend->gpudriver_id;

    if (subop->interm_event) {
        rc = yaksuri_global.gpudriver[id].info->event_destroy(subop->interm_event);
        YAKSU_ERR_CHECK(rc, fn_fail);
    }

    rc = yaksuri_global.gpudriver[id].info->event_destroy(subop->event);
    YAKSU_ERR_CHECK(rc, fn_fail);

    /* free the device buffer */
    if (subop->gpu_tmpbuf) {
        int devid;

        if (elem->pup.puptype == YAKSURI_PUPTYPE__PACK)
            devid = elem->pup.inattr.device;
        else
            devid = elem->pup.outattr.device;

        assert(subop->gpu_tmpbuf ==
               (char *) yaksuri_global.gpudriver[id].device[devid].slab +
               yaksuri_global.gpudriver[id].device[devid].slab_head_offset);
        if (subop->next) {
            yaksuri_global.gpudriver[id].device[devid].slab_head_offset =
                (uintptr_t) ((char *) subop->next->gpu_tmpbuf -
                             (char *) yaksuri_global.gpudriver[id].device[devid].slab);
        } else {
            yaksuri_global.gpudriver[id].device[devid].slab_head_offset =
                yaksuri_global.gpudriver[id].device[devid].slab_tail_offset = 0;
        }
    }

    /* free the host buffer */
    if (subop->host_tmpbuf) {
        assert(subop->host_tmpbuf ==
               (char *) yaksuri_global.gpudriver[id].host.slab +
               yaksuri_global.gpudriver[id].host.slab_head_offset);
        if (subop->next) {
            yaksuri_global.gpudriver[id].host.slab_head_offset =
                (uintptr_t) ((char *) subop->next->gpu_tmpbuf -
                             (char *) yaksuri_global.gpudriver[id].host.slab);
        } else {
            yaksuri_global.gpudriver[id].host.slab_head_offset =
                yaksuri_global.gpudriver[id].host.slab_tail_offset = 0;
        }
    }

    if (elem->pup.subop_head == subop && elem->pup.subop_tail == subop) {
        elem->pup.subop_head = elem->pup.subop_tail = NULL;
    } else if (elem->pup.subop_head == subop) {
        elem->pup.subop_head = subop->next;
    } else {
        for (progress_subop_s * tmp = elem->pup.subop_head; tmp->next; tmp = tmp->next) {
            if (tmp->next == subop) {
                tmp->next = subop->next;
                if (elem->pup.subop_tail == subop)
                    elem->pup.subop_tail = tmp;
                break;
            }
        }
    }

    free(subop);

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

static int progress_subreq(yaksuri_subreq_s *subreq, int *done)
{
    int rc = YAKSA_SUCCESS;
    int completed;

    switch (subreq->kind) {
    case YAKSURI_SUBREQ_KIND__DIRECT:
        rc = yaksuri_global.gpudriver[id].info->event_query(subop->event, done);
        YAKSU_ERR_CHECK(rc, fn_fail);
        break;

    case YAKSURI_SUBREQ_KIND__COPY_D2URH:
    {
        rc = yaksu_buffer_pool_elem_alloc(yaksuri_global.gpudriver[id].rh, &rh);
        YAKSU_ERR_CHECK(rc, fn_fail);
        break;
    }

    case YAKSURI_SUBREQ_KIND__COPY_URH2D:
        break;

    case YAKSURI_SUBREQ_KIND__PACK_D2D_IPC:
        break;

    case YAKSURI_SUBREQ_KIND__PACK_D2D_STAGED:
        break;

    case YAKSURI_SUBREQ_KIND__PACK_D2RH:
        break;

    case YAKSURI_SUBREQ_KIND__PACK_D2URH:
        break;

    case YAKSURI_SUBREQ_KIND__PACK_RH2D:
        break;

    case YAKSURI_SUBREQ_KIND__PACK_URH2D:
        break;

    case YAKSURI_SUBREQ_KIND__UNPACK_D2D_IPC:
        break;

    case YAKSURI_SUBREQ_KIND__UNPACK_D2D_STAGED:
        break;

    case YAKSURI_SUBREQ_KIND__UNPACK_D2RH:
        break;

    case YAKSURI_SUBREQ_KIND__UNPACK_D2URH:
        break;

    case YAKSURI_SUBREQ_KIND__UNPACK_RH2D:
        break;

    case YAKSURI_SUBREQ_KIND__UNPACK_URH2D:
        break;

    default:
        assert(0);
    }

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}

int yaksuri_progress_poke(void)
{
    int rc = YAKSA_SUCCESS;

    /* We only poke the head of the progress engine as all of our
     * operations are independent.  This reduces the complexity of the
     * progress engine and keeps the amount of time we spend in the
     * progress engine small. */

    pthread_mutex_lock(&progress_mutex);

    /* if there's nothing to do, return */
    if (HASH_COUNT(progress_list) == 0)
        goto fn_exit;

    progress_elem_s *elem, *tmp;
    HASH_ITER(hh, progress_list, elem, tmp) {
        int alldone = 1;
        yaksuri_subreq_s *subreq;
        DL_FOREACH_SAFE(elem->request->subreqs, subreq) {
            rc = progress_subreq(subreq, &done);
            YAKSU_ERR_CHECK(rc, fn_fail);

            if (done) {
                DL_DELETE(elem->request->subreqs, subreq);
                free(subreq);
            } else {
                alldone = 0;
            }
        }

        if (alldone) {
            HASH_DEL(progress_list, elem);
            free(elem);
        }
    }

    /* the progress engine has three steps: (1) check for completions
     * and free up any held up resources; (2) if we don't have
     * anything else to do, return; and (3) issue any pending
     * subops. */
    yaksi_type_s *byte_type;
    rc = yaksi_type_get(YAKSA_TYPE__BYTE, &byte_type);
    YAKSU_ERR_CHECK(rc, fn_fail);

    progress_elem_s *elem;
    elem = progress_head;
    yaksuri_request_s *request_backend;
    request_backend = (yaksuri_request_s *) elem->request->backend.priv;
    yaksuri_gpudriver_id_e id;
    id = request_backend->gpudriver_id;

    /****************************************************************************/
    /* Step 1: Check for completion and free up any held up resources */
    /****************************************************************************/
    for (progress_subop_s * subop = elem->pup.subop_head; subop;) {
        int completed;
        rc = yaksuri_global.gpudriver[id].info->event_query(subop->event, &completed);
        YAKSU_ERR_CHECK(rc, fn_fail);

        if (completed) {
            if (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
                elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
                elem->pup.outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
                char *dbuf = (char *) elem->pup.outbuf + subop->count_offset * elem->pup.type->size;
                rc = yaksuri_seq_ipack(subop->host_tmpbuf, dbuf,
                                       subop->count * elem->pup.type->size, byte_type, elem->info);
                YAKSU_ERR_CHECK(rc, fn_fail);
            } else if (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
                       elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
                       (elem->pup.outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST ||
                        elem->pup.outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST)) {
                char *dbuf =
                    (char *) elem->pup.outbuf + subop->count_offset * elem->pup.type->extent;
                rc = yaksuri_seq_iunpack(subop->host_tmpbuf, dbuf, subop->count, elem->pup.type,
                                         elem->info);
                YAKSU_ERR_CHECK(rc, fn_fail);
            }

            elem->pup.completed_count += subop->count;
            elem->pup.issued_count -= subop->count;

            progress_subop_s *tmp = subop->next;
            rc = free_subop(subop);
            YAKSU_ERR_CHECK(rc, fn_fail);
            subop = tmp;
        } else {
            break;
        }
    }


    /****************************************************************************/
    /* Step 2: If we don't have any more work to do, return */
    /****************************************************************************/
    if (elem->pup.completed_count == elem->pup.count) {
        rc = progress_dequeue(elem);
        YAKSU_ERR_CHECK(rc, fn_fail);
        goto fn_exit;
    }


    /****************************************************************************/
    /* Step 3: Issue any pending subops */
    /****************************************************************************/
    while (elem->pup.completed_count + elem->pup.issued_count < elem->pup.count) {
        progress_subop_s *subop;

        rc = alloc_subop(&subop);
        YAKSU_ERR_CHECK(rc, fn_fail);

        if (subop == NULL) {
            break;
        }

        if (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
            elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
            elem->pup.outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) {
            const char *sbuf = (const char *) elem->pup.inbuf +
                subop->count_offset * elem->pup.type->extent;
            char *dbuf = (char *) elem->pup.outbuf + subop->count_offset * elem->pup.type->size;

            rc = yaksuri_global.gpudriver[id].info->ipack(sbuf, dbuf, subop->count, elem->pup.type,
                                                          elem->info, &subop->event,
                                                          subop->gpu_tmpbuf,
                                                          elem->pup.inattr.device);
            YAKSU_ERR_CHECK(rc, fn_fail);
        } else if (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
                   elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
                   elem->pup.outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST) {
            const char *sbuf = (const char *) elem->pup.inbuf +
                subop->count_offset * elem->pup.type->extent;

            rc = yaksuri_global.gpudriver[id].info->ipack(sbuf, subop->host_tmpbuf, subop->count,
                                                          elem->pup.type, elem->info,
                                                          &subop->event, subop->gpu_tmpbuf,
                                                          elem->pup.inattr.device);
            YAKSU_ERR_CHECK(rc, fn_fail);
        } else if ((elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
                    elem->pup.inattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST &&
                    elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) ||
                   (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
                    elem->pup.inattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST &&
                    elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU)) {
            const char *sbuf = (const char *) elem->pup.inbuf +
                subop->count_offset * elem->pup.type->extent;
            char *dbuf = (char *) elem->pup.outbuf + subop->count_offset * elem->pup.type->size;

            rc = yaksuri_seq_ipack(sbuf, subop->host_tmpbuf, subop->count, elem->pup.type,
                                   elem->info);
            YAKSU_ERR_CHECK(rc, fn_fail);

            rc = yaksuri_global.gpudriver[id].info->ipack(subop->host_tmpbuf, dbuf,
                                                          subop->count * elem->pup.type->size,
                                                          byte_type, elem->info, &subop->event,
                                                          NULL, elem->pup.outattr.device);
            YAKSU_ERR_CHECK(rc, fn_fail);
        } else if (elem->pup.puptype == YAKSURI_PUPTYPE__PACK &&
                   elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
                   elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) {
            assert(elem->pup.inattr.device != elem->pup.outattr.device);
            const char *sbuf = (const char *) elem->pup.inbuf +
                subop->count_offset * elem->pup.type->extent;
            char *dbuf = (char *) elem->pup.outbuf + subop->count_offset * elem->pup.type->size;

            bool is_enabled;
            rc = yaksuri_global.gpudriver[id].info->check_p2p_comm(elem->pup.inattr.device,
                                                                   elem->pup.outattr.device,
                                                                   &is_enabled);
            YAKSU_ERR_CHECK(rc, fn_fail);

            if (is_enabled) {
                rc = yaksuri_global.gpudriver[id].info->ipack(sbuf, dbuf, subop->count,
                                                              elem->pup.type, elem->info,
                                                              &subop->event, subop->gpu_tmpbuf,
                                                              elem->pup.inattr.device);
                YAKSU_ERR_CHECK(rc, fn_fail);
            } else {
                rc = yaksuri_global.gpudriver[id].info->ipack(sbuf, subop->host_tmpbuf,
                                                              subop->count, elem->pup.type,
                                                              elem->info, &subop->interm_event,
                                                              subop->gpu_tmpbuf,
                                                              elem->pup.inattr.device);
                YAKSU_ERR_CHECK(rc, fn_fail);

                rc = yaksuri_global.gpudriver[id].info->event_add_dependency(subop->interm_event,
                                                                             elem->pup.
                                                                             outattr.device);
                YAKSU_ERR_CHECK(rc, fn_fail);

                rc = yaksuri_global.gpudriver[id].info->ipack(subop->host_tmpbuf, dbuf,
                                                              subop->count * elem->pup.type->size,
                                                              byte_type, elem->info, &subop->event,
                                                              NULL, elem->pup.outattr.device);
                YAKSU_ERR_CHECK(rc, fn_fail);
            }
        } else if (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
                   elem->pup.inattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST &&
                   elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) {
            const char *sbuf = (const char *) elem->pup.inbuf +
                subop->count_offset * elem->pup.type->size;
            char *dbuf = (char *) elem->pup.outbuf + subop->count_offset * elem->pup.type->extent;

            rc = yaksuri_global.gpudriver[id].info->iunpack(sbuf, dbuf, subop->count,
                                                            elem->pup.type, elem->info,
                                                            &subop->event, subop->gpu_tmpbuf,
                                                            elem->pup.outattr.device);
            YAKSU_ERR_CHECK(rc, fn_fail);
        } else if (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
                   elem->pup.inattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST &&
                   elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) {
            const char *sbuf = (const char *) elem->pup.inbuf +
                subop->count_offset * elem->pup.type->size;
            char *dbuf = (char *) elem->pup.outbuf + subop->count_offset * elem->pup.type->extent;

            rc = yaksuri_seq_iunpack(sbuf, subop->host_tmpbuf,
                                     subop->count * elem->pup.type->size, byte_type, elem->info);
            YAKSU_ERR_CHECK(rc, fn_fail);

            rc = yaksuri_global.gpudriver[id].info->iunpack(subop->host_tmpbuf, dbuf, subop->count,
                                                            elem->pup.type, elem->info,
                                                            &subop->event, subop->gpu_tmpbuf,
                                                            elem->pup.outattr.device);
            YAKSU_ERR_CHECK(rc, fn_fail);
        } else if ((elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
                    elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
                    elem->pup.outattr.type == YAKSUR_PTR_TYPE__REGISTERED_HOST) ||
                   (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
                    elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
                    elem->pup.outattr.type == YAKSUR_PTR_TYPE__UNREGISTERED_HOST)) {
            const char *sbuf = (const char *) elem->pup.inbuf +
                subop->count_offset * elem->pup.type->size;

            rc = yaksuri_global.gpudriver[id].info->iunpack(sbuf, subop->host_tmpbuf,
                                                            subop->count * elem->pup.type->size,
                                                            byte_type, elem->info,
                                                            &subop->event, NULL,
                                                            elem->pup.inattr.device);
            YAKSU_ERR_CHECK(rc, fn_fail);
        } else if (elem->pup.puptype == YAKSURI_PUPTYPE__UNPACK &&
                   elem->pup.inattr.type == YAKSUR_PTR_TYPE__GPU &&
                   elem->pup.outattr.type == YAKSUR_PTR_TYPE__GPU) {
            const char *sbuf = (const char *) elem->pup.inbuf +
                subop->count_offset * elem->pup.type->size;
            char *dbuf = (char *) elem->pup.outbuf + subop->count_offset * elem->pup.type->extent;

            bool is_enabled;
            rc = yaksuri_global.gpudriver[id].info->check_p2p_comm(elem->pup.inattr.device,
                                                                   elem->pup.outattr.device,
                                                                   &is_enabled);
            YAKSU_ERR_CHECK(rc, fn_fail);

            if (is_enabled) {
                rc = yaksuri_global.gpudriver[id].info->iunpack(sbuf, dbuf, subop->count,
                                                                elem->pup.type, elem->info,
                                                                &subop->event, subop->gpu_tmpbuf,
                                                                elem->pup.inattr.device);
                YAKSU_ERR_CHECK(rc, fn_fail);
            } else {
                rc = yaksuri_global.gpudriver[id].info->iunpack(sbuf, subop->host_tmpbuf,
                                                                subop->count * elem->pup.type->size,
                                                                byte_type, elem->info,
                                                                &subop->interm_event, NULL,
                                                                elem->pup.inattr.device);
                YAKSU_ERR_CHECK(rc, fn_fail);

                rc = yaksuri_global.gpudriver[id].info->event_add_dependency(subop->interm_event,
                                                                             elem->pup.
                                                                             outattr.device);
                YAKSU_ERR_CHECK(rc, fn_fail);

                rc = yaksuri_global.gpudriver[id].info->iunpack(subop->host_tmpbuf, dbuf,
                                                                subop->count, elem->pup.type,
                                                                elem->info, &subop->event,
                                                                subop->gpu_tmpbuf,
                                                                elem->pup.outattr.device);
                YAKSU_ERR_CHECK(rc, fn_fail);
            }
        } else {
            rc = YAKSA_ERR__INTERNAL;
            goto fn_fail;
        }

        elem->pup.issued_count += subop->count;
    }

  fn_exit:
    pthread_mutex_unlock(&progress_mutex);
    return rc;
  fn_fail:
    goto fn_exit;
}
