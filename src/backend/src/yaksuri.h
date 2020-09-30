/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#ifndef YAKSURI_H_INCLUDED
#define YAKSURI_H_INCLUDED

#include "yaksi.h"

typedef enum yaksuri_gpudriver_id_e {
    YAKSURI_GPUDRIVER_ID__UNSET = -1,
    YAKSURI_GPUDRIVER_ID__CUDA = 0,
    YAKSURI_GPUDRIVER_ID__LAST,
} yaksuri_gpudriver_id_e;

typedef enum yaksuri_pup_e {
    YAKSURI_PUPTYPE__PACK,
    YAKSURI_PUPTYPE__UNPACK,
} yaksuri_puptype_e;

#define YAKSURI_TMPBUF_ELEM_SIZE   (1024 * 1024)
#define YAKSURI_TMPBUF_NUM_ELEMS   (16)

typedef struct {
    struct {
        yaksur_gpudriver_info_s *info;
        struct {
            yaksu_buffer_pool_s host;
            yaksu_buffer_pool_s *device;
        } tmpbuf_pool;
    } gpudriver[YAKSURI_GPUDRIVER_ID__LAST];
} yaksuri_global_s;
extern yaksuri_global_s yaksuri_global;

typedef struct yaksuri_chunk {
    yaksu_buffer_pool_s pool;
    void *buf;
    uintptr_t nelems;

    struct yaksuri_chunk *next;
    struct yaksuri_chunk *prev;
} yaksuri_chunk_s;

typedef struct yaksuri_event {
    void *event;

    struct yaksuri_event *next;
    struct yaksuri_event *prev;
} yaksuri_event_s;

typedef struct {
    const void *inbuf;
    void *outbuf;
    uintptr_t total_elems;
    yaksi_type_s * type;
    yaksi_info_s * info;
    uintptr_t issued_elems;
    uintptr_t completed_elems;
} yaksuri_progress_state_s;

typedef struct yaksuri_subreq {
    yaksi_request_s *request;

    union {
        struct {
            void *event;
        } direct;
        struct {
            yaksuri_progress_state_s state;
            yaksuri_event_s *events;
            yaksuri_chunk_s *rh;
            yaksuri_chunk_s *src_d;
            yaksuri_chunk_s *dst_d;
        } indirect;
    } u;

    struct yaksuri_subreq *next;
    struct yaksuri_subreq *prev;
} yaksuri_subreq_s;

typedef struct yaksuri_request {
    enum {
        YAKSURI_REQUEST_KIND__PACK_H2H,
        YAKSURI_REQUEST_KIND__PACK_D2D_SINGLE,
        YAKSURI_REQUEST_KIND__PACK_D2D_IPC,
        YAKSURI_REQUEST_KIND__PACK_D2D_STAGED,
        YAKSURI_REQUEST_KIND__PACK_D2RH,
        YAKSURI_REQUEST_KIND__PACK_D2URH,
        YAKSURI_REQUEST_KIND__PACK_RH2D,
        YAKSURI_REQUEST_KIND__PACK_URH2D,

        YAKSURI_REQUEST_KIND__UNPACK_H2H,
        YAKSURI_REQUEST_KIND__UNPACK_D2D_SINGLE,
        YAKSURI_REQUEST_KIND__UNPACK_D2D_IPC,
        YAKSURI_REQUEST_KIND__UNPACK_D2D_STAGED,
        YAKSURI_REQUEST_KIND__UNPACK_D2RH,
        YAKSURI_REQUEST_KIND__UNPACK_D2URH,
        YAKSURI_REQUEST_KIND__UNPACK_RH2D,
        YAKSURI_REQUEST_KIND__UNPACK_URH2D,
    } kind;

    yaksur_ptr_attr_s inattr;
    yaksur_ptr_attr_s outattr;
    yaksuri_gpudriver_id_e gpudriver_id;
    yaksuri_subreq_s *subreqs;
} yaksuri_request_s;

int yaksuri_progress_enqueue(const void *inbuf, void *outbuf, uintptr_t count, yaksi_type_s * type,
                             yaksi_info_s * info, yaksi_request_s * request,
                             yaksur_ptr_attr_s inattr, yaksur_ptr_attr_s outattr,
                             yaksuri_puptype_e puptype);
int yaksuri_progress_poke(void);

#endif /* YAKSURI_H_INCLUDED */
