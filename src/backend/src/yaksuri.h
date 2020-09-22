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
        struct {
            yaksu_buffer_pool_s host;
            yaksu_buffer_pool_s *device;
        } tmpbuf_pool;
        yaksur_gpudriver_info_s *info;
    } gpudriver[YAKSURI_GPUDRIVER_ID__LAST];
} yaksuri_global_s;
extern yaksuri_global_s yaksuri_global;

typedef struct yaksuri_chunk {
    yaksu_buffer_pool_s pool;
    void *buf;
    void *event;

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
    uintptr_t count;
    yaksi_type_s * type;
    yaksi_info_s * info;
    uintptr_t offset;
} yaksuri_progress_state_s;

typedef struct yaksuri_subreq {
    enum {
        YAKSURI_SUBREQ_KIND__DIRECT,

        YAKSURI_SUBREQ_KIND__COPY_D2URH,
        YAKSURI_SUBREQ_KIND__COPY_URH2D,

        YAKSURI_SUBREQ_KIND__PACK_D2D_IPC,
        YAKSURI_SUBREQ_KIND__PACK_D2D_STAGED,
        YAKSURI_SUBREQ_KIND__PACK_D2RH,
        YAKSURI_SUBREQ_KIND__PACK_D2URH,
        YAKSURI_SUBREQ_KIND__PACK_RH2D,
        YAKSURI_SUBREQ_KIND__PACK_URH2D,

        YAKSURI_SUBREQ_KIND__UNPACK_D2D_IPC,
        YAKSURI_SUBREQ_KIND__UNPACK_D2D_STAGED,
        YAKSURI_SUBREQ_KIND__UNPACK_D2RH,
        YAKSURI_SUBREQ_KIND__UNPACK_D2URH,
        YAKSURI_SUBREQ_KIND__UNPACK_RH2D,
        YAKSURI_SUBREQ_KIND__UNPACK_URH2D,
    } kind;

    union {
        struct {
            void *event;
        } direct;
        struct {
            yaksuri_chunk_s *rh;
            yaksuri_event_s *events;
        } copy_d2urh;
        struct {
            yaksuri_chunk_s *rh;
            yaksuri_event_s *events;
        } copy_urh2d;
        struct {
            yaksuri_chunk_s *src_d;
            yaksuri_event_s *events;
        } pack_d2d_ipc;
        struct {
            yaksuri_chunk_s *src_d;
            yaksuri_chunk_s *rh;
            yaksuri_event_s *events;
        } pack_d2d_staged;
        struct {
            yaksuri_chunk_s *src_d;
            yaksuri_event_s *events;
        } pack_d2rh;
        struct {
            yaksuri_chunk_s *src_d;
            yaksuri_chunk_s *rh;
            yaksuri_event_s *events;
        } pack_d2urh;
        struct {
            yaksuri_chunk_s *rh;
            yaksuri_event_s *events;
        } pack_rh2d;
        struct {
            yaksuri_chunk_s *rh;
            yaksuri_event_s *events;
        } pack_urh2d;
        struct {
            yaksuri_chunk_s *dst_d;
            yaksuri_event_s *events;
        } unpack_d2d_ipc;
        struct {
            yaksuri_chunk_s *rh;
            yaksuri_chunk_s *dst_d;
            yaksuri_event_s *events;
        } unpack_d2d_staged;
        struct {
            yaksuri_chunk_s *rh;
            yaksuri_event_s *events;
        } unpack_d2rh;
        struct {
            yaksuri_chunk_s *rh;
            yaksuri_event_s *events;
        } unpack_d2urh;
        struct {
            yaksuri_chunk_s *dst_d;
            yaksuri_event_s *events;
        } unpack_rh2d;
        struct {
            yaksuri_chunk_s *rh;
            yaksuri_chunk_s *dst_d;
            yaksuri_event_s *events;
        } unpack_urh2d;
    } u;

    yaksuri_progress_state_s state;

    struct yaksuri_subreq *next;
    struct yaksuri_subreq *prev;
} yaksuri_subreq_s;

typedef struct yaksuri_request {
    yaksuri_gpudriver_id_e gpudriver_id;
    yaksuri_subreq_s *subreqs;
} yaksuri_request_s;

int yaksuri_progress_enqueue(const void *inbuf, void *outbuf, uintptr_t count, yaksi_type_s * type,
                             yaksi_info_s * info, yaksi_request_s * request,
                             yaksur_ptr_attr_s inattr, yaksur_ptr_attr_s outattr,
                             yaksuri_puptype_e puptype);
int yaksuri_progress_poke(void);

#endif /* YAKSURI_H_INCLUDED */
