/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "yaksi.h"
#include "yaksuri_cudai.h"
#include <assert.h>
#include <string.h>
#include <stdlib.h>

int yaksuri_cudai_setup_cuda_fns(void)
{
    int rc = YAKSA_SUCCESS;

    yaksuri_cudai_global.launch_kernel = cudaLaunchKernel;

    yaksuri_cudai_global.malloc_host = cudaMallocHost;
    yaksuri_cudai_global.malloc = cudaMalloc;
    yaksuri_cudai_global.malloc_managed = cudaMallocManaged;
    yaksuri_cudai_global.free_host = cudaFreeHost;
    yaksuri_cudai_global.free = cudaFree;

    yaksuri_cudai_global.get_device_count = cudaGetDeviceCount;
    yaksuri_cudai_global.get_device = cudaGetDevice;
    yaksuri_cudai_global.set_device = cudaSetDevice;

    yaksuri_cudai_global.device_can_access_peer = cudaDeviceCanAccessPeer;
    yaksuri_cudai_global.device_enable_peer_access = cudaDeviceEnablePeerAccess;

    yaksuri_cudai_global.event_create = cudaEventCreate;
    yaksuri_cudai_global.event_record = cudaEventRecord;
    yaksuri_cudai_global.event_query = cudaEventQuery;
    yaksuri_cudai_global.event_synchronize = cudaEventSynchronize;
    yaksuri_cudai_global.event_destroy = cudaEventDestroy;

    yaksuri_cudai_global.stream_create = cudaStreamCreate;
    yaksuri_cudai_global.stream_create_with_flags = cudaStreamCreateWithFlags;
    yaksuri_cudai_global.stream_destroy = cudaStreamDestroy;
    yaksuri_cudai_global.stream_wait_event = cudaStreamWaitEvent;

    yaksuri_cudai_global.pointer_get_attributes = cudaPointerGetAttributes;
    yaksuri_cudai_global.memcpy_async = cudaMemcpyAsync;

  fn_exit:
    return rc;
  fn_fail:
    goto fn_exit;
}
