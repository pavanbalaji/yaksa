/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include <yaksa.h>
#include <assert.h>
#include <stdlib.h>
#include "matrix_util.h"

#define BLKLEN (4)

int main()
{
    int rc;
    int input_matrix[SIZE];
    int pack_buf[SIZE];
    int unpack_buf[SIZE];
    yaksa_type_t indexed_block;
    int array_of_displacements[ROWS] = {
        4, 12, 20, 28,
        32, 40, 48, 56
    };

    yaksa_init(YAKSA_INIT_ATTR__DEFAULT);       /* before any yaksa API is called the library
                                                 * must be initialized */

    init_matrix(input_matrix, ROWS, COLS);
    set_matrix(pack_buf, ROWS, COLS, 0);
    set_matrix(unpack_buf, ROWS, COLS, 0);

    rc = yaksa_create_indexed_block(ROWS, BLKLEN, array_of_displacements, YAKSA_TYPE__INT,
                                    &indexed_block);
    assert(rc == YAKSA_SUCCESS);

    yaksa_request_t request;
    uintptr_t actual_pack_bytes;
    rc = yaksa_ipack(input_matrix, 1, indexed_block, 0, pack_buf, ROWS * BLKLEN * sizeof(int),
                     &actual_pack_bytes, &request);
    assert(rc == YAKSA_SUCCESS);
    rc = yaksa_request_wait(request);
    assert(rc == YAKSA_SUCCESS);

    rc = yaksa_iunpack(pack_buf, ROWS * BLKLEN * sizeof(int), unpack_buf, 1, indexed_block, 0,
                       &request);
    assert(rc == YAKSA_SUCCESS);
    rc = yaksa_request_wait(request);
    assert(rc == YAKSA_SUCCESS);

    print_matrix(input_matrix, ROWS, COLS, "input_matrix=");
    print_matrix(unpack_buf, ROWS, COLS, "unpack_buf=");

    yaksa_free(indexed_block);
    yaksa_finalize();
    return 0;
}