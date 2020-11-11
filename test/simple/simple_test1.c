/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include <stdio.h>
#include "yaksa.h"
#include <assert.h>

#define DIMSIZE (80)

int inbuf[DIMSIZE * DIMSIZE], outbuf[DIMSIZE * DIMSIZE];

int main()
{
    int rc = YAKSA_SUCCESS;
    yaksa_type_t vector, vector_vector;
    uintptr_t actual;

    yaksa_init(NULL);
    int idx = 0;

    for (int i = 0; i < DIMSIZE * DIMSIZE; i++) {
        inbuf[i] = i;
        outbuf[i] = -1;
    }

    rc = yaksa_type_create_vector(3, 2, 3, YAKSA_TYPE__INT, NULL, &vector);
    assert(rc == YAKSA_SUCCESS);

    yaksa_request_t request;
    rc = yaksa_ipack(inbuf, 1, vector, 0, outbuf, DIMSIZE * DIMSIZE * sizeof(int), &actual,
                     NULL, &request);
    assert(rc == YAKSA_SUCCESS);

    /* Here inbuf is the dest buffer that contains strided datai, outbuf contains packed data */
    rc = yaksa_iacc(outbuf, actual, inbuf, 1, vector, 0, YAKSA_OP__SUM,
                       NULL, &request);
    assert(rc == YAKSA_SUCCESS);

    rc = yaksa_request_wait(request);
    assert(rc == YAKSA_SUCCESS);

    idx = 0;
     for (int i = 0; i < 3*2; i++) {
	     if (i == 2 || i ==5)
		     continue;
	     if (inbuf[i] != i*2)
            	fprintf(stderr, "Error at i = %d\n", i);
    }

    yaksa_type_free(vector_vector);
    yaksa_type_free(vector);

    yaksa_finalize();

    return 0;
}
