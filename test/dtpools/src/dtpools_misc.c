/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "dtpools_internal.h"
#include <assert.h>
#include <string.h>
#include "yaksa.h"

#define MAX_TYPES (10)
#define MAX_TYPE_LEN (64)

int DTPI_func_nesting = -1;

static yaksa_type_t name_to_type(DTPI_pool_s * dtpi, const char *name)
{
    yaksa_type_t type;
    int rc = YAKSA_SUCCESS;

    if (!strcmp(name, "_Bool") || !strcmp(name, "bool"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE___BOOL, &type);
    else if (!strcmp(name, "char"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__CHAR, &type);
    else if (!strcmp(name, "signed_char"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__SIGNED_CHAR, &type);
    else if (!strcmp(name, "unsigned_char"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UNSIGNED_CHAR, &type);
    else if (!strcmp(name, "wchar"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__WCHAR_T, &type);
    else if (!strcmp(name, "int"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT, &type);
    else if (!strcmp(name, "unsigned"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UNSIGNED, &type);
    else if (!strcmp(name, "short"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__SHORT, &type);
    else if (!strcmp(name, "unsigned_short"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UNSIGNED_SHORT, &type);
    else if (!strcmp(name, "long"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__LONG, &type);
    else if (!strcmp(name, "unsigned_long"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UNSIGNED_LONG, &type);
    else if (!strcmp(name, "long_long"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__LONG_LONG, &type);
    else if (!strcmp(name, "unsigned_long_long"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UNSIGNED_LONG_LONG, &type);
    else if (!strcmp(name, "int8_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT8_T, &type);
    else if (!strcmp(name, "int16_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT16_T, &type);
    else if (!strcmp(name, "int32_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT32_T, &type);
    else if (!strcmp(name, "int64_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT64_T, &type);
    else if (!strcmp(name, "uint8_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT8_T, &type);
    else if (!strcmp(name, "uint16_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT16_T, &type);
    else if (!strcmp(name, "uint32_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT32_T, &type);
    else if (!strcmp(name, "uint64_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT64_T, &type);
    else if (!strcmp(name, "int_fast8_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT_FAST8_T, &type);
    else if (!strcmp(name, "int_fast16_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT_FAST16_T, &type);
    else if (!strcmp(name, "int_fast32_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT_FAST32_T, &type);
    else if (!strcmp(name, "int_fast64_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT_FAST64_T, &type);
    else if (!strcmp(name, "uint_fast8_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT_FAST8_T, &type);
    else if (!strcmp(name, "uint_fast16_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT_FAST16_T, &type);
    else if (!strcmp(name, "uint_fast32_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT_FAST32_T, &type);
    else if (!strcmp(name, "uint_fast64_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT_FAST64_T, &type);
    else if (!strcmp(name, "int_least8_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT_LEAST8_T, &type);
    else if (!strcmp(name, "int_least16_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT_LEAST16_T, &type);
    else if (!strcmp(name, "int_least32_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT_LEAST32_T, &type);
    else if (!strcmp(name, "int_least64_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INT_LEAST64_T, &type);
    else if (!strcmp(name, "uint_least8_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT_LEAST8_T, &type);
    else if (!strcmp(name, "uint_least16_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT_LEAST16_T, &type);
    else if (!strcmp(name, "uint_least32_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT_LEAST32_T, &type);
    else if (!strcmp(name, "uint_least64_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINT_LEAST64_T, &type);
    else if (!strcmp(name, "intmax_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INTMAX_T, &type);
    else if (!strcmp(name, "uintmax_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINTMAX_T, &type);
    else if (!strcmp(name, "size_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__SIZE_T, &type);
    else if (!strcmp(name, "float"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__FLOAT, &type);
    else if (!strcmp(name, "double"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__DOUBLE, &type);
    else if (!strcmp(name, "long_double"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__LONG_DOUBLE, &type);
    else if (!strcmp(name, "intptr_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__INTPTR_T, &type);
    else if (!strcmp(name, "uintptr_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__UINTPTR_T, &type);
    else if (!strcmp(name, "ptrdiff_t"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__PTRDIFF_T, &type);
    else if (!strcmp(name, "c_complex"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__C_COMPLEX, &type);
    else if (!strcmp(name, "c_double_complex"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__C_DOUBLE_COMPLEX, &type);
    else if (!strcmp(name, "c_long_double_complex"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__C_LONG_DOUBLE_COMPLEX, &type);
    else if (!strcmp(name, "float_int"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__FLOAT_INT, &type);
    else if (!strcmp(name, "double_int"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__DOUBLE_INT, &type);
    else if (!strcmp(name, "long_int"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__LONG_INT, &type);
    else if (!strcmp(name, "2int"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__2INT, &type);
    else if (!strcmp(name, "short_int"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__SHORT_INT, &type);
    else if (!strcmp(name, "long_double_int"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__LONG_DOUBLE_INT, &type);
    else if (!strcmp(name, "byte"))
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__BYTE, &type);
    else
        rc = yaksa_type_get_predefined(dtpi->context, YAKSA_TYPE__NULL, &type);

    assert(rc == YAKSA_SUCCESS);
    return type;
}

int DTPI_parse_base_type_str(DTP_pool_s * dtp, const char *str)
{
    yaksa_type_t *array_of_types = NULL;
    int *array_of_blklens = NULL;
    char **typestr = NULL;
    char **countstr = NULL;
    int num_types = 0;
    int i;
    intptr_t lb;
    DTPI_pool_s *dtpi = dtp->priv;
    int rc = DTP_SUCCESS;

    DTPI_FUNC_ENTER();

    DTPI_ERR_ASSERT(str, rc);

    strncpy(dtpi->base_type_str, str, DTPI_MAX_BASE_TYPE_STR_LEN);

    DTPI_ALLOC_OR_FAIL(typestr, MAX_TYPES * sizeof(char *), rc);
    DTPI_ALLOC_OR_FAIL(countstr, MAX_TYPES * sizeof(char *), rc);

    int stridx = 0;
    for (num_types = 0; num_types < MAX_TYPES; num_types++) {
        if (str[stridx] == '\0')
            break;

        DTPI_ALLOC_OR_FAIL(typestr[num_types], MAX_TYPE_LEN, rc);
        DTPI_ALLOC_OR_FAIL(countstr[num_types], MAX_TYPE_LEN, rc);

        for (i = 0; str[stridx] != '\0' && str[stridx] != ':' && str[stridx] != '+'; i++, stridx++)
            typestr[num_types][i] = str[stridx];
        typestr[num_types][i] = '\0';

        if (str[stridx] == ':') {
            stridx++;

            for (i = 0; str[stridx] != '\0' && str[stridx] != '+'; i++, stridx++)
                countstr[num_types][i] = str[stridx];
            countstr[num_types][i] = '\0';
        } else {
            countstr[num_types][0] = '\0';
        }

        if (str[stridx] == '+')
            stridx++;
    }
    DTPI_ERR_ASSERT(str[stridx] == '\0', rc);
    DTPI_ERR_ASSERT(num_types >= 1, rc);
    DTPI_ERR_ASSERT(num_types < MAX_TYPES, rc);

    DTPI_ALLOC_OR_FAIL(array_of_types, num_types * sizeof(yaksa_type_t), rc);
    DTPI_ALLOC_OR_FAIL(array_of_blklens, num_types * sizeof(int), rc);

    for (i = 0; i < num_types; i++) {
        array_of_types[i] = name_to_type(dtpi, typestr[i]);

        if (strlen(countstr[i]) == 0)
            array_of_blklens[i] = 1;
        else
            array_of_blklens[i] = atoi(countstr[i]);
    }

    /* if it's a single basic datatype, just return it */
    if (num_types == 1 && array_of_blklens[0] == 1) {
        dtp->DTP_base_type = array_of_types[0];
        dtpi->base_type_is_struct = 0;
        DTPI_FREE(array_of_types);
        DTPI_FREE(array_of_blklens);
        goto fn_exit;
    }

    /* non-basic type, create a struct */
    intptr_t *array_of_displs;
    DTPI_ALLOC_OR_FAIL(array_of_displs, num_types * sizeof(intptr_t), rc);
    intptr_t displ = 0;

    for (i = 0; i < num_types; i++) {
        uintptr_t size;
        rc = yaksa_type_get_size(array_of_types[i], &size);
        DTPI_ERR_CHK_RC(rc);

        array_of_displs[i] = displ;
        displ += (array_of_blklens[i] * size);
    }

    rc = yaksa_type_create_struct(num_types, array_of_blklens, array_of_displs, array_of_types,
                                  NULL, &dtp->DTP_base_type);
    DTPI_ERR_CHK_RC(rc);

    dtpi->base_type_is_struct = 1;
    dtpi->base_type_attrs.numblks = num_types;
    dtpi->base_type_attrs.array_of_blklens = array_of_blklens;
    dtpi->base_type_attrs.array_of_displs = array_of_displs;
    dtpi->base_type_attrs.array_of_types = array_of_types;

  fn_exit:
    yaksa_type_get_extent(dtp->DTP_base_type, &lb, &dtpi->base_type_extent);
    for (i = 0; i < num_types; i++) {
        DTPI_FREE(typestr[i]);
        DTPI_FREE(countstr[i]);
    }
    if (typestr)
        DTPI_FREE(typestr);
    if (countstr)
        DTPI_FREE(countstr);
    DTPI_FUNC_EXIT();
    return rc;

  fn_fail:
    if (array_of_types)
        DTPI_FREE(array_of_types);
    if (array_of_blklens)
        DTPI_FREE(array_of_blklens);
    goto fn_exit;
}

unsigned int DTPI_low_count(unsigned int count)
{
    int ret;

    DTPI_FUNC_ENTER();

    if (count == 1) {
        ret = count;
        goto fn_exit;
    }

    /* if 'count' is a prime number, return 1; else return the lowest
     * prime factor of 'count' */
    for (ret = 2; ret < count; ret++) {
        if (count % ret == 0)
            break;
    }

  fn_exit:
    DTPI_FUNC_EXIT();
    return ret == count ? 1 : ret;
}

unsigned int DTPI_high_count(unsigned int count)
{
    DTPI_FUNC_ENTER();

    DTPI_FUNC_EXIT();
    return count / DTPI_low_count(count);
}

int DTPI_rand(DTPI_pool_s * dtpi)
{
    int ret;
    int rc = DTP_SUCCESS;

    DTPI_FUNC_ENTER();

    ret = dtpi->rand_list[dtpi->rand_idx];
    dtpi->rand_idx++;

  fn_exit:
    DTPI_FUNC_EXIT();
    assert(rc == DTP_SUCCESS);
    return ret;

  fn_fail:
    goto fn_exit;
}
