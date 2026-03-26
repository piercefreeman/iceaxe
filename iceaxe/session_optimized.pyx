from typing import Any, List, Tuple
from iceaxe.base import TableBase
from iceaxe.queries import FunctionMetadata
from iceaxe.alias_values import Alias
from cpython.ref cimport PyObject
from cpython.object cimport PyObject_GetItem
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from cpython.ref cimport Py_INCREF, Py_DECREF

cdef struct FieldInfo:
    char* name                # Field name
    char* select_attribute    # Corresponding attribute in the select_raw
    bint is_json              # Flag indicating if the field is JSON

cdef char* allocate_cstring(bytes data):
    cdef Py_ssize_t length = len(data)
    cdef char* c_str = <char*>malloc((length + 1) * sizeof(char))
    if not c_str:
        raise MemoryError("Failed to allocate memory for C string.")
    memcpy(c_str, <char*>data, length)  # Cast bytes to char* for memcpy
    c_str[length] = 0  # Null-terminate the string
    return c_str

cdef void free_fields(FieldInfo** fields, Py_ssize_t* num_fields_array, Py_ssize_t num_selects):
    cdef Py_ssize_t j, k
    if fields:
        for j in range(num_selects):
            if fields[j]:
                for k in range(num_fields_array[j]):
                    free(fields[j][k].name)
                    free(fields[j][k].select_attribute)
                free(fields[j])
        free(fields)
    if num_fields_array:
        free(num_fields_array)

cdef FieldInfo** precompute_fields(list select_raws, list select_types, Py_ssize_t num_selects, Py_ssize_t* num_fields_array):
    cdef FieldInfo** fields = <FieldInfo**>malloc(num_selects * sizeof(FieldInfo*))
    cdef Py_ssize_t j, k, num_fields
    cdef dict field_dict
    cdef bytes select_bytes, field_bytes
    cdef char* c_select
    cdef char* c_field
    cdef object select_raw
    cdef bint raw_is_table, raw_is_column, raw_is_function_metadata

    if not fields:
        raise MemoryError("Failed to allocate memory for fields.")

    for j in range(num_selects):
        select_raw = select_raws[j]
        raw_is_table, raw_is_column, raw_is_function_metadata = select_types[j]

        if raw_is_table:
            field_dict = {field: info.is_json for field, info in select_raw.get_client_fields().items() if not info.exclude}
            num_fields = len(field_dict)
            num_fields_array[j] = num_fields
            fields[j] = <FieldInfo*>malloc(num_fields * sizeof(FieldInfo))
            if not fields[j]:
                raise MemoryError("Failed to allocate memory for FieldInfo.")

            for k, (field, is_json) in enumerate(field_dict.items()):
                select_bytes = f"{select_raw.get_table_name()}_{field}".encode('utf-8')
                c_select = allocate_cstring(select_bytes)

                field_bytes = field.encode('utf-8')
                c_field = allocate_cstring(field_bytes)

                fields[j][k].select_attribute = c_select
                fields[j][k].name = c_field
                fields[j][k].is_json = is_json
        else:
            num_fields_array[j] = 0
            fields[j] = NULL

    return fields

cdef list process_values(
    list values,
    FieldInfo** fields,
    Py_ssize_t* num_fields_array,
    list select_raws,
    list select_types,
    Py_ssize_t num_selects
):
    cdef Py_ssize_t num_values = len(values)
    cdef list result_all = [None] * num_values
    cdef Py_ssize_t i, j, k, num_fields
    cdef PyObject** result_value
    cdef object value, obj, item
    cdef dict obj_dict
    cdef bint raw_is_table, raw_is_column, raw_is_function_metadata, raw_is_alias
    cdef char* field_name_c
    cdef char* select_name_c
    cdef str field_name
    cdef str select_name
    cdef object field_value
    cdef object select_raw
    cdef PyObject* temp_obj
    cdef bint all_none

    for i in range(num_values):
        value = values[i]
        result_value = <PyObject**>malloc(num_selects * sizeof(PyObject*))
        if not result_value:
            raise MemoryError("Failed to allocate memory for result_value.")
        try:
            for j in range(num_selects):
                select_raw = select_raws[j]
                raw_is_table, raw_is_column, raw_is_function_metadata = select_types[j]
                raw_is_alias = isinstance(select_raw, Alias)

                if raw_is_table:
                    obj_dict = {}
                    num_fields = num_fields_array[j]
                    all_none = True

                    # First pass: collect all fields and check if they're all None
                    for k in range(num_fields):
                        field_name_c = fields[j][k].name
                        select_name_c = fields[j][k].select_attribute
                        field_name = field_name_c.decode('utf-8')
                        select_name = select_name_c.decode('utf-8')

                        try:
                            field_value = value[select_name]
                        except KeyError:
                            raise KeyError(f"Key '{select_name}' not found in value.")

                        if field_value is not None:
                            all_none = False
                            if fields[j][k].is_json:
                                field_value = select_raw.model_fields[field_name].from_db_value(field_value)

                        obj_dict[field_name] = field_value

                    # If all fields are None, store None instead of creating the table object
                    if all_none:
                        result_value[j] = <PyObject*>None
                        Py_INCREF(None)
                    else:
                        obj = select_raw(**obj_dict)
                        result_value[j] = <PyObject*>obj
                        Py_INCREF(obj)

                elif raw_is_column:
                    try:
                        # Use the table-qualified column name
                        table_name = select_raw.root_model.get_table_name()
                        column_name = select_raw.key
                        item = value[f"{table_name}_{column_name}"]
                    except KeyError:
                        raise KeyError(f"Key '{table_name}_{column_name}' not found in value.")
                    if item is not None and select_raw.field_definition.is_json:
                        item = select_raw.field_definition.from_db_value(item)
                    result_value[j] = <PyObject*>item
                    Py_INCREF(item)

                elif raw_is_function_metadata:
                    try:
                        item = value[select_raw.local_name]
                    except KeyError:
                        raise KeyError(f"Key '{select_raw.local_name}' not found in value.")
                    result_value[j] = <PyObject*>item
                    Py_INCREF(item)

                elif raw_is_alias:
                    try:
                        item = value[select_raw.name]
                    except KeyError:
                        raise KeyError(f"Key '{select_raw.name}' not found in value.")
                    result_value[j] = <PyObject*>item
                    Py_INCREF(item)

            # Assemble the result
            if num_selects == 1:
                result_all[i] = <object>result_value[0]
                Py_DECREF(<object>result_value[0])
            else:
                result_tuple = tuple([<object>result_value[j] for j in range(num_selects)])
                for j in range(num_selects):
                    Py_DECREF(<object>result_value[j])
                result_all[i] = result_tuple

        finally:
            free(result_value)

    return result_all

cdef list optimize_casting(list values, list select_raws, list select_types):
    cdef Py_ssize_t num_selects = len(select_raws)
    cdef Py_ssize_t* num_fields_array = <Py_ssize_t*>malloc(num_selects * sizeof(Py_ssize_t))
    cdef FieldInfo** fields
    cdef list result_all

    if not num_fields_array:
        raise MemoryError("Failed to allocate memory for num_fields_array.")

    try:
        fields = precompute_fields(select_raws, select_types, num_selects, num_fields_array)
        result_all = process_values(values, fields, num_fields_array, select_raws, select_types, num_selects)
    finally:
        free_fields(fields, num_fields_array, num_selects)

    return result_all

def optimize_exec_casting(
    values: List[Any],
    select_raws: List[Any],
    select_types: List[Tuple[bool, bool, bool]]
) -> List[Any]:
    return optimize_casting(values, select_raws, select_types)
