from typing import Any, List, Tuple
from iceaxe.base import TableBase, PolymorphicBase
from iceaxe.queries import FunctionMetadata
from iceaxe.alias_values import Alias
from json import loads as json_loads
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
    cdef object model_class

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

                    # Check if this is a polymorphic model - only for full model selects
                    is_polymorphic = hasattr(select_raw, "get_discriminator_field") and select_raw.get_discriminator_field() is not None
                    discriminator_value = None
                    discriminator_field = None if not is_polymorphic else select_raw.get_discriminator_field()
                    
                    print(f"Processing row for table {select_raw.get_table_name()}")
                    print(f"Model class: {select_raw.__name__}")
                    if is_polymorphic:
                        print(f"  Polymorphic model detected with discriminator field: {discriminator_field}")
                        if hasattr(select_raw, '_PolymorphicBase__poly_registry'):
                            print(f"  Registry keys: {list(select_raw._PolymorphicBase__poly_registry.keys())}")
                    
                    # First pass: collect all fields and check if they're all None
                    for k in range(num_fields):
                        field_name_c = fields[j][k].name
                        select_name_c = fields[j][k].select_attribute
                        field_name = field_name_c.decode('utf-8')
                        select_name = select_name_c.decode('utf-8')

                        try:
                            field_value = value[select_name]
                            print(f"  Field: {field_name}, Select name: {select_name}, Value: {field_value}")
                        except KeyError:
                            raise KeyError(f"Key '{select_name}' not found in value.")

                        if field_value is not None:
                            all_none = False
                            if fields[j][k].is_json:
                                field_value = json_loads(field_value)
                                
                            # If field name matches discriminator field, store the value for later
                            if is_polymorphic and field_name == discriminator_field:
                                discriminator_value = field_value
                                print(f"  Found discriminator value: {discriminator_value}")

                        obj_dict[field_name] = field_value

                    # If all fields are None, store None instead of creating the table object
                    if all_none:
                        result_value[j] = <PyObject*>None
                        Py_INCREF(None)
                    else:
                        # For polymorphic models, select proper concrete model class
                        model_class = select_raw
                        if is_polymorphic and discriminator_value is not None:
                            print(f"  Looking for subclass for discriminator value: {discriminator_value} (type: {type(discriminator_value)})")
                            try:
                                # If the discriminator value is an enum, we might need special handling
                                # to get the correct subclass
                                if hasattr(discriminator_value, 'value'):
                                    # Try with both the enum instance and its string value
                                    print(f"  Discriminator is an enum with value: {discriminator_value.value}")
                                    model_class = select_raw.get_subclass_for_discriminator(discriminator_value.value)
                                    
                                    # If we didn't find a match, try with the enum instance
                                    if model_class == select_raw:
                                        model_class = select_raw.get_subclass_for_discriminator(discriminator_value)
                                else:
                                    # For non-enum values, use the value directly
                                    model_class = select_raw.get_subclass_for_discriminator(discriminator_value)
                            except Exception as e:
                                print(f"  Error finding subclass: {str(e)}")
                                model_class = select_raw
                                
                            print(f"  Selected model class: {model_class.__name__}")
                            
                        print(f"  Creating instance of {model_class.__name__} with data: {obj_dict}")
                        obj = model_class(**obj_dict)
                        print(f"  Created instance: {type(obj).__name__}")
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
