from typing import Any, List, Tuple

from iceaxe.alias_values import Alias


def optimize_exec_casting(
    values: List[Any],
    select_raws: List[Any],
    select_types: List[Tuple[bool, bool, bool]],
) -> List[Any]:
    cdef Py_ssize_t num_selects = len(select_raws)
    cdef Py_ssize_t num_values = len(values)
    cdef Py_ssize_t i, j
    cdef list fields_by_select = []
    cdef list select_names = []
    cdef list column_converters = []
    cdef list fields, result_value
    cdef list result_all = [None] * num_values
    cdef dict obj_dict
    cdef object select_raw, info, converter, value, field_value, item
    cdef str field_name, select_name, table_name
    cdef bint raw_is_table, raw_is_column, raw_is_function_metadata, all_none

    if len(select_types) != num_selects:
        raise ValueError("Select values and types must have the same length.")

    # Keep Python strings and bound converters alive for the entire row loop.
    for j in range(num_selects):
        select_raw = select_raws[j]
        raw_is_table, raw_is_column, raw_is_function_metadata = select_types[j]
        fields = []
        select_name = ""
        converter = None

        if raw_is_table:
            table_name = select_raw.get_table_name()
            fields = [
                (
                    field_name,
                    f"{table_name}_{field_name}",
                    info.from_db_value if info.is_json else None,
                )
                for field_name, info in select_raw.get_client_fields().items()
                if not info.exclude
            ]
        elif raw_is_column:
            select_name = f"{select_raw.root_model.get_table_name()}_{select_raw.key}"
            if select_raw.field_definition.is_json:
                converter = select_raw.field_definition.from_db_value
        elif raw_is_function_metadata:
            select_name = select_raw.local_name
        elif isinstance(select_raw, Alias):
            select_name = select_raw.name
        else:
            raise TypeError(f"Unsupported select value: {select_raw!r}")

        fields_by_select.append(fields)
        select_names.append(select_name)
        column_converters.append(converter)

    result_value = [None] * num_selects
    for i in range(num_values):
        value = values[i]
        for j in range(num_selects):
            select_raw = select_raws[j]
            raw_is_table, raw_is_column, raw_is_function_metadata = select_types[j]

            if raw_is_table:
                obj_dict = {}
                all_none = True
                fields = fields_by_select[j]
                for field_name, select_name, converter in fields:
                    try:
                        field_value = value[select_name]
                    except KeyError:
                        raise KeyError(f"Key '{select_name}' not found in value.")

                    if field_value is not None:
                        all_none = False
                        if converter is not None:
                            field_value = converter(field_value)
                    obj_dict[field_name] = field_value

                # An unmatched outer join must not construct a model of null fields.
                item = None if all_none else select_raw(**obj_dict)
            else:
                select_name = select_names[j]
                try:
                    item = value[select_name]
                except KeyError:
                    raise KeyError(f"Key '{select_name}' not found in value.")
                converter = column_converters[j]
                if item is not None and converter is not None:
                    item = converter(item)

            if num_selects == 1:
                result_all[i] = item
            else:
                result_value[j] = item

        if num_selects != 1:
            result_all[i] = tuple(result_value)

    return result_all
