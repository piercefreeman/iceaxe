from iceaxe.base import TableBase
from iceaxe.field import DBFieldClassDefinition, DBFieldInfo


def test_db_field_class_definition_instantiation():
    field_def = DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=DBFieldInfo()
    )
    assert field_def.root_model == TableBase
    assert field_def.key == "test_key"
    assert isinstance(field_def.field_definition, DBFieldInfo)
