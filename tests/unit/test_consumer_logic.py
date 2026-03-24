from chempulse_consumer.validation import is_valid_event


def test_is_valid_sensor_event_ok():
    event = {"quality_flag": "OK"}
    assert is_valid_event("chem.sensor_readings.v1", event) is True


def test_is_valid_sensor_event_bad():
    event = {"quality_flag": "BAD"}
    assert is_valid_event("chem.sensor_readings.v1", event) is False


def test_is_valid_material_event():
    event = {"status": "COMPLETED"}
    assert is_valid_event("chem.material_movements.v1", event) is True


def test_is_valid_chemical_mdm_event():
    event = {"is_active": True}
    assert is_valid_event("chem.chemical_mdm.v1", event) is True