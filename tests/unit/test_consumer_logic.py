from chempulse_consumer.validation import is_valid_event
from chempulse_consumer.routing import route_event
from chempulse_storage.sql_server_writer import insert_event

#TEST 1 - event validation

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

#TEST 2 — routing / check if the data is going to the right place

def test_route_valid_event():
    event = {"quality_flag": "OK"}
    path = route_event("chem.sensor_readings.v1", event)
    assert "valid" in path


def test_route_invalid_event():
    event = {"quality_flag": "BAD"}
    path = route_event("chem.sensor_readings.v1", event)
    assert "invalid" in path

#TEST 3 — duplicate handling (mock)

def test_insert_event_duplicate(monkeypatch):
    def mock_insert(*args, **kwargs):
        return False

    monkeypatch.setattr(
        "chempulse_storage.sql_server_writer.insert_event",
        mock_insert
    )

    result = insert_event("chem.sensor_readings.v1", {"event_id": "123"})
    assert result is False