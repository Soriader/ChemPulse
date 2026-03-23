from __future__ import annotations

from typing import Any


def is_valid_event(topic: str, event: dict[str, Any]) -> bool:
    if topic in {"chem.sensor_readings.v1", "chem.lab_results.v1"}:
        return event.get("quality_flag") == "OK"

    if topic == "chem.material_movements.v1":
        return event.get("status") == "COMPLETED"

    if topic == "chem.chemical_mdm.v1":
        return event.get("is_active") is True

    return False