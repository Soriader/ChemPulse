from __future__ import annotations

from pathlib import Path

from chempulse_gen import generate_data


def test_generate_data_pipeline_creates_output_files(tmp_path, monkeypatch) -> None:
    raw_dir = tmp_path / "raw"
    invalid_dir = tmp_path / "invalid"

    monkeypatch.setattr(generate_data, "RAW_DIR", raw_dir)
    monkeypatch.setattr(generate_data, "INVALID_DIR", invalid_dir)

    generate_data.main()

    expected_raw_files = [
        raw_dir / "chem.sensor_readings.v1.jsonl",
        raw_dir / "chem.lab_results.v1.jsonl",
        raw_dir / "chem.material_movements.v1.jsonl",
        raw_dir / "chem.chemical_mdm.v1.jsonl",
    ]

    expected_invalid_files = [
        invalid_dir / "chem.sensor_readings.v1.invalid.jsonl",
        invalid_dir / "chem.lab_results.v1.invalid.jsonl",
        invalid_dir / "chem.material_movements.v1.invalid.jsonl",
        invalid_dir / "chem.chemical_mdm.v1.invalid.jsonl",
    ]

    for file_path in expected_raw_files:
        assert file_path.exists(), f"Missing raw output file: {file_path}"

    for file_path in expected_invalid_files:
        assert file_path.exists(), f"Missing invalid output file: {file_path}"

    sensor_raw_lines = (raw_dir / "chem.sensor_readings.v1.jsonl").read_text(encoding="utf-8").splitlines()
    lab_raw_lines = (raw_dir / "chem.lab_results.v1.jsonl").read_text(encoding="utf-8").splitlines()
    movement_raw_lines = (raw_dir / "chem.material_movements.v1.jsonl").read_text(encoding="utf-8").splitlines()
    chemical_raw_lines = (raw_dir / "chem.chemical_mdm.v1.jsonl").read_text(encoding="utf-8").splitlines()

    assert len(sensor_raw_lines) == 200
    assert len(lab_raw_lines) == 100
    assert len(movement_raw_lines) == 80
    assert len(chemical_raw_lines) == 20