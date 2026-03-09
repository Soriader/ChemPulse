from __future__ import annotations

METRICS = [
    ("temperature", "C"),
    ("pressure", "bar"),
    ("ph", "pH"),
    ("conductivity", "uS/cm"),
    ("humidity", "%"),
]

QUALITY_FLAGS = ["OK", "WARN", "BAD"]

LAB_TESTS = [
    ("chloride_content", "mg/L", "ASTM_D512"),
    ("sulfur_content", "mg/kg", "ASTM_D4294"),
    ("density", "kg/m3", "ISO_12185"),
    ("viscosity", "mm2/s", "ASTM_D445"),
    ("ph", "pH", "ISO_10523"),
]

LAB_RESULT_STATUSES = ["APPROVED", "REJECTED", "PENDING"]

MOVEMENT_TYPES = ["TRANSFER", "CONSUMPTION", "RECEIPT", "DISPOSAL"]
MOVEMENT_STATUSES = ["COMPLETED", "PENDING", "CANCELLED"]

MATERIAL_TYPES = ["sample", "reagent", "chemical", "solvent"]

LOCATIONS = [
    "warehouse_a",
    "warehouse_b",
    "lab_1",
    "lab_2",
    "reactor_zone",
    "cold_storage",
]

SOURCE_SYSTEMS = ["scada", "lims", "erp", "mes"]

HAZARD_CLASSES = [
    "flammable",
    "corrosive",
    "toxic",
    "oxidizing",
    "non_hazardous",
]

CHEMICALS = [
    ("CHEM-001", "Hydrochloric Acid", "7647-01-0", "corrosive", "L", "SUP-001"),
    ("CHEM-002", "Sodium Hydroxide", "1310-73-2", "corrosive", "kg", "SUP-002"),
    ("CHEM-003", "Ethanol", "64-17-5", "flammable", "L", "SUP-003"),
    ("CHEM-004", "Sulfuric Acid", "7664-93-9", "corrosive", "L", "SUP-001"),
    ("CHEM-005", "Acetone", "67-64-1", "flammable", "L", "SUP-004"),
]