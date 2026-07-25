import importlib.metadata

# Ver: https://docs.astral.sh/ruff/linter/#file-level
# Ver: https://docs.astral.sh/ruff/rules/unsorted-imports/
# ruff: noqa: I001
from .motor_ingesta import MotorIngesta
from .agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto
from .flujo_diario import FlujoDiario

__version__ = importlib.metadata.version("motor_ingesta")
__author__ = importlib.metadata.metadata("motor_ingesta")["Author"]

__all__ = [
    "MotorIngesta",
    "FlujoDiario",
    "aniade_hora_utc",
    "aniade_intervalos_por_aeropuerto",
]


def get_version() -> str:
    """
    Devuelve la versión del paquete motor_ingesta.
    :return: Versión del paquete motor_ingesta.
    """
    return importlib.metadata.version("motor_ingesta")
