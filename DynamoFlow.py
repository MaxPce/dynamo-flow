from typing import Any, Dict, List, Iterable, Callable, Optional
from abc import ABC, abstractmethod
import logging

# libreria para logs
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Clase abstracta para operaciones.
class Operation(ABC):
    

    def __init__(self, field_name: str):
        self.field = field_name   

    @abstractmethod
    def apply(self, dato: Dict[str, Any]) -> Dict[str, Any]: 
        pass

# Convierte un campo de texto a float.
class NormalizeAmountOperation(Operation):

    def apply(self, fila: Dict[str, Any]) -> Dict[str, Any]:
        valor = fila.get(self.field)

        if valor is None:
            logging.warning(f"Campo '{self.field}' no encontrado.")
            fila[self.field] = None
            return fila

        if isinstance(valor, (int, float)):
            fila[self.field] = float(valor)
            return fila

        if isinstance(valor, str):
            try:
                normalizado = valor.replace("EUR", "").replace("$", "").strip()
                normalizado = normalizado.replace(",", ".")
                fila[self.field] = float(normalizado)
            except Exception as e:
                print("Error en NormalizeAmount:", e) 
                fila[self.field] = None

        return fila

# Chequea que un campo exista y no esté vacío.
class ContextualFieldValidation(Operation):
    
    
    def __init__(self, campo: str, condicion: Optional[Callable[[Any], bool]] = None):
        super().__init__(campo)
        self.condicion = condicion or (lambda v: v not in (None, ""))

    def apply(self, fila: Dict[str, Any]) -> Dict[str, Any]:
        valor = fila.get(self.field)
        if not self.condicion(valor):
            logging.error(f"El campo '{self.field}' no es válido.")
            fila["_invalid"] = True
        return fila


# Gestor de registros

class RecordContextManager:

    def __init__(self):
        self.contextos: Dict[str, List[Operation]] = {}

    def register_context(self, tipo: str, ops: List[Operation]) -> None:
        self.contextos[tipo] = ops

    def process_record(self, fila: Dict[str, Any]) -> Dict[str, Any]:
        tipo = fila.get("_type_")

        if not tipo or tipo not in self.contextos:
            logging.warning(f"Tipo de registro '{tipo}' no configurado.")
            fila["_invalid"] = True
            return fila

        for operacion in self.contextos[tipo]:
            fila = operacion.apply(fila)

        return fila

    def process_stream(self, data_stream: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        salida = {"procesados": [], "validos": [], "invalidos": [], "errores": []}

        for fila in data_stream:
            try:
                procesado = self.process_record(fila)
                salida["procesados"].append(procesado)

                if procesado.get("_invalid"):
                    salida["invalidos"].append(procesado)
                else:
                    salida["validos"].append(procesado)

            except Exception as e:
                logging.error(f"Error procesando fila: {e}")
                fila["_error"] = str(e)
                salida["errores"].append(fila)

        return salida


if __name__ == "__main__":
    
    gestor = RecordContextManager()

    # order
    gestor.register_context("order_event", [
        NormalizeAmountOperation("amount"),
        ContextualFieldValidation("order_id"),
        ContextualFieldValidation("customer_name"),
    ])

    # product
    gestor.register_context("product_update", [
        NormalizeAmountOperation("price"),
        ContextualFieldValidation("product_sku"),
        ContextualFieldValidation("is_active"),
    ])

    # registros
    data = [
        {"_type_": "order_event", "order_id": "ORD789", "customer_name": "Luis Vargas", "amount": "123,45 EUR"},
        {"_type_": "order_event", "order_id": "ORD100", "customer_name": "Bob", "amount": "no_es_un_numero"},
        {"_type_": "product_update", "product_sku": "SKU_P002", "price": None, "is_active": "False"},
        {"_type_": "product_update", "product_sku": "SKU_P003", "price": "25.00"},
        {}, 
    ]

    resultado = gestor.process_stream(data)

    print("Procesados:", resultado["procesados"])
    print("Válidos:", resultado["validos"])
    print("Inválidos:", resultado["invalidos"])
