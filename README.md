# Dynamo-flow - Justificación

Se optó por un enfoque orientado a objetos para que todas las operaciones sigan una misma estructura.

La operación NormalizeAmountOperation se diseñó para ser flexible: puede aplicarse sobre un campo numérico, no solo amount o price. Se encarga de limpiar símbolos de moneda, aceptar tanto coma como punto, y convertir el valor a float. Si el campo no existe o no se puede convertir se deja en None y se registra una advertencia para no interrumpir el flujo.

La operación ContextualFieldValidation asegura que campos obligatorios existan y cumplan una condición. Si un registro no cumple, se marca con _invalid = True. De esta manera, no se descarta la información, sino que queda identificada como inválida.

Finalmente, no usé eval() ni reglas dinámicas porque, aunque harían el sistema más flexible que podrian ocasionar problemas en el mantenimiento
