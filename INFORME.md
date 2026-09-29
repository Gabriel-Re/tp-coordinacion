Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

# Informe Gabriel Re (105095)

Los clientes se distinguen mediante `ClientID`, que acompaña los mensajes hasta que el gateway entrega el resultado correspondiente. Sum y Aggregation mantienen acumuladores separados por cliente, permitiendo procesar mensajes intercalados sin mezclar sus datos.

## Distribución de datos

Sum consume la entrada del gateway y reparte los mensajes completos por turnos entre las colas de trabajo, una por Sum y no por cliente. También realiza cálculos desde su propia cola, con un consumidor separado del distribuidor.

El EOF de cada cliente se envía a todos los Sum usando los mismos publicadores que sus datos. Como los envíos son secuenciales, cada trabajador recibe el EOF después de los registros que le fueron asignados. El distribuidor confirma la entrada al completar los envíos y continúa sin esperar los resultados.

## Acumulación y resultados

Sum y Aggregation declaran la misma cola de comunicación, donde los mensajes esperan hasta ser consumidos. Cada Sum envía sus acumulados y luego un EOF con su `SumID`, incluso si no recibió datos del cliente. Aggregation espera los EOF de todas las instancias antes de calcular el top, sin volver a contar IDs repetidos mientras el cliente está pendiente.

Aggregation envía el top y después su EOF. Join reenvía el resultado identificado al gateway y consume el EOF sin reenviarlo. El indicador explícito de EOF permite diferenciar que haya terminado de un resultado vacío. Los estados se eliminan después de completar los envíos exitosamente.
