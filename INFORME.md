Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

# Informe Gabriel Re (105095)

Los clientes se distinguen mediante `ClientID` y sus acumuladores separados.
Sum y Aggregation mantienen un mapa por cliente, por lo que terminar uno no elimina los datos de los demás.

## Comunicación entre Sum y Aggregation

Ahora ambos procesos declaran la misma cola nombrada. Sum la prepara antes de publicar y Aggregation antes de consumir. Si Sum arranca primero, los mensajes esperan en esa cola. Si Aggregation arranca primero, espera allí los mensajes.

Sum guarda sus destinos en el slice `outputQueues`, con una cola por Aggregation configurada y no por cliente. Con una sola Aggregation hay un único elemento. Por ahora cada mensaje se envía a todos los destinos.

Para cada cliente, Sum envía los acumulados y luego su EOF. Aggregation envía el top y después su EOF.

