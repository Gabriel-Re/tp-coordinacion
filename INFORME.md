Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

# Informe Gabriel Re (105095)

Los clientes se distinguen mediante `ClientID`, que acompaña los mensajes hasta que el gateway entrega el resultado correspondiente. Sum y Aggregation mantienen acumuladores separados por cliente, y Join conserva sus candidatos y finalizaciones de la misma manera. Así pueden procesar mensajes intercalados sin mezclar sus datos.

## Distribución de datos

Sum 0 consume la entrada del gateway y reparte los mensajes completos por turnos entre las colas de trabajo, una por Sum y no por cliente. También realiza cálculos desde su propia cola, con un consumidor separado del distribuidor.

El EOF de cada cliente se envía a todos los Sum usando los mismos publicadores que sus datos. Como los envíos son secuenciales, cada trabajador recibe el EOF después de los registros que le fueron asignados. El distribuidor confirma la entrada al completar los envíos y continúa sin esperar los resultados.

## Acumulación y resultados

Cada Sum elige el destino de sus acumulados aplicando hash al nombre de la fruta y usando el módulo por la cantidad de Aggregations. Todas las apariciones de una misma fruta llegan a la misma réplica.

Después de enviar los acumulados, cada Sum envía su EOF con `SumID` a todas las Aggregations, incluso a las que no recibieron frutas de ese cliente.

Cada Aggregation espera los EOF de todos los Sum para enviar su top parcial y después un EOF con su `AggregationID`. Join combina cada top parcial con los candidatos del cliente, los ordena y conserva como máximo `TopSize` elementos. No vuelve a sumar cantidades porque cada fruta ya fue consolidada en una única Aggregation.

Join registra los EOF distintos por cliente y espera a todas las Aggregations antes de enviar un único top global al gateway. Los EOF parciales se confirman para seguir consumiendo. 
Si todos los tops estaban vacíos, se envía igualmente una lista vacía.

## Escalabilidad y concurrencia

Los registros se procesan a medida que llegan y se conservan acumulados por fruta, en lugar de guardar todos los registros recibidos. La memoria utilizada no es constante, ya que depende de los clientes activos y de la cantidad de frutas distintas de cada uno. Atender más clientes al mismo tiempo aumenta ese estado. Cada etapa elimina los datos del cliente cuando termina correctamente su procesamiento y completa los envíos correspondientes, sin afectar a los demás.

Las réplicas de Sum reparten el procesamiento de los mensajes, mientras que las de Aggregation reparten el trabajo por fruta y mantienen juntos sus acumulados. Join conserva únicamente los candidatos necesarios para el top de cada cliente. Agregar réplicas permite repartir trabajo, pero no garantiza una mejora proporcional del rendimiento. El distribuidor de Sum 0 y Join siguen siendo puntos centralizados.
