
// --- PARTE 1: OPERACIONES BÁSICAS ---
val conRepetidos = sc.parallelize(List(1, 2, 2, 3, 3, 3, 4))
println("Distinct: " + conRepetidos.distinct().collect().mkString(", "))

val ventasBase = sc.parallelize(List(
  ("norte", 100), ("sur", 200), ("norte", 150), ("sur", 50), ("este", 300)
))
val agrupado = ventasBase.groupByKey()
println("Agrupado: " + agrupado.collect().mkString(", "))

val totalPorZona = ventasBase.reduceByKey((a, b) => a + b)
println("Total por zona: " + totalPorZona.collect().mkString(", "))

// --- EJERCICIOS ---
// Ejercicio 1
val celsius = sc.parallelize(List(0.0, 20.0, 37.0, 100.0, -10.0))
val fahrenheit = celsius.map(c => c * 9.0 / 5.0 + 32.0)
println("Temperaturas Fahrenheit:")
fahrenheit.collect().foreach(f => println(f"$f%.1f °F"))

// Ejercicio 2
val productos = sc.parallelize(List(("Teclado", 35.99), ("Monitor", 199.99), ("Ratón", 22.50), ("Auriculares", 89.00), ("Alfombrilla", 12.00), ("Webcam", 65.00)))
val caros = productos.filter(_._2 > 50.0).sortBy(-_._2)
println("Productos > 50€:")
caros.collect().foreach { case (n, p) => println(s"  $n → $p€") }

// Ejercicio 3
val frases = sc.parallelize(List("Apache Spark es un motor", "Scala es el lenguaje", "Los RDDs son la base"))
val palabras = frases.flatMap(_.split(" "))
println(s"Total palabras: ${palabras.count()}")

// Ejercicio 6 (Ventas por departamento)
val ventasDepto = sc.parallelize(List(("Electrónica", 1200), ("Ropa", 340), ("Hogar", 560), ("Ropa", 120)))
val totalDepto = ventasDepto.reduceByKey(_ + _)
println("Ventas por depto:")
totalDepto.collect().foreach(println)

// Ejercicio 9
val textoRDD = sc.parallelize(List("spark es rápido spark es potente", "scala es el lenguaje de spark"))
val conteo = textoRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
println("Top 5 palabras:")
conteo.take(5).foreach(println)

// Ejercicio 10
val logsRDD = sc.parallelize(List("192.168.1.1 GET 200", "10.0.0.2 POST 200", "192.168.1.3 GET 404"))
val peticiones = logsRDD.map(_.split(" ").last).map((_, 1)).reduceByKey(_ + _).sortByKey()
println("Peticiones HTTP:")
peticiones.collect().foreach(println)



//caso de estudio 1
//contexto de empresa:
//MercaData S.L. es una cadena de supermercados con tiendas en cuatro provincias españolas:
  //Madrid, Barcelona, Valencia y Sevilla. Su departamento de tecnología ha decidido migrar 
  //el análisis de ventas a Apache Spark para poder procesar el volumen creciente de datos
  // de forma distribuida.

//Te han contratado como analista de datos para realizar el primer análisis real sobre los 
//datos de la última semana. Los datos llegan en bruto como colecciones de cadenas de texto,
// tal y como los exporta su sistema de caja antiguo.

//formato: "ID_TIENDA|PROVINCIA|PRODUCTO|CATEGORIA|IMPORTE|EMPLEADO_ID"
// ================= DATOS =================
val transacciones = sc.parallelize(List(
  "T01|Madrid|Leche Entera|Lácteos|1.20|E03",
  "T02|Barcelona|Pan de Molde|Panadería|1.85|E07",
  "T03|Valencia|Leche Entera|Lácteos|1.20|E11",
  "T04|Madrid|Zumo de Naranja|Bebidas|2.40|E03",
  "T05|Sevilla|Yogur Natural|Lácteos|0.75|E15",
  "T06|Barcelona|Agua Mineral|Bebidas|0.60|E07",
  "T07|Madrid|Cerveza Rubia|Bebidas|1.10|E04",
  "T08|Valencia|Pan de Molde|Panadería|1.85|E12",
  "T09|Sevilla|Leche Entera|Lácteos|1.20|E15",
  "T10|Madrid|Yogur Natural|Lácteos|0.75|E03",
  "T11|Barcelona|Zumo de Naranja|Bebidas|2.40|E08",
  "T12|Valencia|Cerveza Rubia|Bebidas|1.10|E11",
  "T13|Sevilla|Agua Mineral|Bebidas|0.60|E16",
  "T14|Madrid|Leche Entera|Lácteos|1.20|E04",
  "T15|Barcelona|Pan de Molde|Panadería|1.85|E07",
  "T16|Valencia|Yogur Natural|Lácteos|0.75|E12",
  "T17|Sevilla|Zumo de Naranja|Bebidas|2.40|E15",
  "T18|Madrid|Agua Mineral|Bebidas|0.60|E03",
  "T19|Barcelona|Leche Entera|Lácteos|1.20|E08",
  "T20|Valencia|Pan de Molde|Panadería|1.85|E11",
  "T21|Sevilla|Cerveza Rubia|Bebidas|1.10|E16",
  "T22|Madrid|Zumo de Naranja|Bebidas|2.40|E04",
  "T23|Barcelona|Yogur Natural|Lácteos|0.75|E07",
  "T24|Valencia|Leche Entera|Lácteos|1.20|E12",
  "T25|Sevilla|Pan de Molde|Panadería|1.85|E15"
))

val facturacionProvincia = (
  transacciones
    .map(linea => {
      val partes = linea.split("\\|")
      val provincia = partes(1)
      val importe = partes(4).toDouble
      (provincia, importe)
    })
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
)

println("Facturación por provincia (mayor a menor):")

facturacionProvincia.collect().zipWithIndex.foreach {
  case ((provincia, total), i) =>
    println(s"${i + 1}. $provincia -> ${"%.2f".format(total)}€")
}

// ================= PREGUNTA 2 =================

val categoriasMadrid = (
  transacciones
    .map(linea => linea.split("\\|"))
    .filter(partes => partes(1) == "Madrid")
    .map(partes => partes(3))
    .distinct()
    .sortBy(categoria => categoria)
)

println("Categorías en Madrid:")
categoriasMadrid.collect().foreach(println)

// ================= PREGUNTA 3 =================
// ¿Cuántas transacciones ha gestionado cada empleado?


// RDD de empleados
val empleados = sc.parallelize(List(
  ("E03", "Carmen Vidal"),
  ("E04", "Luis Herrero"),
  ("E07", "Marta Soler"),
  ("E08", "Diego Fuentes"),
  ("E11", "Ana Romero"),
  ("E12", "Pablo Leal"),
  ("E15", "Rosa Cano"),
  ("E16", "Javier Mora")
))

val transaccionesPorEmpleado = (
  transacciones
    .map(linea => {
      val partes = linea.split("\\|")
      val empleadoId = partes(5)
      (empleadoId, 1)
    })
    .reduceByKey(_ + _)
)

val resultado = (
  empleados
    .join(transaccionesPorEmpleado)
    .map {
      case (id, (nombre, total)) => (nombre, total)
    }
    .sortBy(_._1)
)

println("Transacciones por empleado:")

resultado.collect().foreach {
  case (nombre, total) =>
    val texto = if (total == 1) "transacción" else "transacciones"
    println(s"$nombre -> $total $texto")
}


// ================= PREGUNTA 4 =================
// ¿Qué productos se venden tanto en Madrid como en Barcelona?

val datos = transacciones.map(_.split("\\|"))

val productosMadrid = (
  datos
    .filter(partes => partes(1) == "Madrid")
    .map(partes => partes(2))
    .distinct()
)

val productosBarcelona = (
  datos
    .filter(partes => partes(1) == "Barcelona")
    .map(partes => partes(2))
    .distinct()
)

val productosComunes = (
  productosMadrid
    .intersection(productosBarcelona)
    .sortBy(producto => producto)
)

println("Productos en Madrid y Barcelona:")
productosComunes.collect().foreach(println)

// ================= PREGUNTA 5 =================
// ¿Cuál es la facturación total por categoría?

val facturacionCategoria = (
  transacciones
    .map(linea => {
      val partes = linea.split("\\|")
      val categoria = partes(3)
      val importe = partes(4).toDouble
      (categoria, importe)
    })
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
)

println("Facturación por categoría:")

facturacionCategoria.collect().foreach {
  case (categoria, total) =>
    println(s"$categoria -> ${"%.2f".format(total)}€")
}

// ================= PREGUNTA 6 =================
// Lista completa de productos únicos disponibles en la cadena

val catalogoProductos = (
  transacciones
    .map(linea => linea.split("\\|")(2))
    .distinct()
    .sortBy(producto => producto)
)

println("Catálogo de productos:")
catalogoProductos.collect().foreach(println)