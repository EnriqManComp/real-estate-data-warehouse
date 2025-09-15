from pyspark.sql import SparkSession

def main():
    # Inicializar SparkSession
    spark = SparkSession.builder \
        .appName("PruebaClusterSpark") \
        .master("local[*]") \
        .getOrCreate()

    # ====== RDD Test ======
    numeros = spark.sparkContext.parallelize(range(1, 101))
    suma = numeros.reduce(lambda a, b: a + b)
    print(f"Suma de 1 a 100 = {suma}")

    # ====== DataFrame Test ======
    datos = [
        ("Alice", 34),
        ("Bob", 45),
        ("Carmen", 29),
        ("Daniel", 40)
    ]
    columnas = ["nombre", "edad"]

    df = spark.createDataFrame(datos, columnas)
    df.show()

    # Operaciones simples en DataFrame
    df.filter(df.edad > 30).show()
    df.groupBy().avg("edad").show()

    # Finalizar sesión
    spark.stop()

if __name__ == "__main__":
    main()
