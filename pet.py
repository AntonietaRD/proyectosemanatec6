from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    # Inicia la sesión de Spark
    spark = SparkSession \
        .builder \
        .appName("pets") \
        .getOrCreate()

    # Ruta al archivo CSV de mascotas
    print("Leyendo dataset de mascotas ...")
    path_pets = "dataset.csv"
    
    # Lee el archivo CSV, especificando que la primera fila tiene los encabezados y usa inferencia de tipos
    df_pets = spark.read.csv(path_pets, header=True, inferSchema=True)
    
    # Renombrar algunas columnas para facilitar el uso (si es necesario)
    # Por ejemplo: renombrar "size" si fuera necesario, aunque en este caso no parece necesario

    # Muestra la estructura del DataFrame
    df_pets.createOrReplaceTempView("pets")
    query = 'DESCRIBE pets'
    spark.sql(query).show(20)

    # Consulta para obtener el nombre, edad, sexo y tamaño de las mascotas
    query = """SELECT name, age, sex, size FROM pets WHERE name IS NOT NULL"""
    df_pets_filtered = spark.sql(query)
    df_pets_filtered.show(20)

    # Consulta para obtener las mascotas mayores de una cierta edad (por ejemplo, mayores de 3 años)
    query_age = """SELECT name, age, sex, size FROM pets WHERE age > 3"""
    df_pets_age_filtered = spark.sql(query_age)
    df_pets_age_filtered.show(20)

    # Convertir el DataFrame filtrado a formato JSON
    results = df_pets_filtered.toJSON().collect()

    # Guardar los resultados en un archivo JSON
    with open('results/pets_data.json', 'w') as file:
        json.dump(results, file)

    # Realizar un conteo de las diferentes razas y tamaños de las mascotas
    query_breed_size = """SELECT breed, size, COUNT(*) FROM pets GROUP BY breed, size"""
    df_breed_size = spark.sql(query_breed_size)
    df_breed_size.show()

    # Detener la sesión de Spark al final
    spark.stop()
