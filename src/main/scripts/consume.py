from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, filter, from_json, array_contains
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, BooleanType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True)
        ]), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
            StructField("icon", StringType(), True)
        ])), True),
        StructField("base", StringType(), True),
        StructField("main", StructType([
            StructField("temp", DoubleType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("temp_min", DoubleType(), True),
            StructField("temp_max", DoubleType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("humidity", IntegerType(), True)
        ]), True),
        StructField("visibility", IntegerType(), True),
        StructField("wind", StructType([
            StructField("speed", DoubleType(), True),
            StructField("deg", IntegerType(), True)
        ]), True),
        StructField("clouds", StructType([
            StructField("all", IntegerType(), True)
        ]), True),
        StructField("dt", LongType(), True),
        StructField("sys", StructType([
            StructField("type", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("sunrise", LongType(), True),
            StructField("sunset", LongType(), True)
        ]), True),
        StructField("timezone", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("cod", IntegerType(), True)
    ])

    # Topic subscription
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "test3")
        .load()
    )

    data = raw.select(from_json(col("value").cast("string"), schema).alias("data"))

    def check_flight_conditions(wind_speed, visibility, rain, storm, cloud_below_200m, temperature, sunrise, sunset,
                                current_time):
        return (
            wind_speed < 4 and
            visibility >= 2000 and
            not rain and
            not storm and
            not cloud_below_200m and
            temperature < 26 and
            (
                (current_time >= sunrise and current_time <= sunrise_plus_3h) or
                (current_time >= sunset_minus_3h and current_time <= sunset)
            )
        )

    
    wind_speed = data.select(col("data.wind.speed")).alias("wind_speed")

    
    visibility = data.select(col("data.visibility")).alias("visibility")
    rain = data.select(
        (array_contains(col("data.weather.main"), "Rain") |
         array_contains(col("data.weather.main"), "Snow")).alias("rain")
    )
    storm = data.select(
        expr("exists(data.weather, weather -> weather.main == 'Thunderstorm')").alias("storm")
    )
    temperature = data.select(col("data.main.temp")).alias("temperature")
    sunrise = data.select(col("data.sys.sunrise")).alias("sunrise")
    sunset = data.select(col("data.sys.sunset")).alias("sunset")
    current_time = current_timestamp().alias("current_time")
    
    # Check for cloud base below 200m
    cloud_below_200m = data.select(
        expr("size(filter(array(data.clouds.all), all -> all < 200)) > 0").alias("cloud_below_200m")
    )

    can_fly = check_flight_conditions(wind_speed = wind_speed[0],
                                      visibility[0], 
                                      rain = tain[0],
                                      storm = storm[0],
                                      cloud_below_200m = cloud_below_200m[0],
                                      temperature = temperature[0], 
                                      sunrise = sunrise[0],
                                      sunset = sunset[0],
                                      current_time = current_time[0])

    
    # Start the streaming query
    query = wind_speed.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
