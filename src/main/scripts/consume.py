from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, filter, from_json
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

    wind_speed = data.select(col("data.wind.speed").alias("wind_speed"))
    visibility = data.select(col("data.visibility").alias("visibility"))
    rain = data.select(
        expr("array_contains(data.weather.main, 'Rain') OR array_contains(data.weather.main, 'Snow')").alias("rain")
    )
    storm = data.select(
        expr("exists(data.weather, weather -> weather.main == 'Thunderstorm')").alias("storm")
    )
    temperature = data.select(col("data.main.temp").alias("temperature"))
    sunrise = data.select(col("data.sys.sunrise").alias("sunrise"))
    sunset = data.select(col("data.sys.sunset").alias("sunset"))
    current_time = current_timestamp().alias("current_time")

    # Check for cloud base below 200m
    cloud_below_200m = data.select(
        expr("size(filter(data.clouds.all, all -> all < 200)) > 0").alias("cloud_below_200m")
    )

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

    # Apply the flight condition checks
    flight_conditions = data.select(
        wind_speed,
        visibility,
        rain,
        storm,
        cloud_below_200m,
        temperature,
        sunrise,
        sunset,
        current_time,
        check_flight_conditions(
            col("wind_speed.wind_speed"),
            col("visibility.visibility"),
            col("rain.rain"),
            col("storm.storm"),
            col("cloud_below_200m.cloud_below_200m"),
            col("temperature.temperature"),
            col("sunrise.sunrise"),
            col("sunset.sunset"),
            col("current_time.current_time")
        ).alias("flight_conditions")
    )

    # Start the streaming query
    query = flight_conditions.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
