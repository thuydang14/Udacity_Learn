from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, ArrayType


schema_video = StructType([
    StructField('video_id', StringType(), True),
    StructField('trending_date', StringType(), True),
    StructField('title', StringType(), True),
    StructField('channel_title', StringType(), True),
    StructField('category_id', IntegerType(), True),
    StructField('publish_time', TimestampType(), True),
    StructField('tags', StringType(), True),
    StructField('views', IntegerType(), True),
    StructField('likes', IntegerType(), True),
    StructField('dislikes', IntegerType(), True),
    StructField('comment_count', IntegerType(), True),
    StructField('thumbnail_link', StringType(), True),
    StructField('comments_disabled', BooleanType(), True),
    StructField('ratings_disabled', BooleanType(), True),
    StructField('video_error_or_removed', BooleanType(), True),
    StructField('description', StringType(), True)
])


schema_category = StructType([
    StructField('etag', StringType(), True),
    StructField('items', ArrayType(
            StructType([
                StructField('etag', StringType(), True),
                StructField('id', StringType(), True),
                StructField('kind', StringType(), True),
                StructField('snippet', StructType([
                    StructField('assignable', BooleanType(), True),
                    StructField('channelId', StringType(), True),
                    StructField('title', StringType(), True)
                    ])
                ),
            ])
    )),
    StructField('kind', StringType(), True)
])


