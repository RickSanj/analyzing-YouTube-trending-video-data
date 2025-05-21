from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, date_sub, dayofweek, last_day, countDistinct, col, row_number, input_file_name, to_date, trunc, count, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum, struct, min as _min, max as _max, first
import os
from datetime import timedelta
import json


def load_category_mapping(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return {item["id"]: item["snippet"]["title"] for item in data["items"]}


def get_category_name(category_id, source_file):
    country_code = os.path.basename(source_file)[:2]
    json_path = f"/opt/app/data/{country_code}_category_id.json"

    # Load mapping only once and cache
    if not hasattr(get_category_name, "cache"):
        get_category_name.cache = {}

    if country_code not in get_category_name.cache:
        get_category_name.cache[country_code] = load_category_mapping(
            json_path)

    return get_category_name.cache[country_code].get(str(category_id), "Unknown")


def task_1(df):
    # top 10 video_ids by trending count
    top_videos_df = df.groupBy("video_id").agg(count("*").alias("cnt")) \
                      .orderBy(desc("cnt"), desc("video_id")) \
                      .limit(10)

    top_video_ids = top_videos_df.collect()

    df_top = df.filter(col("video_id").isin(top_video_ids))

    # get latest stats using window
    window_latest = Window.partitionBy(
        "video_id").orderBy(desc("trending_date"))

    df_latest = df_top.withColumn("rn", row_number().over(window_latest)) \
                      .filter(col("rn") == 1) \
                      .select("video_id", "title", "description", "views", "likes", "dislikes")

    # aggregate trending_days info
    df_trending = df_top.select("video_id", "trending_date", "views", "likes", "dislikes") \
        .groupBy("video_id") \
        .agg(
            collect_list(
                struct(
                    col("trending_date").cast("string").alias("date"),
                    col("views").cast("int").alias("views"),
                    col("likes").cast("int").alias("likes"),
                    col("dislikes").cast("int").alias("dislikes")
                )
            ).alias("trending_days")
    )

    df_combined = df_latest.join(df_trending, on="video_id")

    video_schemas = []
    for row in df_combined.collect():
        video_schemas.append({
            "id": row["video_id"],
            "title": row["title"],
            "description": row["description"],
            "latest_views": int(row["views"]),
            "latest_likes": int(row["likes"]),
            "latest_dislikes": int(row["dislikes"]),
            "trending_days": [
                {
                    "date": day["date"],
                    "views": day["views"],
                    "likes": day["likes"],
                    "dislikes": day["dislikes"]
                } for day in row["trending_days"]
            ]
        })

    data = {"videos": video_schemas}

    with open('/opt/app/output/1_videos.json', 'w', encoding='UTF-8') as json_file:
        json.dump(data, json_file,
                  indent=4, ensure_ascii=False)

    return 0


def task_2(df):
    # change datatype trending_date to date
    df = df.withColumn("trending_date", to_date(
        col("trending_date"), "yy.dd.MM"))

    # add week start date
    df = df.withColumn(
        "week_start_date", date_sub(
            col("trending_date"), dayofweek(col("trending_date")) - 1)
    )

    # aggregate by week
    week_schemas = []
    week_data = df.groupBy("week_start_date", "category_id").agg(
        countDistinct("video_id").alias("number_of_videos"),
        _sum("views").alias("total_views"),
        collect_list("video_id").alias("video_ids"),
        first("source_file").alias("source_file")
    ).orderBy("week_start_date")

    for row in week_data.collect():
        start_date = row["week_start_date"]
        end_date = (start_date + timedelta(days=6))

        category_name = get_category_name(
            row["category_id"], row['source_file'])

        week_schema = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "category_id": row["category_id"],
            "category_name": category_name,
            "number_of_videos": row["number_of_videos"],
            "total_views": row["total_views"],
            "video_ids": row["video_ids"]
        }
        week_schemas.append(week_schema)

    data = {
        "weeks": week_schemas
    }

    with open('/opt/app/output/2_weeks.json', 'w', encoding='UTF-8') as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)

    return 0


def task_3(df):
    # # change datatype trending_date to date
    df = df.withColumn("trending_date", to_date(
        col("trending_date"), "yy.dd.MM"))

    df = df.withColumn("month_start_date", trunc("trending_date", "month"))

    df = df.withColumn("month_end_date", last_day("month_start_date"))

    video_tags_df = df.select("video_id", "tags").dropDuplicates(["video_id"])

    # join with original df
    video_tags_with_month = df.select("video_id", "month_start_date", "month_end_date").distinct() \
        .join(video_tags_df, on="video_id", how="left")

    # group by month and collect video IDs and tags
    month_data = video_tags_with_month.groupBy("month_start_date").agg(
        collect_list("video_id").alias("video_ids"),
        collect_list(struct("video_id", "tags")).alias("video_tags"),
        first("month_end_date").alias("month_end_date")
    ).orderBy("month_start_date")

    month_schemas = []
    for row in month_data.collect():
        start_date = row["month_start_date"]
        end_date = row["month_end_date"]

        dct = {}
        for video_id, video_tags in row["video_tags"]:
            if video_tags is None:
                try:
                    dct["[none]"].append(video_id)
                except Exception:
                    dct["[none]"] = []
                    dct["[none]"].append(video_id)
                finally:
                    continue
            for tag in video_tags.split("|"):
                tag = tag.strip('"')
                try:
                    dct[tag].append(video_id)
                except Exception:
                    dct[tag] = []
                    dct[tag].append(video_id)
        # Sort by length of value list in descending order
        # Keep fisrt 10 elements
        top_10_dct = dict(
            sorted(dct.items(), key=lambda item: len(item[1]), reverse=True)[:10])

        month_schema = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'tags': [
                {
                    'tag': tag,
                    'number_of_videos': len(video_list),
                    'video_ids':  video_list
                } for tag, video_list in top_10_dct.items()
            ]
        }
        month_schemas.append(month_schema)

        data = {
            "months": month_schemas
        }

        with open('/opt/app/output/3_month.json', 'w', encoding='UTF-8') as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)

    return 0


def task_4(df):
    # get last appearance of all videos per channel sorted in desc order by views
    w = Window.partitionBy("channel_title", "video_id").orderBy(
        col("trending_date").desc())
    df_latest = df.withColumn("rn", row_number().over(
        w)).filter(col("rn") == 1).drop("rn")

    # aggregate video stats for each channel
    video_stats_df = df_latest.select(
        "channel_title",
        "views",
        "trending_date",
        struct("video_id", "views").alias("video_stat")
    ).groupBy("channel_title").agg(
        # array of all video_stat in channel_title
        collect_list("video_stat").alias("videos_views"),
        _sum("views").alias("total_views"),
        _min("trending_date").alias("start_date"),
        _max("trending_date").alias("end_date")
    )

    # top 20 channels by total_views
    top_channels = video_stats_df.orderBy(col("total_views").desc()).limit(20)

    channels = [
        {
            "channel_name": row["channel_title"],
            "start_date": row["start_date"],
            "end_date": row["end_date"],
            "total_views": row["total_views"],
            "videos_views": [{"video_id": vs["video_id"], "views": vs["views"]} for vs in row["videos_views"]]
        }
        for row in top_channels.collect()
    ]

    data = {
        "channels": channels
    }

    with open('/opt/app/output/4_channel.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    return 0


def task_5(df):
    df = df.withColumn("trending_date", to_date(
        col("trending_date"), "yy.dd.MM"))

    # count unique trending days per video_id
    video_trending_days = df.groupBy("video_id", "title").agg(
        countDistinct("trending_date").alias("trending_days")
    )

    # keep only unique video_id
    video_info = df.select("video_id", "title",
                           "channel_title").dropDuplicates(["video_id"])

    # join video info and trending days info
    video_details = video_info.join(
        video_trending_days, on=["video_id", "title"])

    # Group by channel and collect video stats
    channel_agg = video_details.groupBy("channel_title").agg(
        _sum("trending_days").alias("total_trending_days"),
        collect_list(
            struct("video_id", col("title").alias(
                "video_title"), "trending_days")
        ).alias("videos_days")
    )

    #  top 10 channels by total_trending_days
    top_channels = channel_agg.orderBy(
        desc("total_trending_days")).limit(10).collect()

    data = {
        "channels": [
            {
                "channel_name": row["channel_title"],
                "total_trending_days": str(row["total_trending_days"]),
                "videos_days": [
                    {
                        "video_id": v["video_id"],
                        "video_title": v["video_title"],
                        "trending_days": v["trending_days"]
                    }
                    for v in row["videos_days"]
                ]
            }
            for row in top_channels
        ]
    }

    with open('/opt/app/output/5_channels.json', 'w', encoding='UTF-8') as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)

    return 0


def task_6(df):
    video_stats_df = df.withColumn("ratio", col("likes") / col("dislikes")).select(
        "video_id",
        "title",
        "views",
        "ratio",
        "category_id",
        "likes",
        "source_file",
        struct("video_id", "title", "views", "ratio",
               "source_file").alias("video_stat")
    ).filter("likes > 100000")

    window_spec = Window.partitionBy(
        "category_id").orderBy(col("ratio").desc())

    video_stats_df = video_stats_df.withColumn(
        "rank", row_number().over(window_spec))

    video_stats_df_top_10 = video_stats_df.filter(col("rank") <= 10)

    video_stats_df_top_10 = video_stats_df_top_10.groupBy("category_id").agg(
        collect_list("video_stat").alias("video_stats"))

    category = [
        {
            'category_id': row['category_id'],
            'category_name': get_category_name(row['category_id'], row['video_stats'][0]['source_file']),
            'videos': [{
                "video_id": vs["video_id"],
                "video_title": vs["title"],
                "ratio_likes_dislikes": vs["ratio"],
                "views": vs["views"]
            } for vs in row['video_stats']]
        } for row in video_stats_df_top_10.orderBy(col("video_stats.ratio").desc()).limit(10).collect()
    ]

    data = {
        'categories': category
    }

    with open('/opt/app/output/6_videos.json', 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)

    return 0


def main():
    spark = SparkSession.builder.appName('CountLinesApp').getOrCreate()

    files = [
        "/opt/app/data/CAvideos.csv",
        "/opt/app/data/DEvideos.csv",
        "/opt/app/data/FRvideos.csv",
        "/opt/app/data/GBvideos.csv",
        "/opt/app/data/INvideos.csv",
        "/opt/app/data/JPvideos.csv",
        "/opt/app/data/KRvideos.csv",
        "/opt/app/data/MXvideos.csv",
        "/opt/app/data/RUvideos.csv",
        "/opt/app/data/USvideos.csv"
    ]

    df = spark.read.option("header", True) \
        .option("quotes", '"') \
        .option("escape", "'") \
        .option("multiLine", True) \
        .option("inferSchema", True) \
        .csv(files) \
        .withColumn("source_file", input_file_name())  # to understand region for json

    # Convert to correct data types
    df = df.withColumn("views", df.views.cast('int')) \
        .withColumn("likes", df.likes.cast('int')) \
        .withColumn("dislikes", df.dislikes.cast('int')) \
        .withColumn("comment_count", df.comment_count.cast('int'))

    task_1(df)
    task_2(df)
    task_3(df)
    task_4(df)
    task_5(df)
    task_6(df)

    return 0


if __name__ == "__main__":
    main()
