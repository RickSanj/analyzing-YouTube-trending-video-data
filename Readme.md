# Analyzing YouTube Trending Video Data

**Author:** Anna Monastyrska

📝 **Description**  
This project focuses on processing and analyzing YouTube trending video data using Apache Spark inside a Dockerized environment. The goals of the project are:
- To calculate and aggregate analytical insights (e.g. top trending videos, channels, category-wise stats).
- To export results into JSON files.
- To automate the setup using Docker and share the results with screenshots and output samples.

**Tasks**
1) Find Top 10 videos that were amongst the trending videos for the highest 
number of days (it doesn't need to be a consecutive period of time). 
You should also include information about different metrics for each day 
the video was trending. The result should have the following schema:
		
{ <br>
	videos: array [video] <br>
}<br>

video’s schema:<br>
{ <br>
	id: String,<br>
	title: String,<br>
	description: String,<br>
	latest_views: Long, // this should be taken from the latest day video was trending<br>
	latest_likes: Long, // this should be taken from the latest day video was trending<br>
	latest_dislikes: Long, // this should be taken from the latest day video was trending<br>
	trending_days: array[trending_day]<br>
}<br>

trending_day’s schema<br>
{<br>
	date: String,<br>
	views: Long,<br>
	likes: Long,<br>
	dislikes: Long<br>
}<br>
<br>
2) Find what was the most popular category for each week (7 days slices). 
Popularity is decided based on the total number of views for videos of 
this category. Note, to calculate it you can’t just sum up the number of views.
If a particular video appeared only once during the given period, it shouldn’t be 
counted. Only if it appeared more than once you should count the number of new 
views. For example, if video A appeared on day 1 with 100 views, then on day 4 
with 250 views and again on day 6 with 400 views, you should count it as 400 - 100 = 300. 
For our purpose, it will mean that this particular video was watched 300 times 
in the given time period.  The result should have the following schema:

{<br>
	weeks: array[week]<br>
}<br>

week’s schema<br>
{ <br>
	start_date: String,<br>
	end_date: String,<br>
	category_id: Int,<br>
	category_name: String,<br>
	number_of_videos: Long,<br>
	total_views: Long,<br>
	video_ids: array[String]<br>
}<br>
<br>
3) What were the 10 most used tags amongst trending videos for each 30days time period? 
Note, if during the specified period the same video appears multiple times, 
you should count tags related to that video only once. The result should have the following 
schema:

{<br>
	months: array[month]<br>
}<br>

month’s schema:<br>
{ <br>
	start_date: String,<br>
	end_date: String,<br>
	tags: array[tag_stat]<br>
}<br>

tag_stat’s schema:<br>
{<br>
	tag: String,<br>
	number_of_videos: Long,<br>
	video_ids: array[String]<br>
}<br>

<br>
4) Show the top 20 channels by the number of views for the whole period. 
Note, if there are multiple appearances of the same video for some channel, 
you should take into account only the last appearance (with the highest 
number of views). The result should have the following schema:

{<br>
	channels: array[channel]<br>
}<br>

channel’s schema:<br>
{ <br>
	channel_name: String,<br>
	start_date: String,<br>
	end_date: String,<br>
	total_views: Long,<br>
	videos_views: array[video_stat]<br>
}<br>

video_stat’s schema:<br>
{<br>
	video_id: String, <br>
	views: Long<br>
}<br>

<br>
5) Show the top 10 channels with videos trending for the highest number of days 
(it doesn't need to be a consecutive period of time) for the whole period. 
In order to calculate it, you may use the results from the question №1. 
The total_trending_days count will be a sum of the numbers of trending days 
for videos from this channel. The result should have the following schema:

{<br>
	channels: array[channel]<br>
}<br>

channel’s schema<br>
{ <br>
	channel_name: String,<br>
	total_trending_days: String,<br>
  videos_days: array[video_day]<br>
}<br>

video_day schema:<br>
{<br>
	video_id: String, <br>
	video_title: String, <br>
	trending_days: Long<br>
}<br>

<br>
6) Show the top 10 videos by the ratio of likes/dislikes for each category 
for the whole period. You should consider only videos with more than 100K views. 
If the same video occurs multiple times you should take the record when 
the ratio was the highest. The result should have the following schema:

{<br>
	categories: array[category]<br>
}<br>

category’s schema:<br>
{ <br>
	category_id: Int,<br>
	category_name: String,<br>
	videos: array[video] <br>
}<br>

video’s schema:<br>
{ <br>
	video_id: String,<br>
	video_title: String,<br>
	ratio_likes_dislikes: Double,<br>
	views: Long<br>
}<br>

**Project structure**

.<br>
├── app<br>
│   ├── data<br>
│   │   ├── CA_category_id.json<br>
│   │   ├── CAvideos.csv<br>
│   │   ├── DE_category_id.json<br>
│   │   ├── DEvideos.csv<br>
│   │   ├── FR_category_id.json<br>
│   │   ├── FRvideos.csv<br>
│   │   ├── GB_category_id.json<br>
│   │   ├── GBvideos.csv<br>
│   │   ├── IN_category_id.json<br>
│   │   ├── INvideos.csv<br>
│   │   ├── JP_category_id.json<br>
│   │   ├── JPvideos.csv<br>
│   │   ├── KR_category_id.json<br>
│   │   ├── KRvideos.csv<br>
│   │   ├── MX_category_id.json<br>
│   │   ├── MXvideos.csv<br>
│   │   ├── RU_category_id.json<br>
│   │   ├── RUvideos.csv<br>
│   │   ├── US_category_id.json<br>
│   │   └── USvideos.csv<br>
│   ├── main.py<br>
│   └── output<br>
│       ├── 1_videos.json<br>
│       ├── 2_weeks.json<br>
│       ├── 3_month.json<br>
│       ├── 4_channel.json<br>
│       ├── 5_channels.json<br>
│       └── 6_videos.json<br>
├── docker-compose.yml<br>
├── Readme.md<br>
└── run.sh<br>

🖥️ **Usage**

### How to run the application

1. **Clone the repository**

    ```bash
    git clone <repository-url>
    cd <repository-folder>
    ```
2. **Add data folder**
    Add data folder in the same way as it is shown in project structure
    Create folder output in the same way as it is shown in project structure

3. **Run docker compose:**

    ```bash
    docker compose up
    ```

4. **Run ./run.sh:**

    ```bash
    chmod +x run.sh
    ./run.sh
    ```


### Results
<img src='0.png'>
<img src='1.png'>
<img src='2.png'>
<img src='3.png'>
<img src='4.png'>
<img src='5.png'>
<img src='6.png'>
