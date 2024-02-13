# importing required libraries
from googleapiclient.discovery import build
import mysql.connector as sql
import pandas as pd
from pymongo.mongo_client import MongoClient
from datetime import datetime
from PIL import Image
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px

#request data from youtube
api_key="your API key"
api_service_name = "youtube"
api_version = "v3"
youtube =build(api_service_name, api_version, developerKey=api_key)

#removing prefix from duration
def convert_duration_format(duration_str):
    # Remove 'PT' prefix from duration
    duration = duration_str[2:]

    # Initialize hours, minutes, and seconds
    hours = 0
    minutes = 0
    seconds = 0

    # Check if minutes and/or seconds are present in the duration string
    if "H" in duration:
        hours_index = duration.find("H")
        hours = int(duration[:hours_index])
        duration = duration[hours_index + 1:]
    if "M" in duration:
        minutes_index = duration.index("M")
        minutes = int(duration[:minutes_index])
        duration = duration[minutes_index + 1:]
    if "S" in duration:
        seconds_index = duration.index("S")
        seconds = int(duration[:seconds_index])

    # Convert minutes to hours and remaining minutes
    hours = minutes // 60
    minutes %= 60

    # Format the duration string without double quotes
    duration_str = f"{hours:02}:{minutes:02}:{seconds:02}"

    return duration_str

#convert date time format in iso format
def convert_to_mysql_datetime(iso_datetime):
    # Convert ISO datetime string to a datetime object
    dt_object = datetime.fromisoformat(iso_datetime)

    # Format the datetime object in MySQL datetime format
    mysql_datetime = dt_object.strftime("%Y-%m-%d %H:%M:%S")

    return mysql_datetime

#getting channel information
def get_channel_details(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    for i in response["items"]:
        data=dict(channel_name=i["snippet"]["title"],
                channel_id=i["id"],
                subscription_count=i['statistics']['subscriberCount'],
                channel_views=i['statistics']['viewCount'],
                total_videos=i['statistics']['videoCount'],
                channel_description=i['snippet']['description'],
                platlist_id=i['contentDetails']['relatedPlaylists']['uploads']
                )
    return data

#getting videos ids
def get_videos_ids(channel_id):
    videos_ids=[]
    response = youtube.channels().list(
                                        part="contentDetails",
                                        id=channel_id).execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=playlist_id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            videos_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return videos_ids

#getting videos informations
def get_video_details(videos_Ids):
    video_list=[]
    for video_id in videos_Ids:
        request=youtube.videos().list(
                                    part="snippet,contentDetails,statistics",
                                    id=video_id)
        response=request.execute()
        for item in response['items']:
            data=dict(Channel_name = item['snippet']['channelTitle'],
                    video_id=item['id'],
                    video_name=item['snippet']['title'],
                    video_description=item['snippet'].get('description'),
                    tags=item['snippet'].get('tags'),
                    published_at=convert_to_mysql_datetime(item['snippet'].get('publishedAt')),
                    view_count=item['statistics'].get('viewCount'),
                    like_count=item['statistics'].get('likeCount'),
                    favorite_count=item['statistics']['favoriteCount'],
                    comment_count=item['statistics'].get('commentCount'),
                    duration=convert_duration_format(item['contentDetails']['duration']),
                    thumnail=item['snippet']['thumbnails']['default']['url'],
                    caption_status=item['contentDetails']['caption'])
        video_list.append(data)
    return video_list

#getting comment informations
def get_comment_details(videos_Ids):
    comment_list=[]
    try:
        for id in videos_Ids:
            request=youtube.commentThreads().list(
                                                part="snippet",
                                                videoId=id)
            response=request.execute()

            for i in response['items']:
                data=dict(comment_id=i['snippet']['topLevelComment']['id'],
                        video_id=i['snippet']['topLevelComment']['snippet']['videoId'],  
                        comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_publistedAt=convert_to_mysql_datetime(i['snippet']['topLevelComment']['snippet']['publishedAt']))
                
                comment_list.append(data)

    except:
        pass
    
    return comment_list

# python mongodb connection

import pymongo
Client=pymongo.MongoClient("mongodb://localhost:27017")
db=Client["youtube"]

#data migrate to monodb

def channel_details(channel_id):
    channels= get_channel_details(channel_id)
    Videos_ids= get_videos_ids(channel_id)
    videos= get_video_details(Videos_ids)
    comments= get_comment_details(Videos_ids)

    collections=db["channel_details"]
    collections.insert_one({"channel_info":channels,"video_info":videos,"comment_info":comments})

    return "uploaded successfully"

# python mysql connection

mydb=sql.connect(host="localhost",
                    user="root",
                    password="sunil",
                    database="yt",
                )
cursor=mydb.cursor()

# create table and soring channel details into mysql
def channels_table():
    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()
    create_query='''create table if not exists channels(channel_name varchar(100),channel_id varchar(100) primary key,subscription_count bigint,
                                                            channel_views bigint,total_videos bigint,channel_description text,playlist_id varchar(100))'''
    cursor.execute(create_query)
    mydb.commit()

    ch_list=[]
    db=Client["youtube"]
    collections=db["channel_details"]
    for ch_data in collections.find({},{"_id":0,"channel_info":1}):
        ch_list.append(ch_data['channel_info'])

    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_name,channel_id,subscription_count,channel_views,total_videos,channel_description,playlist_id)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['channel_name'],row['channel_id'],row['subscription_count'],row['channel_views'],row['total_videos'],row['channel_description'],
                row['platlist_id'])

        cursor.execute(insert_query,values)
        mydb.commit()

# create table and soring videos details into mysql
def vidoes_table():
        drop_query='''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists videos(Channel_name varchar(100), video_id varchar(255) primary key,
                                                        video_name varchar(255),video_description text,published_at datetime,
                                                        view_count bigint,like_count bigint,favorite_count int,comment_count bigint,
                                                        duration time,thumnail varchar(500),caption_status varchar(255))'''
        cursor.execute(create_query)
        mydb.commit()

        vi_list=[]
        db=Client["youtube"]
        collections=db["channel_details"]
        for vi_data in collections.find({},{"_id":0,"video_info":1}):
                for i in range(len(vi_data['video_info'])):
                        vi_list.append(vi_data['video_info'][i])
        df1=pd.DataFrame(vi_list)

        for index,row in df1.iterrows():
                        insert_query='''insert into videos(Channel_name,video_id,video_name,video_description,published_at,view_count,
                                                        like_count,favorite_count,comment_count,duration,thumnail,caption_status)
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                        values=(row['Channel_name'],row['video_id'],row['video_name'],row['video_description'],row['published_at'],
                                row['view_count'],row['like_count'],row['favorite_count'],row['comment_count'],row['duration'],
                                row['thumnail'],row['caption_status'])
                        cursor.execute(insert_query,values)
                        mydb.commit()

# create table and soring comment details into mysql

def comments_table():
        drop_query='''drop table if exists comments'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists comments(comment_id VARCHAR(255) PRIMARY KEY,video_id VARCHAR(255),
                                                        comment_text TEXT,comment_author VARCHAR(150),comment_publistedAt datetime)'''

        cursor.execute(create_query)
        mydb.commit()

        com_list=[]
        db=Client["youtube"]
        collections=db["channel_details"]
        for com_data in collections.find({},{"_id":0,"comment_info":1}):
                for i in range(len(com_data['comment_info'])):
                        com_list.append(com_data['comment_info'][i])
        df2=pd.DataFrame(com_list)

        for index,row in df2.iterrows():
                insert_query='''insert into comments(comment_id,video_id,comment_text,comment_author,comment_publistedAt)
                                                values(%s,%s,%s,%s,%s)'''

                values=(row['comment_id'],row['video_id'],row['comment_text'],row['comment_author'],row['comment_publistedAt'])

                cursor.execute(insert_query,values)
                mydb.commit()

def tables():
    channels_table()
    vidoes_table()
    comments_table()
    return "create successfully"

def view_channels_table():
    ch_list=[]
    db=Client["youtube"]
    collections=db["channel_details"]
    for ch_data in collections.find({},{"_id":0,"channel_info":1}):
        ch_list.append(ch_data['channel_info'])

    df=st.dataframe(ch_list)
    return df

def view_videos_table():
        vi_list=[]
        db=Client["youtube"]
        collections=db["channel_details"]
        for vi_data in collections.find({},{"_id":0,"video_info":1}):
                for i in range(len(vi_data['video_info'])):
                        vi_list.append(vi_data['video_info'][i])
        df1=st.dataframe(vi_list)
        return df1

def view_comments_table():
        com_list=[]
        db=Client["youtube"]
        collections=db["channel_details"]
        for com_data in collections.find({},{"_id":0,"comment_info":1}):
                for i in range(len(com_data['comment_info'])):
                        com_list.append(com_data['comment_info'][i])
        df2=st.dataframe(com_list)
        return df2

#some example samples
channel_details={"channel name":["Candy Crafts","Hari zone","Money Maven","Digital Sculler","techTFQ","HR_Navin","Udemy","Reeload Roast","Hareesh Rajendran","Tech Support Tamil"],
                 "channel ID":["UCYWXUZGfp9FpIeLeM5n_DEA","UCITaV_WWRm6bPzYhJZ5Jnmw","UCZpgNrd1zm5TXnO_3DsP2NQ","UCcskSCtpiScqJrHTqrqrmbg","UCnz-ZXXER4jOvuED5trXfEA","UC-O3_F-UpwzKvSkvO0DW9qg","UCzw4hbQIePVtyJQzE_F8QDg","UCCO1WTlxp8JTS4GqjxpDhdw","UCJQJAI7IjbLcpsjWdSzYz0Q","UCeJTusc2HHBFtdOpJ-xB8sw"]
                 }

# streamlit

# SETTING PAGE CONFIGURATIONS
icon = Image.open("Youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By sunilkumar",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *sunilkumar!*"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract and Transform","Query"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "5000px"},
                                   "nav-link-selected": {"background-color": "#F1E419"}})


# HOME PAGE
if selected == "Home":
    # Title Image
    st.image("youtubeMain.png")
    st.markdown("## :blue[Domain] : Social Media")
    st.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    st.markdown("## :blue[Skills Take away] :Python scripting, Data Collection,MongoDB, Streamlit, API integration, Data Management using MongoDB (Atlas) and SQL")
    st.markdown("#   ")
    st.markdown("#   ")


if selected == "Extract and Transform":
    tab1,tab2 = st.tabs(["$\huge EXTRACT $", "$\huge TRANSFORM $"])

    with tab1:

        channel_Id=st.text_input("Enter the channel_id")

        if st.button("collect and store data"):
            ch_ids=[]
            db=Client["youtube"]
            collections=db["channel_details"]
            for ch_data in collections.find({},{"_id":0,"channel_info":1}):
                ch_ids.append(ch_data['channel_info']['channel_id'])
            
            if channel_Id in ch_ids:
                st.success("channel ID details already exists")

            else:
                insert=channel_details(channel_Id)
                st.success(insert)
        channel_IDS=st.table(df0)

        show_table=st.radio("select the table for view",("CHANNELS","VIDEOS","COMMENTS"))

        if show_table=="CHANNELS":
            view_channels_table()   

        elif show_table=="VIDEOS":
            view_videos_table()

        elif show_table=="COMMENTS":
            view_comments_table()

    with tab2:
        if st.button("migrate to mysql"):
            Table=tables()
            st.success(Table)
            st.success("Transformation to MySQL Successful!!!")



if selected == "Query":
    #mysql connection
    mydb=sql.connect(host="localhost",
                        user="root",
                        password="sunil",
                        database="yt",
                    )
    cursor=mydb.cursor()

    # VIEW PAGE
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'],index=0)

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("""SELECT video_name AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("""SELECT channel_name as Channel_Name, total_videos as Total_Videos FROM channels
                            ORDER BY total_videos DESC limit 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        fig = px.bar(df,
                        x=cursor.column_names[0],
                        y=cursor.column_names[1],
                        orientation='v',
                        color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Title, view_count AS Views FROM videos ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names,index=[1,2,3,4,5,6,7,8,9,10])
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                        x=cursor.column_names[2],
                        y=cursor.column_names[1],
                        orientation='h',
                        color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("""SELECT video_name as Video_Name, comment_count as Comment_Counts FROM videos WHERE comment_count IS NOT NULL ORDER BY comment_count desc """)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write("### :green[videos with number of comments :]")
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,video_name AS Title,like_count AS Likes_Count 
                            FROM videos ORDER BY like_count DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names,index=[1,2,3,4,5,6,7,8,9,10])
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                        x=cursor.column_names[1],
                        y=cursor.column_names[2],
                        orientation='v',
                        color=cursor.column_names[0],
                        title="Wide-Form Input"
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT video_name AS Title, like_count AS Likes_Count
                            FROM videos
                            ORDER BY like_count DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write("### :green[Channels with total number of likes :]")
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                            FROM channels
                            ORDER BY channel_views DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write("### :green[Channels vs Views :]")
        st.write(df)
        fig = px.bar(df,
                        x=cursor.column_names[0],
                        y=cursor.column_names[1],
                        orientation='v',
                        color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT channel_name AS Channel_Name FROM videos WHERE published_at LIKE '2022%' GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write("### :green[channels to published videos in the year 2022 :]")
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,AVG(duration) AS "Average_Video_Duration (mins)" FROM videos
                                GROUP BY channel_name ORDER BY AVG(duration) DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write("### :green[Average video duration for channels :]")
        st.write(df)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,comment_count AS Comment FROM videos ORDER BY comment_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names,index=[1,2,3,4,5,6,7,8,9,10])
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                        x=cursor.column_names[0],
                        y=cursor.column_names[1],
                        orientation='v',
                        color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
