import streamlit as st
import pymongo
import psycopg2
import insert_to_sql1 as i
from isodate import parse_duration
import os



from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
from isodate import parse_duration
import streamlit as st

api_key = st.secrets["API_KEY"]
youtube = build('youtube', 'v3', developerKey=api_key)


def channel_info(channel_ids):
    channels = []
    request = youtube.channels().list(part='snippet,statistics,contentDetails',id = channel_ids)
    response = request.execute()
    data = dict(
          Channel_Name = response['items'][0]['snippet']['title'],
          Channel_Id = response['items'][0]['id'],
          Channel_Description = response['items'][0]['snippet']['description'],
          Channel_Views = response['items'][0]['statistics']['viewCount'],
          Subscription_Count = response['items'][0]['statistics']['subscriberCount'] ,
          Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
          Playlist_Name = response['items'][0]['snippet']['title'])
    channels.append(data)
    return channels  

def playlist(channel_id):
    playlists = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails', 
            channelId= channel_id ,
            maxResults=50,
            pageToken = next_page_token
            )
        response = request.execute()
            
        for playlist in response.get('items',[]):
            data = dict(
                channel_id = playlist['snippet']['channelId'],
                playlist_id = playlist['id'],
                playlist_name = playlist['snippet']['title']
                )
            playlists.append(data)
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
            
    return playlists


from pprint import pprint
def video_info(channel_id):
    request = youtube.channels().list(id=channel_id, part='contentDetails')
    response = request.execute()

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    videos_details = []
    next_page_token = None

    while True:
        video_id_request = youtube.playlistItems().list(
            playlistId=playlist_id,
            part='snippet',
            maxResults=50,
            pageToken=next_page_token
        )
        video_id_response = video_id_request.execute()

        for item in video_id_response.get('items', []):
            try:
                video_id = item['snippet']['resourceId']['videoId']

                video_request = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=video_id
                )
                video_response = video_request.execute()

                if "items" not in video_response or not video_response["items"]:
                    continue  # skip if video is invalid or deleted

                video_data = video_response['items'][0]
                stats = video_data.get('statistics', {})
                snippet = video_data.get('snippet', {})
                content = video_data.get('contentDetails', {})

                view_count = int(stats.get('viewCount', 0))
                like_count = int(stats.get('likeCount', 0))
                favorite_count = int(stats.get('favoriteCount', 0))
                comment_count = int(stats.get('commentCount', 0))

                data = dict(
                    video_id=video_data.get('id', ''),
                    channel_id=snippet.get('channelId', ''),
                    video_name=snippet.get('title', ''),
                    video_description=snippet.get('description', ''),
                    video_duration=int(parse_duration(content.get('duration', 'PT0S')).total_seconds()),
                    publishedat=datetime.strptime(snippet.get('publishedAt', '1970-01-01T00:00:00Z'), "%Y-%m-%dT%H:%M:%SZ"),
                    view_count=view_count,
                    like_count=like_count,
                    dislike_count=max(view_count - like_count, 0),  # avoid negative values
                    favorite_count=favorite_count,
                    comment_count=comment_count,
                    thumbnail=snippet.get('thumbnails', {}).get('default', {}).get('url', ''),
                    caption=content.get('caption', 'false')
                )
                videos_details.append(data)
            except Exception as e:
                print(f"Skipping video due to error: {e}")
                continue

        next_page_token = video_id_response.get('nextPageToken')
        if not next_page_token:
            break

    return videos_details


def comment_info(ids):
    comments = []
    for i in ids:
        try:
            
            comments_request = youtube.commentThreads().list(
                        part='snippet',
                        videoId=i,
                        textFormat='plainText',
                        maxResults=50)
            comments_response = comments_request.execute()
            for item in comments_response['items']:
                data = dict(comment_id = item['id'],
                            video_id = item['snippet']['videoId'],
                            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_publishedat =item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comments.append(data) 
        except:
            pass
              
    
    return comments



def main(channel_id):
    channel = channel_info(channel_id)
    playlists = playlist(channel_id)
    videos = video_info(channel_id)
    v_ids = []
    
    for item in videos:
        v_ids.append(item['video_id'])
    comment_details = comment_info(v_ids)
    data = {
        'channel_details': channel,
        'channel_playlists': playlists,
        'video_details': videos,
        'comment_details': comment_details
       }
    
    return data    

MONGO_URI = st.secrets["MONGO_URI"]
myclient = pymongo.MongoClient(MONGO_URI)
mydb = myclient["YTH"]
mycol = mydb["channel_data"]


#Streamlit 
st.write("# YouTube Data Harvesting ")
if 'ch_data' not in st.session_state:
    st.session_state.ch_data = []
      

channel_id = st.sidebar.text_input("Enter Channel_id:")

# Check if the "Fetch Data" button is clicked
if st.sidebar.button("Fetch Data"):
    if channel_id:
        existing_channel = mycol.find_one({"channel_details.Channel_Id": channel_id})
        if existing_channel:
            st.warning("This channel_id ia already exists in the database.")
        else:    
            data = main(channel_id)
            st.session_state.ch_data.append(data)
            if data:
                mycol.insert_one(data)
            else:
                st.warning("Please enter a channel ID before store data.")
    else:
        st.warning("Please enter a channel ID before fetching data.")


if st.session_state.ch_data:
     st.write(st.session_state.ch_data[-1])

channel_names = [channel['channel_details'][0]['Channel_Name'] for channel in mycol.find()]



st.sidebar.selectbox('Channels', channel_names[::-1])
if st.sidebar.button('store_in_sql'):
    i.insert_sql()



conn = psycopg2.connect(
    host=st.secrets["postgres"]["host"],
    port=st.secrets["postgres"]["port"],
    database=st.secrets["postgres"]["database"],
    user=st.secrets["postgres"]["user"],
    password=st.secrets["postgres"]["password"]
)

cursor = conn.cursor()

table_options = ["--select-query--",
                 "1)What are the names of all the videos and their corresponding channels?", 
                 "2)Which channels have the most number of videos, and how many videos dothey have?", 
                 "3)What are the top 10 most viewed videos and their respective channels?",
                 "4)How many comments were made on each video, and what are their corresponding video names?",
                 "5)Which videos have the highest number of likes, and what are their corresponding channel names?",
                 "6)What is the total number of likes and what are their corresponding video names?",
                 "7)What is the total number of views for each channel, and what are their corresponding channel names?",
                 "8)What are the names of all the channels that have published videos in the year 2022?",
                 "9)What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                 "10)Which videos have the highest number of comments, and what are their corresponding channel names?"]
selected_table = st.sidebar.selectbox("Questions", table_options)

# Display the selected table data when a table is selected
if selected_table == "1)What are the names of all the videos and their corresponding channels?":
    
    cursor.execute("""select c.channel_name,v.video_name from channels as c inner join videos as v on c.channel_id = v.channel_id;""")
    data = cursor.fetchall()
    st.dataframe(data)
elif selected_table == "2)Which channels have the most number of videos, and how many videos dothey have?":
    cursor.execute("""select c.channel_name,count(v.video_id) as video_count from channels as c
                        inner join videos as v on c.channel_id = v.channel_id 
                        group by c.channel_name
                        order by video_count desc
                        limit 1;""")
    data = cursor.fetchall()
    st.dataframe(data)
elif selected_table == "3)What are the top 10 most viewed videos and their respective channels?":
    cursor.execute("""select c.channel_name , v.video_name, v.view_count from videos as v
                        inner join channels as c on c.channel_id = v.channel_id
                        order by v.view_count desc 
                        limit 10;""")
    data = cursor.fetchall()
    st.dataframe(data)
elif selected_table == "4)How many comments were made on each video, and what are their corresponding video names?":
   cursor.execute("""select video_name,comment_count from videos;""")
   data = cursor.fetchall()
   st.dataframe(data) 
elif selected_table == "5)Which videos have the highest number of likes, and what are their corresponding channel names?":
   cursor.execute("""select c.channel_name,v.video_name , v.like_count max_likes from videos as v
                        inner join channels as c on v.channel_id = c.channel_id
                        order by v.like_count desc
                        limit 10;""")
   data = cursor.fetchall()
   st.dataframe(data)
elif selected_table == "6)What is the total number of likes and what are their corresponding video names?":
   cursor.execute("""select video_name,like_count from videos;""")
   data = cursor.fetchall()
   st.dataframe(data)
elif selected_table == "7)What is the total number of views for each channel, and what are their corresponding channel names?":
   cursor.execute("""select channel_name,channel_views from channels
                        order by channel_views desc;""")
   data = cursor.fetchall()
   st.dataframe(data)
elif selected_table == "8)What are the names of all the channels that have published videos in the year 2022?":
   cursor.execute("""select distinct c.channel_name
                        from channels AS c
                        inner join  videos as v on c.channel_id = v.channel_id
                        where extract(YEAR FROM v.published_date) = 2022;""")
   data = cursor.fetchall()
   st.dataframe(data)
elif selected_table == "9)What is the average duration of all videos in each channel, and what are their corresponding channel names?":
   cursor.execute("""select c.channel_name,avg(v.video_duration) as avg_duration from channels as c
                        inner join videos as v on c.channel_id = v.channel_id
                        group by c.channel_name order by avg_duration desc;""")
   data = cursor.fetchall()
   st.dataframe(data) 
elif selected_table == "10)Which videos have the highest number of comments, and what are their corresponding channel names?":
   cursor.execute("""select v.video_name, v.comment_count ,c.channel_name from channels as c
                        inner join videos as v on c.channel_id = v.channel_id
                        order by v.comment_count desc
                        limit 10;""")

   data = cursor.fetchall()
   st.dataframe(data)  
cursor.close()
conn.close()                       