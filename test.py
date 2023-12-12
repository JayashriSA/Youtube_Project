import streamlit as st
from pprint import pprint
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import gridfs
api_service_name = "youtube"
api_version = "v3"
api_key = 'AIzaSyCxScUsqtwf6y2U3716q5_G6ykeMpn8ZEM'


youtube = build('youtube', 'v3', developerKey=api_key)
def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    )
    response = request.execute()
    

    data = dict(
                    Channel_Name = response["items"][0]["snippet"]["title"],
                    Channel_Id = response["items"][0]["id"],
                    Subscription_Count= response["items"][0]["statistics"]["subscriberCount"],
                    Views = response["items"][0]["statistics"]["viewCount"],
                    Total_Videos = response["items"][0]["statistics"]["videoCount"],
                    Channel_Description = response["items"][0]["snippet"]["description"],
                    Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
    )

    return data

def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
            
    return All_data

def get_video_ids(channel_id):
    
    video_ids = []
 
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        
        if next_page_token is None:
            break
    return tuple(video_ids)



def get_video_details(video_ids):
    all_video = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                        Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        video_name = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        video_description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        view_count = item['statistics']['viewCount'],
                        like_count = item['statistics'].get('likeCount'),
                        dislike_count = item['statistics'].get('dislikeCount'),
                        comment_count = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount']
            )
            all_video.append(data)

    return all_video




def get_comment_info(video_ids):
        Comment_Information = []
        
        for video_id in video_ids:
                try:

                    request = youtube.commentThreads().list(
                            part = "snippet",
                            videoId = video_id,
                            maxResults = 50
                            )
                    response = request.execute()

                    for item in response["items"]:
                            comment_information = dict(
                                    Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                    Video_Id = item["snippet"]["videoId"],
                                    Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                    Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                            Comment_Information.append(comment_information)
                except:
                        pass

        return Comment_Information

client = pymongo.MongoClient("mongodb+srv://Jayashri:Jayashri@atlascluster.slrextp.mongodb.net/")
mydb = client['Youtube_Project']


def channel_details(channel_id):
    ch_details = get_channel_details(channel_id)
    pl_details = get_playlist_info(channel_id)
    video_ids = get_video_ids(channel_id)    
    vi_details = get_video_details(video_ids)
    com_details = get_comment_info(video_ids)

    d = {"channel_data":ch_details,"video_details":vi_details,"comment_details":com_details}
    return d

mycol = mydb["Youtubeinfo"]
fs = gridfs.GridFS(mydb, collection="large_files")

def store_large_data(d, Youtubeinfo):
    file_id = fs.put(d, filename=Youtubeinfo)
    return file_id   
mycon=mysql.connector.connect(host = "localhost", user = "root" , password = "Jayashri@16",auth_plugin="mysql_native_password"
                             ,autocommit = True,charset='utf8mb4')



mycursor =mycon.cursor(buffered=True)
mycursor.execute('use youtube')

def intosql(Channel_Name):
    mycursor.execute('''CREATE TABLE  IF NOT EXISTS channel_details(Channel_Name VARCHAR(255),Channel_Id VARCHAR(255),Subscription_Count VARCHAR(255),Views VARCHAR(255),Total_Videos VARCHAR(25),Channel_Description text,Playlist_Id VARCHAR(255))''')

    result = mycol.find_one({'channel_data.Channel_Name' :Channel_Name },{'_id':0})
    if result:

        doc = tuple(result['channel_data'].values())


        S1 = '''Insert into channel_details(Channel_Name ,
                                            Channel_Id ,
                                            Subscription_Count ,
                                            Views ,
                                            Total_Videos ,
                                            Channel_Description ,
                                            Playlist_Id) VALUES (%s,%s,%s,%s,%s,%s,%s)'''

        mycursor.execute(S1,doc)
        mycon.commit()

        mycursor.execute('''CREATE TABLE  IF NOT EXISTS video_details(Channel_Name VARCHAR(255),Channel_Id VARCHAR(255),video_Id VARCHAR(255),video_name VARCHAR(255),Tags VARCHAR(255),Thumbnail VARCHAR(255),video_description VARCHAR(5000),Published_Date VARCHAR(255),Duration VARCHAR(255),view_count VARCHAR(255),like_count VARCHAR(255),dislike_count VARCHAR(255),comment_count VARCHAR(255),Favourite_count VARCHAR(255))''')


        S1 = '''Insert into video_details(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            video_name,
                                            Tags,
                                            Thumbnail,
                                            video_description,
                                            Published_Date,
                                            Duration,
                                            view_count,
                                            like_count,
                                            dislike_count,
                                            comment_count,
                                            Favourite_Count) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' 

        data = [tuple(item.values()) for item in result['video_details']]

        for entry in data:
            tags_index = list(result['video_details'][0].keys()).index('Tags')
            tags = entry[tags_index]

            if isinstance(tags, list):
                entry_list = list(entry)
                entry_list[tags_index] = ', '.join(tags)[:255]  # Truncate to fit the VARCHAR(255) limit
                entry = tuple(entry_list)
            elif isinstance(tags, dict):
                entry_list = list(entry)
                entry_list[tags_index] = ', '.join(f"{key}: {value}" for key, value in tags.items())[:255]
                entry = tuple(entry_list)

            mycursor.execute(S1, entry)

        mycon.commit()

        mycursor.execute('''CREATE TABLE IF NOT EXISTS comment_details(Comment_Id VARCHAR(255),Video_Id VARCHAR(255),Comment_Text VARCHAR(15000),Comment_Author VARCHAR(255),Comment_Published VARCHAR(25))''')


        S1 = '''Insert into comment_details(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published) VALUES(%s,%s,%s,%s,%s)''' 

        data = [tuple(item.values()) for item in result['comment_details']]

        for entry in data:
            mycursor.execute(S1, entry)
    else:
        st.warning(f"No data found for channel ID: {channel_id}")

with st.sidebar:
     st.title(":red[YOUTUBE DATA HARWESTING AND DATA WAREHOUSING]")
     st.header("Skill Take Away")
     st.caption("Python Scripting")
     st.caption("Data Collection")
     st.caption("Mongo DB")
     st.caption("API Integration")
     st.caption("Data Management using Mongodb and SQL")

channel_id = st.text_input("Enter the Channel ID")

if channel_id and st.button("Scrape"):
    data = channel_details(channel_id)
    st.write(data)
if channel_id and st.button("Insert into MongoDB"):
    data = channel_details(channel_id)

    # Insert data into MongoDB
    mycol.insert_one(data)
    st.success("Data inserted into MongoDB successfully!")

available_channel_ids = mycol.distinct("channel_data.Channel_Name")
channel_id = st.selectbox("Select the Channel Name from Mongodb", available_channel_ids)

if  channel_id and st.button("Migrate into SQL"):
    data = intosql(channel_id)
    mycursor.execute(data)
    st.write(data)

    st.success('Data  migrated')
else:
    st.warning(f"None")



question = st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                                "2.Which channels have the most number of videos, and how many videos do they have ?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
if question== "1.What are the names of all the videos and their corresponding channels?":
    query1 = '''select video_name as Title , Channel_Name as ChannelName from video_details'''
    mycursor.execute(query1)
    mycon.commit()
    t1= mycursor.fetchall()
    df1 = pd.DataFrame(t1,columns=['Title','Channel Name'])
    st.write(df1)
elif question== "2.Which channels have the most number of videos, and how many videos do they have ?":
    query2 = '''select Channel_Name as ChannelName , Total_Videos as No_of_Videos from channel_details'''
    mycursor.execute(query2)
    mycon.commit()
    t2= mycursor.fetchall()
    df2 = pd.DataFrame(t2,columns=['ChannelName','No_of_Videos'])
    st.write(df2)
elif question== "3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select view_count as Views , Channel_Name as ChannelName,video_name as Video_Title from video_details
                 where view_count is not null order by view_count desc limit 10'''
    mycursor.execute(query3)
    mycon.commit()
    t3= mycursor.fetchall()
    df3 = pd.DataFrame(t3,columns=['Views','ChannelName','Title'])
    st.write(df3)
elif question== "4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comment_count as No_Comment, video_name as Video_Title from video_details'''
    mycursor.execute(query4)
    mycon.commit()
    t4= mycursor.fetchall()
    df4 = pd.DataFrame(t4,columns=['No of Comment','Title'])
    st.write(df4)    
elif question== "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select video_name as Video_Title , Channel_Name as ChannelName, like_count as Likes from video_details
                where like_count is not null order by like_count desc'''
    mycursor.execute(query5)
    mycon.commit()
    t5= mycursor.fetchall()
    df5 = pd.DataFrame(t5,columns=['Video_Title','ChannelName','Likes'])
    st.write(df5)   
elif question== "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''select video_name as Video_Title , Channel_Name as ChannelName, like_count as Likes, dislike_count as Dislikes from video_details'''
    mycursor.execute(query6)
    mycon.commit()
    t6= mycursor.fetchall()
    df6 = pd.DataFrame(t6,columns=['Video_Title','ChannelName','Likes','Dislikes'])
    st.write(df6)
elif question== "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select Channel_Name as ChannelName , Views as Total_views from channel_details'''
    mycursor.execute(query7)
    mycon.commit()
    t7= mycursor.fetchall()
    df7 = pd.DataFrame(t7,columns=['ChannelName','Total_views'])
    st.write(df7)
elif question== "8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select video_name as Video_Title ,Published_Date as video_release, Channel_Name as ChannelName from video_details
                where EXTRACT(year from Published_Date) = 2022'''
    mycursor.execute(query8)
    mycon.commit()
    t8= mycursor.fetchall()
    df8 = pd.DataFrame(t8,columns=['Video_Title','video_release','ChannelName'])
    st.write(df8)
elif question== "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select Channel_Name as ChannelName, AVG(TIME_TO_SEC(ADDTIME( ADDTIME('00:00:00', SUBSTRING(Duration, 3, CHAR_LENGTH(Duration) - 4)),ADDTIME('00:00:00', SUBSTRING(Duration, CHAR_LENGTH(Duration) - 1))))) as Average_duration from video_details
                group by Channel_Name'''
    mycursor.execute(query9)
    mycon.commit()
    t9= mycursor.fetchall()
    df9 = pd.DataFrame(t9,columns=['ChannelName','Average_duration'])
    
    T9 =[]
    for i,r in df9.iterrows():
        channel_name = r['ChannelName']
        average_duration = r['Average_duration']
        average_duration_str = str(average_duration)
        T9.append(dict(channel_Name= channel_name, Average_duration=average_duration_str))
    df = pd.DataFrame(T9)
    st.write(df)

elif question== "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select video_name AS Video_Title ,Channel_Name AS ChannelName ,comment_count AS Total_Comments FROM video_details
                  where comment_count IS NOT NULL order by comment_count desc'''
    mycursor.execute(query10)
    mycon.commit()
    t10= mycursor.fetchall()
    df10 = pd.DataFrame(t10,columns=['Video_Title','ChannelName','Total_Comments'])
    st.write(df10)
    

    




     
     
     
   
    
