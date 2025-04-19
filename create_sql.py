import streamlit as st
import psycopg2

# PostgreSQL connection using secrets
conn = psycopg2.connect(
    host=st.secrets["postgres"]["host"],
    port=st.secrets["postgres"]["port"],
    database=st.secrets["postgres"]["database"],
    user=st.secrets["postgres"]["user"],
    password=st.secrets["postgres"]["password"]
)
cursor = conn.cursor()

# Create channels table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS channels (
        channel_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255),
        channel_views INT,
        channel_description TEXT,
        channel_subscription_count INT
    );
""")

# Create playlist table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS playlist (
        playlist_id VARCHAR(255) PRIMARY KEY,
        channel_id VARCHAR(255),
        playlist_name VARCHAR(255)
    );
""")

# Create videos table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        video_id VARCHAR(255) PRIMARY KEY,
        channel_id VARCHAR(255),
        video_name VARCHAR(255),
        video_description TEXT,
        published_date TIMESTAMP,
        view_count INT,
        like_count INT,
        favorite_count INT,
        comment_count INT,
        video_duration INT,
        thumbnail VARCHAR(255),
        caption_satatus VARCHAR(255)
    );
""")

# Create comments table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        comment_id VARCHAR(255) PRIMARY KEY,
        video_id VARCHAR(255),
        comment_text TEXT,
        comment_author VARCHAR(255),
        comment_published_date TIMESTAMP
    );
""")

# Commit the changes (very important)
conn.commit()
