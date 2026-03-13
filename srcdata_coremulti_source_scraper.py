"""
Multi-Source Intelligence Engine
Purpose: Scrapes internal lessons + 3 external sentiment sources with validation
Architecture Choice: Abstract base class enables easy addition of new sources
Error Handling: Timeouts, rate limits, format changes, network failures
"""
import asyncio
import logging
import json
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import requests
from requests.exceptions import RequestException, Timeout
import praw
import tweepy
from textblob import TextBlob
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
import ccxt
from google.cloud import firestore

logger = logging.getLogger(__name__)

class DataSource(ABC):
    """Abstract base class for all data sources"""
    
    @abstractmethod
    def fetch_data(self) -> Dict[str, Any]:
        """Fetch raw data from source"""
        pass
    
    @abstractmethod
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """Validate data integrity and format"""
        pass
    
    @abstractmethod
    def get_source_name(self) -> str:
        """Return source identifier"""
        pass

class InternalLessonsSource(DataSource):
    """Scrapes internal strategic lessons from markdown files"""
    
    def __init__(self, lessons_directory: str = "./lessons"):
        self.lessons_directory = lessons_directory
        self.required_extensions = {'.md', '.txt', '.json'}
        self._validate_directory()
    
    def _validate_directory(self) -> None:
        """Ensure lessons directory exists and contains files"""
        if not os.path.exists(self.lessons_directory):
            logger.warning(f"Lessons directory not found: {self.lessons_directory}")
            os.makedirs(self.lessons_directory, exist_ok=True)
            # Create sample lesson file if empty
            sample_path = os.path.join(self.lessons_directory, "sample_lesson.md")
            with open(sample_path, 'w') as f:
                f.write("# Strategic Lesson: Market Sentiment Analysis\n\nAlways verify multiple data sources before making trading decisions.")
    
    def fetch_data(self) -> Dict[str, Any]:
        """Parse all markdown lesson files into structured data"""
        lessons_data = {
            'source': 'internal_lessons',
            'timestamp': datetime.utcnow().isoformat(),
            'lessons': [],
            'total_files': 0,
            'file_errors': 0
        }
        
        try:
            for filename in os.listdir(self.lessons_directory):
                file_path = os.path.join(self.lessons_directory, filename)
                
                # Skip non-files and unsupported extensions
                if not os.path.isfile(file_path):
                    continue
                
                _, ext = os.path.splitext(filename)
                if ext.lower() not in self.required_extensions:
                    continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Basic markdown parsing
                    lesson = {
                        'filename': filename,
                        'content': content,
                        'word_count': len(content.split()),
                        'sentiment_score': self._analyze_sentiment(content),
                        'topics': self._extract_topics(content)
                    }
                    lessons_data['lessons'].append(lesson)
                    lessons_data['total_files'] += 1
                    
                except Exception as e:
                    logger.error(f"Error reading file {filename}: {e}")
                    lessons_data['file_errors'] += 1
            
            logger.info(f"Processed {lessons_data['total_files']} internal lesson files")
            return lessons_data
            
        except Exception as e:
            logger.error(f"Critical error in internal lessons scraper: {e}")
            return lessons_data  # Return partial data
    
    def _analyze_sentiment(self, text: str) -> float:
        """Calculate TextBlob sentiment polarity (-1 to 1)"""
        try:
            analysis = TextBlob(text)
            return float(analysis.sentiment.polarity)
        except:
            return 0.0
    
    def _extract_topics(self, text: str) -> List[str]:
        """Extract potential topics using simple keyword matching"""
        crypto_keywords = ['bitcoin', 'ethereum', 'defi', 'nft', 'trading', 'sentiment', 'market']
        topics = []
        text_lower = text.lower()
        
        for keyword in crypto_keywords:
            if keyword in text_lower:
                topics.append(keyword)
        
        return topics[:5]  # Limit to top 5 topics
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """Validate internal lessons data structure"""
        required_keys = ['source', 'timestamp', 'lessons', 'total_files']
        
        if not all(key in data for key in required_keys):
            return False
        
        if not isinstance(data['lessons'], list):
            return False
        
        if data['total_files'] == 0:
            logger.warning("No lesson files found - continuing with empty dataset")
        
        return True
    
    def get_source_name(self) -> str:
        return "internal_lessons"

class RedditSentimentSource(DataSource):
    """Fetches crypto sentiment from Reddit using PRAW"""
    
    def __init__(self, subreddits: List[str] = None):
        self.subreddits = subreddits or ['CryptoCurrency', 'Bitcoin', 'ethereum']
        self.client_id = os.environ.get('REDDIT_CLIENT_ID')
        self.client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
        self.user_agent = os.environ.get('REDDIT_USER_AGENT', 'SentientPump/1.0')
        
        if not all([self.client_id, self.client_secret]):
            logger.warning("Reddit API credentials not configured")
            self.reddit = None
        else:
            try:
                self.reddit = praw.Reddit(
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    user_agent=self.user_agent
                )
                logger.info("Reddit client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Reddit client: {e}")
                self.reddit = None
    
    def fetch_data(self) -> Dict[str, Any]:
        """Fetch hot posts from configured subreddits"""
        if not self.reddit:
            return self._get_fallback_data()
        
        reddit_data = {
            'source': 'reddit',
            'timestamp': datetime.utcnow().isoformat(),
            'posts': [],
            'subreddits_processed': 0,
            'total_posts': 0
        }
        
        try:
            for subreddit_name in self.subreddits:
                try:
                    subreddit = self.reddit.subreddit(subreddit_name)
                    
                    # Fetch hot posts (limit to 10 per subreddit)
                    for post in subreddit.hot(limit=10):
                        post_data = {
                            'id': post.id,
                            'title': post.title,
                            'score': post.score,
                            'upvote_ratio': post.upvote_ratio,
                            'num_comments': post.num_comments,
                            'created_utc': post.created_utc,
                            'sentiment': self._analyze_post_sentiment(post),
                            'subreddit': subreddit_name
                        }
                        reddit_data['posts'].append(post_data)
                        reddit_data['total_posts'] += 1
                    
                    reddit_data['subreddits_processed'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing subreddit {subreddit_name}: {e}")
                    continue
            
            logger.info(f"Collected {reddit_data['total_posts']} posts from {reddit_data['subreddits_processed']} subreddits")
            return reddit_data
            
        except Exception as e:
            logger.error(f"Critical Reddit fetch error: {e}")
            return self._get_fallback_data()
    
    def _analyze_post_sentiment(self, post) -> Dict[str, float]:
        """Analyze sentiment of post title and content"""
        try:
            # Analyze title
            title_blob = TextBlob(post.title)
            title_sentiment = title_blob.sentiment.polarity
            
            # Analyze selftext if available
            content_sentiment = 0.0
            if post.selftext:
                content_blob = TextBlob(post.selftext[:1000])  # Limit length
                content_sentiment = content_blob.sentiment.polarity
            
            return {
                'title': title_sentiment,
                'content': content_sentiment,
                'average': (title_sentiment + content_sentiment) / 2 if post.selftext else title_sentiment
            }
        except:
            return {'title': 0.0, 'content': 0.0, 'average': 0.0}
    
    def _get_fallback_data(self) -> Dict[str, Any]:
        """Return fallback data when Reddit API fails"""
        return {
            'source': 'reddit',
            'timestamp': datetime.utcnow().isoformat(),
            'posts': [],
            'subreddits_processed': 0,
            'total_posts': 0,
            'data_degraded': True,
            'degradation_reason': 'API unavailable or misconfigured'
        }
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """Validate Reddit data structure"""
        if not isinstance(data, dict):
            return False
        
        if data.get('data_degraded', False):
            logger.warning("Reddit data degraded - using fallback data")
            return True  # Still valid as fallback
        
        required_keys = ['source', 'timestamp', 'posts', 'total_posts']
        return all(key in data for key in required_keys)
    
    def get_source_name(self) -> str:
        return "reddit"

class TwitterSentimentSource(DataSource):
    """Fetches crypto sentiment from Twitter using Tweepy"""
    
    def __init__(self):