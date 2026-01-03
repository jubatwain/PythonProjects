import tweepy
import logging
from typing import Optional, Dict, Any
import time
from config.settings import (
    X_API_KEY, X_API_SECRET,
    X_ACCESS_TOKEN, X_ACCESS_SECRET,
    MAX_POST_LENGTH
)
from utils.logger import logger

class XPoster:
    def __init__(self):
        self.client = None
        self.api_v1 = None
        self.max_length = MAX_POST_LENGTH
        self.authenticated = False
        
    def authenticate(self) -> bool:
        """Authenticate with X API"""
        try:
            # Check if credentials are provided
            if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET]):
                logger.warning("X API credentials not configured")
                logger.info("Add credentials to .env file or config/settings.py")
                return False
            
            # Twitter API v2 client
            self.client = tweepy.Client(
                consumer_key=X_API_KEY,
                consumer_secret=X_API_SECRET,
                access_token=X_ACCESS_TOKEN,
                access_token_secret=X_ACCESS_SECRET,
                wait_on_rate_limit=True
            )
            
            # API v1.1 for media upload
            auth = tweepy.OAuth1UserHandler(
                X_API_KEY, X_API_SECRET,
                X_ACCESS_TOKEN, X_ACCESS_SECRET
            )
            self.api_v1 = tweepy.API(auth, wait_on_rate_limit=True)
            
            # Verify credentials
            user = self.api_v1.verify_credentials()
            logger.info(f"✓ Authenticated with X as @{user.screen_name}")
            self.authenticated = True
            return True
            
        except tweepy.TweepyException as e:
            logger.error(f"X authentication failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during X authentication: {e}")
            return False
    
    def post_tweet(self, text: str, media_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Post a tweet to X"""
        if not self.authenticated and not self.authenticate():
            logger.error("Cannot post: Not authenticated")
            return None
        
        try:
            # Validate text length
            if len(text) > self.max_length:
                logger.warning(f"Text too long ({len(text)} chars), truncating...")
                text = text[:self.max_length-3] + "..."
            
            logger.info(f"Posting tweet ({len(text)} chars)...")
            logger.debug(f"Tweet content: {text}")
            
            # Post with media if provided
            if media_path:
                try:
                    # Upload media
                    media = self.api_v1.media_upload(media_path)
                    
                    # Post tweet with media
                    response = self.client.create_tweet(
                        text=text,
                        media_ids=[media.media_id]
                    )
                    logger.info("✓ Tweet posted with media")
                except Exception as media_error:
                    logger.error(f"Media upload failed: {media_error}")
                    # Fallback to text-only
                    response = self.client.create_tweet(text=text)
            else:
                # Text-only tweet
                response = self.client.create_tweet(text=text)
            
            if response.data:
                tweet_id = response.data['id']
                logger.info(f"✓ Tweet posted successfully! ID: {tweet_id}")
                
                # Get tweet URL
                user = self.api_v1.verify_credentials()
                tweet_url = f"https://twitter.com/{user.screen_name}/status/{tweet_id}"
                logger.info(f"Tweet URL: {tweet_url}")
                
                return {
                    "success": True,
                    "tweet_id": tweet_id,
                    "tweet_url": tweet_url,
                    "text": text,
                    "timestamp": time.time()
                }
            else:
                logger.error("No response data from X API")
                return None
                
        except tweepy.TweepyException as e:
            logger.error(f"X API error: {e}")
            
            # Rate limit handling
            if "429" in str(e):
                logger.warning("Rate limit exceeded. Waiting 15 minutes...")
                time.sleep(900)  # Wait 15 minutes
            
            return {
                "success": False,
                "error": str(e),
                "text": text
            }
        except Exception as e:
            logger.error(f"Unexpected error posting tweet: {e}")
            return None
    
    def reply_to_tweet(self, text: str, tweet_id: str) -> Optional[Dict[str, Any]]:
        """Reply to an existing tweet"""
        if not self.authenticated and not self.authenticate():
            logger.error("Cannot reply: Not authenticated")
            return None
        
        try:
            response = self.client.create_tweet(
                text=text[:self.max_length],
                in_reply_to_tweet_id=tweet_id
            )
            
            if response.data:
                logger.info(f"✓ Reply posted to tweet {tweet_id}")
                return {
                    "success": True,
                    "tweet_id": response.data['id'],
                    "reply_to": tweet_id
                }
            return None
            
        except tweepy.TweepyException as e:
            logger.error(f"Error posting reply: {e}")
            return None
    
    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get X API rate limit status"""
        try:
            status = self.api_v1.rate_limit_status()
            return {
                "remaining": status['resources']['statuses']['/statuses/update']['remaining'],
                "reset": status['resources']['statuses']['/statuses/update']['reset'],
                "limit": status['resources']['statuses']['/statuses/update']['limit']
            }
        except Exception as e:
            logger.error(f"Error getting rate limits: {e}")
            return {}