import json
from datetime import datetime
import os
from pathlib import Path
import pytz
import tweepy

# --- File Locations ---
SCRIPT_DIR = Path(__file__).parent
TWEETS_FILE = SCRIPT_DIR / "tweets.json"
REPORT_FILE = SCRIPT_DIR / "reports.txt"

def get_today_key():
    # Get date in Eastern Time as MM-DD
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern)
    return today.strftime("%m-%d")

def log_report(success=True):
    # Get current UTC time
    utc_now = datetime.utcnow().strftime("%b %d, %H:%M:%S UTC")
    status = "tweet posted successfully" if success else "tweet failed to post"
    with open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(f"{utc_now}, {status}\n")

def main():
    # Load tweet data
    with open(TWEETS_FILE, "r", encoding="utf-8") as f:
        tweets = json.load(f)

    today_key = get_today_key()
    tweet_text = tweets.get(today_key)

    if not tweet_text:
        print(f"No tweet scheduled for {today_key}")
        log_report(success=False)
        return

    # Twitter API credentials from GitHub Secrets
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    api_key = os.environ["TWITTER_API_KEY"]
    api_secret = os.environ["TWITTER_API_SECRET"]
    access_token = os.environ["TWITTER_ACCESS_TOKEN"]
    access_secret = os.environ["TWITTER_ACCESS_SECRET"]

    try:
        # Tweepy Client for Twitter API v2
        client = tweepy.Client(
            bearer_token=bearer_token,
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_secret
        )

        response = client.create_tweet(text=tweet_text)
        if response.data and "id" in response.data:
            print(f"Tweet posted for {today_key}: {tweet_text}")
            log_report(success=True)
        else:
            print(f"Failed to post tweet, response: {response}")
            log_report(success=False)

    except Exception as e:
        print(f"Failed to post tweet: {e}")
        log_report(success=False)

if __name__ == "__main__":
    main()
