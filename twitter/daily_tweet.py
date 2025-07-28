import json
from datetime import datetime
import os
from pathlib import Path
import pytz
import tweepy
import sys

# --- File Locations ---
SCRIPT_DIR = Path(__file__).resolve().parent
TWEETS_FILE = SCRIPT_DIR / "tweets.json"
REPORT_FILE = SCRIPT_DIR / "reports.txt"

REQUIRED_ENV_VARS = [
    "TWITTER_BEARER_TOKEN",
    "TWITTER_API_KEY",
    "TWITTER_API_SECRET",
    "TWITTER_ACCESS_TOKEN",
    "TWITTER_ACCESS_SECRET"
]

def check_required_env():
    missing = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

def get_today_key():
    # Get date in Eastern Time as MM-DD
    eastern = pytz.timezone("US/Eastern")
    today = datetime.now(eastern)
    return today.strftime("%m-%d")

def already_logged_success():
    """Check if today's success has already been logged."""
    if not REPORT_FILE.exists():
        return False
    today = datetime.utcnow().strftime("%b %d")
    with open(REPORT_FILE, "r", encoding="utf-8") as f:
        return any(today in line and "tweet posted successfully" in line for line in f)

def log_report(success: bool, tweet_text: str = None):
    utc_now = datetime.utcnow().strftime("%b %d, %H:%M:%S UTC")
    status = "tweet posted successfully" if success else "tweet failed to post"
    with open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(f"{utc_now}, {status} | {tweet_text or ''}\n")

def main():
    try:
        check_required_env()
    except RuntimeError as e:
        print(f"Environment Error: {e}")
        sys.exit(1)

    # Load tweet data
    with open(TWEETS_FILE, "r", encoding="utf-8") as f:
        tweets = json.load(f)

    today_key = get_today_key()
    tweet_text = tweets.get(today_key)

    if not tweet_text:
        print(f"No tweet scheduled for {today_key}")
        log_report(success=False)
        sys.exit(0)

    if already_logged_success():
        print("Tweet for today already logged as success. Skipping duplicate post.")
        sys.exit(0)

    try:
        # Tweepy Client for Twitter API v2
        client = tweepy.Client(
            bearer_token=os.environ["TWITTER_BEARER_TOKEN"],
            consumer_key=os.environ["TWITTER_API_KEY"],
            consumer_secret=os.environ["TWITTER_API_SECRET"],
            access_token=os.environ["TWITTER_ACCESS_TOKEN"],
            access_token_secret=os.environ["TWITTER_ACCESS_SECRET"]
        )

        response = client.create_tweet(text=tweet_text)
        if response.data and "id" in response.data:
            print(f"Tweet posted for {today_key}: {tweet_text}")
            log_report(success=True, tweet_text=tweet_text)
        else:
            print(f"Failed to post tweet, API returned: {response}")
            log_report(success=False, tweet_text=tweet_text)
            sys.exit(1)

    except Exception as e:
        print(f"Failed to post tweet: {repr(e)}")
        log_report(success=False, tweet_text=tweet_text)
        sys.exit(1)

if __name__ == "__main__":
    main()
