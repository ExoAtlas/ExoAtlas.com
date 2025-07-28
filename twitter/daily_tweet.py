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
    # Append to report file
    with open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(f"{utc_now.replace(',', '')}, {status}\n")

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
    api_key = os.environ["TWITTER_API_KEY"]
    api_secret = os.environ["TWITTER_API_SECRET"]
    access_token = os.environ["TWITTER_ACCESS_TOKEN"]
    access_secret = os.environ["TWITTER_ACCESS_SECRET"]

    # Authenticate
    auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_secret)
    api = tweepy.API(auth)

    try:
        api.update_status(status=tweet_text)
        print(f"Tweet posted for {today_key}: {tweet_text}")
        log_report(success=True)
    except Exception as e:
        print(f"Failed to post tweet: {e}")
        log_report(success=False)

if __name__ == "__main__":
    main()
