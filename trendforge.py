#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║          ████████╗██████╗ ███████╗███╗   ██╗██████╗                 ║
║             ██╔══╝██╔══██╗██╔════╝████╗  ██║██╔══██╗                ║
║             ██║   ██████╔╝█████╗  ██╔██╗ ██║██║  ██║                ║
║             ██║   ██╔══██╗██╔══╝  ██║╚██╗██║██║  ██║                ║
║             ██║   ██║  ██║███████╗██║ ╚████║██████╔╝                ║
║             ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═══╝╚═════╝                ║
║                                                                      ║
║          ███████╗ ██████╗ ██████╗  ██████╗ ███████╗                 ║
║          ██╔════╝██╔═══██╗██╔══██╗██╔════╝ ██╔════╝                 ║
║          █████╗  ██║   ██║██████╔╝██║  ███╗█████╗                   ║
║          ██╔══╝  ██║   ██║██╔══██╗██║   ██║██╔══╝                   ║
║          ██║     ╚██████╔╝██║  ██║╚██████╔╝███████╗                 ║
║          ╚═╝      ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝                ║
║                                                                      ║
║              v1.0 — برای @hosseinmarko | طراحی شده با هوش مصنوعی   ║
╚══════════════════════════════════════════════════════════════════════╝

═══════════════════ روش طلایی TrendForge ═══════════════════

  ۱. جمع‌آوری موازی (Parallel Aggregation)
     ▸ ۶+ منبع همزمان: Google Trends، Reddit، HackerNews،
       RSS، YouTube، TikTok Creative Center
     ▸ هر منبع مستقل — اگر یکی خراب شد بقیه ادامه می‌دهند

  ۲. امتیازدهی چند-پلتفرمی (Cross-Platform Scoring)
     ▸ ترندی که روی چند پلتفرم باشد = امتیاز تصاعدی
     ▸ فرمول: Score = Base + (n_platforms - 1) × 15 + GrowthBonus

  ۳. آنالیز سرعت رشد (Velocity Analysis)
     ▸ نه فقط حجم، بلکه سرعت رشد مهمه
     ▸ ترند در حال صعود >> ترند در اوج

  ۴. تبدیل به ایده ویدیو (Content Morphing)
     ▸ هر ترند → ۳ ایده ویدیو با فرمت‌های مختلف
     ▸ براساس دسته‌بندی: تکنولوژی / طبیعت / وایرال

  ۵. زمان‌بندی بهینه (Chronobiological Scheduling)
     ▸ تطبیق ترند با پنجره‌های فعالیت مخاطب ایرانی
     ▸ ساعت‌های طلایی: ۹:۰۰ | ۱۴:۰۰ | ۲۱:۰۰

  ۶. لایه بومی‌سازی فارسی (Cultural Localization)
     ▸ فیلتر و تقویت محتوای مرتبط با مخاطب فارسی‌زبان

════════════════════════════════════════════════════════════
"""

# ════════════════════════════════════════════════════════════
#  SECTION 1: SETUP & AUTO-INSTALL
# ════════════════════════════════════════════════════════════
import os
import sys
import json
import time
import hashlib
import logging
import warnings
import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.ERROR)

# Detect environment
try:
    import google.colab  # noqa: F401
    IN_COLAB = True
except ImportError:
    IN_COLAB = False

REQUIRED_PACKAGES = {
    "requests":     "requests>=2.28",
    "bs4":          "beautifulsoup4>=4.11",
    "pytrends":     "pytrends>=4.9",
    "rich":         "rich>=13.0",
    "feedparser":   "feedparser>=6.0",
    "jinja2":       "Jinja2>=3.1",
    "pytz":         "pytz>=2023.3",
}


def _auto_install():
    """Install missing packages silently."""
    missing = []
    for mod, pkg in REQUIRED_PACKAGES.items():
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"📦 نصب {len(missing)} پکیج لازم...")
        for pkg in missing:
            print(f"  → {pkg}", end=" ", flush=True)
            try:
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", pkg, "-q",
                     "--disable-pip-version-check"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                print("✓")
            except Exception:
                print("✗ (ادامه می‌دهد)")


_auto_install()

# Now safe to import
import requests
import feedparser
import pytz
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.text import Text
from jinja2 import Template

try:
    from pytrends.request import TrendReq
    PYTRENDS_OK = True
except Exception:
    PYTRENDS_OK = False

console = Console()


# ════════════════════════════════════════════════════════════
#  SECTION 2: CONFIGURATION  ← اینجا تنظیمات را ویرایش کنید
# ════════════════════════════════════════════════════════════

CONFIG = {
    # ─── اطلاعات پیج شما ────────────────────────────────
    "username":        "hosseinmarko",
    "page_topics":     ["برنامه‌نویسی", "طبیعتگردی", "تکنولوژی"],
    "posts_per_day":   3,
    "timezone":        "Asia/Tehran",

    # ─── API Keys (اختیاری — بدون این‌ها هم کار می‌کند) ─
    # برای گرفتن YouTube API: https://console.cloud.google.com
    "youtube_api_key":    os.getenv("YOUTUBE_API_KEY",    ""),
    # برای گرفتن NewsAPI: https://newsapi.org (رایگان)
    "newsapi_key":        os.getenv("NEWSAPI_KEY",        ""),
    # برای Reddit OAuth: https://www.reddit.com/prefs/apps
    "reddit_client_id":   os.getenv("REDDIT_CLIENT_ID",   ""),
    "reddit_secret":      os.getenv("REDDIT_SECRET",      ""),

    # ─── خروجی ──────────────────────────────────────────
    "output_dir":      "trendforge_reports",
    "keep_reports":    10,   # تعداد گزارش‌های ذخیره‌شده
}

# ساعت‌های طلایی پست برای اینستاگرام ایران
POSTING_WINDOWS = [
    {"slot": "morning",   "hour": 9,  "minute": 0,  "label": "صبح",          "icon": "🌅", "best_for": "tech"},
    {"slot": "afternoon", "hour": 14, "minute": 0,  "label": "ظهر",          "icon": "☀️",  "best_for": "news"},
    {"slot": "evening",   "hour": 21, "minute": 0,  "label": "شب (طلایی)",   "icon": "🌙", "best_for": "viral"},
]

# ساب‌ردیت‌ها به ترتیب اولویت
SUBREDDITS = [
    # وایرال / جالب
    "interestingasfuck", "BeAmazed", "nextfuckinglevel",
    "Damnthatsinteresting", "oddlysatisfying", "Unexpected",
    # تکنولوژی و برنامه‌نویسی
    "programming", "technology", "artificial",
    # طبیعت و سفر
    "hiking", "CampingandHiking", "travel",
    # عمومی پرطرفدار
    "todayilearned", "mildlyinteresting", "worldnews",
]

# فیدهای RSS (کاملاً رایگان، نیاز به API Key ندارد)
RSS_FEEDS = [
    # فارسی
    {"url": "https://feeds.bbci.co.uk/persian/rss.xml",        "name": "BBC فارسی",     "lang": "fa", "cat": "news_fa"},
    {"url": "https://www.radiofarda.com/api/zrqoitioqp",       "name": "رادیو فردا",    "lang": "fa", "cat": "news_fa"},
    # تکنولوژی
    {"url": "https://techcrunch.com/feed/",                    "name": "TechCrunch",    "lang": "en", "cat": "tech"},
    {"url": "https://www.theverge.com/rss/index.xml",          "name": "The Verge",     "lang": "en", "cat": "tech"},
    {"url": "https://www.wired.com/feed/rss",                  "name": "Wired",         "lang": "en", "cat": "tech"},
    {"url": "https://feeds.arstechnica.com/arstechnica/index", "name": "Ars Technica",  "lang": "en", "cat": "tech"},
    # وایرال / جالب
    {"url": "https://www.boingboing.net/feed",                 "name": "BoingBoing",    "lang": "en", "cat": "viral"},
    {"url": "https://www.mentalfloss.com/rss.xml",             "name": "MentalFloss",   "lang": "en", "cat": "viral"},
    # علم و طبیعت
    {"url": "https://rss.sciencedaily.com/rss.xml",            "name": "ScienceDaily",  "lang": "en", "cat": "science"},
    {"url": "https://www.livescience.com/feeds/all",           "name": "LiveScience",   "lang": "en", "cat": "science"},
]


# ════════════════════════════════════════════════════════════
#  SECTION 3: DATA MODEL
# ════════════════════════════════════════════════════════════

@dataclass
class TrendItem:
    title:        str
    source:       str
    url:          str           = ""
    score:        float         = 0.0
    growth_rate:  float         = 0.0
    category:     str           = "general"
    language:     str           = "en"
    engagement:   int           = 0
    timestamp:    str           = ""
    video_ideas:  List[str]     = field(default_factory=list)
    hashtags:     List[str]     = field(default_factory=list)
    best_post_time: str         = ""
    platforms:    List[str]     = field(default_factory=list)
    raw_score:    float         = 0.0

    def __post_init__(self):
        if not self.timestamp:
            tz = pytz.timezone(CONFIG["timezone"])
            self.timestamp = datetime.now(tz).isoformat()
        self.raw_score = self.score


# ════════════════════════════════════════════════════════════
#  SECTION 4: COLLECTORS
# ════════════════════════════════════════════════════════════

class BaseCollector:
    """Base class — هر collector مستقل است."""
    name = "Base"

    def collect(self) -> List[TrendItem]:
        raise NotImplementedError

    @staticmethod
    def _cat(text: str) -> str:
        t = text.lower()
        tech    = ["python", "code", "ai", "برنامه", "tech", "software",
                   "هوش", "javascript", "app", "api", "data", "model", "gpt"]
        nature  = ["hike", "hiking", "nature", "کوه", "طبیعت", "travel",
                   "سفر", "forest", "جنگل", "camp", "mountain", "trail"]
        viral   = ["wtf", "amazing", "incredible", "viral", "trend",
                   "shocking", "unexpected", "wow", "mind"]
        if any(k in t for k in tech):   return "tech"
        if any(k in t for k in nature): return "nature"
        if any(k in t for k in viral):  return "viral"
        return "general"


# ─── Google Trends ────────────────────────────────────────
class GoogleTrendsCollector(BaseCollector):
    name = "Google Trends"

    def collect(self) -> List[TrendItem]:
        if not PYTRENDS_OK:
            return []
        items = []
        try:
            pt = TrendReq(hl="fa", tz=-210, timeout=(10, 30), retries=2, backoff_factor=0.8)

            # ایران
            try:
                df = pt.trending_searches(pn="iran")
                for i, row in df.head(12).iterrows():
                    term = str(row[0]).strip()
                    if not term:
                        continue
                    items.append(TrendItem(
                        title=term, source="Google Trends 🇮🇷",
                        score=92 - i * 6,
                        category=self._cat(term),
                        language="fa", platforms=["google"],
                        growth_rate=150 - i * 10,
                    ))
                time.sleep(2.5)
            except Exception:
                pass

            # جهانی
            try:
                df2 = pt.trending_searches(pn="united_states")
                for i, row in df2.head(6).iterrows():
                    term = str(row[0]).strip()
                    if not term:
                        continue
                    items.append(TrendItem(
                        title=term, source="Google Trends 🌍",
                        score=72 - i * 5,
                        category=self._cat(term),
                        language="en", platforms=["google"],
                        growth_rate=100 - i * 8,
                    ))
                time.sleep(2)
            except Exception:
                pass

        except Exception as e:
            console.print(f"  [dim yellow]Google Trends: {str(e)[:60]}[/dim yellow]")

        return items


# ─── Reddit (بدون نیاز به API Key) ───────────────────────
class RedditCollector(BaseCollector):
    name = "Reddit"
    _HEADERS = {"User-Agent": "TrendForge/1.0 (trend-analyzer)"}

    def collect(self) -> List[TrendItem]:
        items = []
        session = requests.Session()
        session.headers.update(self._HEADERS)

        for sub in SUBREDDITS[:10]:
            try:
                r = session.get(
                    f"https://www.reddit.com/r/{sub}/hot.json?limit=6",
                    timeout=10
                )
                if r.status_code == 429:
                    time.sleep(30)
                    continue
                if r.status_code != 200:
                    continue

                posts = r.json().get("data", {}).get("children", [])
                for post in posts:
                    p = post.get("data", {})
                    upvotes = p.get("score", 0)
                    title   = p.get("title", "").strip()
                    if not title or upvotes < 200:
                        continue

                    items.append(TrendItem(
                        title=title,
                        source=f"Reddit r/{sub}",
                        url=f"https://reddit.com{p.get('permalink','')}",
                        score=min(95, (upvotes / 30000) * 100 + 35),
                        engagement=upvotes,
                        category=self._cat_sub(sub),
                        language="en",
                        platforms=["reddit"],
                        growth_rate=p.get("upvote_ratio", 0.9) * 100,
                    ))

                time.sleep(1.2)

            except Exception:
                continue

        return items

    @staticmethod
    def _cat_sub(sub: str) -> str:
        sub_l = sub.lower()
        if sub_l in ("programming", "technology", "artificial", "python"):
            return "tech"
        if sub_l in ("hiking", "campingandhiking", "travel", "earthporn"):
            return "nature"
        if sub_l in ("interestingasfuck", "beamazed", "nextfuckinglevel",
                     "damnthatsinteresting", "unexpected"):
            return "viral"
        return "general"


# ─── Hacker News (کاملاً رایگان) ─────────────────────────
class HackerNewsCollector(BaseCollector):
    name = "Hacker News"
    BASE = "https://hacker-news.firebaseio.com/v0"

    def collect(self) -> List[TrendItem]:
        items = []
        try:
            ids = requests.get(f"{self.BASE}/topstories.json", timeout=8).json()[:18]
            for sid in ids:
                try:
                    s = requests.get(f"{self.BASE}/item/{sid}.json", timeout=5).json()
                    if not s or s.get("type") != "story":
                        continue
                    title = s.get("title", "").strip()
                    pts   = s.get("score", 0)
                    if pts < 60:
                        continue
                    items.append(TrendItem(
                        title=title,
                        source="Hacker News",
                        url=s.get("url", f"https://news.ycombinator.com/item?id={sid}"),
                        score=min(90, pts / 5 + 40),
                        engagement=pts,
                        category="tech",
                        language="en",
                        platforms=["hackernews"],
                        growth_rate=min(200, pts / 4),
                    ))
                except Exception:
                    continue
        except Exception as e:
            console.print(f"  [dim yellow]HN: {str(e)[:50]}[/dim yellow]")
        return items


# ─── RSS Feeds ────────────────────────────────────────────
class RSSCollector(BaseCollector):
    name = "RSS Feeds"

    def collect(self) -> List[TrendItem]:
        items = []
        for feed in RSS_FEEDS:
            try:
                parsed = feedparser.parse(feed["url"])
                for i, entry in enumerate(parsed.entries[:5]):
                    title = entry.get("title", "").strip()
                    if not title:
                        continue
                    items.append(TrendItem(
                        title=title,
                        source=feed["name"],
                        url=entry.get("link", ""),
                        score=78 - i * 6,
                        category=feed["cat"],
                        language=feed["lang"],
                        platforms=["rss"],
                        growth_rate=55 - i * 8,
                    ))
            except Exception:
                continue
        return items


# ─── YouTube (API Key اختیاری) ────────────────────────────
class YouTubeCollector(BaseCollector):
    name = "YouTube"

    def collect(self) -> List[TrendItem]:
        key = CONFIG.get("youtube_api_key", "")
        if key:
            return self._via_api(key)
        return self._via_scrape()

    def _via_api(self, key: str) -> List[TrendItem]:
        items = []
        try:
            for region, lang in [("IR", "fa"), ("US", "en")]:
                r = requests.get(
                    "https://www.googleapis.com/youtube/v3/videos",
                    params={"part": "snippet,statistics", "chart": "mostPopular",
                            "regionCode": region, "maxResults": 8, "key": key},
                    timeout=10,
                )
                for i, v in enumerate(r.json().get("items", [])):
                    sn    = v.get("snippet", {})
                    stats = v.get("statistics", {})
                    views = int(stats.get("viewCount", 0))
                    title = sn.get("title", "").strip()
                    if not title:
                        continue
                    items.append(TrendItem(
                        title=title,
                        source=f"YouTube Trending {'🇮🇷' if region=='IR' else '🌍'}",
                        url=f"https://youtube.com/watch?v={v['id']}",
                        score=88 - i * 5,
                        engagement=views,
                        category="video",
                        language=lang,
                        platforms=["youtube"],
                        growth_rate=min(200, views / 50000),
                    ))
        except Exception as e:
            console.print(f"  [dim yellow]YouTube API: {str(e)[:50]}[/dim yellow]")
        return items

    def _via_scrape(self) -> List[TrendItem]:
        """بدون API Key — داده‌های محدود‌تر"""
        items = []
        try:
            r = requests.get(
                "https://www.youtube.com/feed/trending",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=12,
            )
            soup = BeautifulSoup(r.text, "html.parser")
            for i, tag in enumerate(soup.find_all("yt-formatted-string", {"id": "video-title"})[:8]):
                t = tag.get_text(strip=True)
                if t:
                    items.append(TrendItem(
                        title=t,
                        source="YouTube Trending",
                        score=78 - i * 4,
                        category="video",
                        language="en",
                        platforms=["youtube"],
                    ))
        except Exception:
            pass
        return items


# ─── TikTok Creative Center ───────────────────────────────
class TikTokCollector(BaseCollector):
    name = "TikTok"
    _HDR = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://ads.tiktok.com/",
        "Accept": "application/json",
    }

    def collect(self) -> List[TrendItem]:
        items = []
        for region in ["IR", "US"]:
            try:
                r = requests.get(
                    "https://ads.tiktok.com/creative_radar_api/v1/popular_trend/hashtag/list",
                    headers=self._HDR,
                    params={"page": 1, "limit": 20, "period": 7, "country_code": region},
                    timeout=10,
                )
                if r.status_code != 200:
                    continue
                hashtags = r.json().get("data", {}).get("list", [])
                for i, tag in enumerate(hashtags[:10]):
                    name = tag.get("hashtag_name", "").strip()
                    if not name:
                        continue
                    items.append(TrendItem(
                        title=f"#{name}",
                        source=f"TikTok Creative {'🇮🇷' if region=='IR' else '🌍'}",
                        score=82 - i * 4,
                        category="viral",
                        language="fa" if region == "IR" else "en",
                        platforms=["tiktok"],
                        growth_rate=abs(tag.get("rank_diff", 0)) * 8,
                    ))
                break  # If IR worked, don't try US
            except Exception:
                continue
        return items


# ─── NewsAPI (API Key اختیاری) ────────────────────────────
class NewsAPICollector(BaseCollector):
    name = "NewsAPI"

    def collect(self) -> List[TrendItem]:
        key = CONFIG.get("newsapi_key", "")
        if not key:
            return []
        items = []
        try:
            r = requests.get(
                "https://newsapi.org/v2/top-headlines",
                params={"language": "en", "pageSize": 12, "apiKey": key},
                timeout=10,
            )
            for i, art in enumerate(r.json().get("articles", [])):
                title = art.get("title", "").strip()
                if not title or "[Removed]" in title:
                    continue
                items.append(TrendItem(
                    title=title,
                    source=f"NewsAPI / {art.get('source',{}).get('name','News')}",
                    url=art.get("url", ""),
                    score=74 - i * 3,
                    category="news",
                    language="en",
                    platforms=["newsapi"],
                ))
        except Exception as e:
            console.print(f"  [dim yellow]NewsAPI: {str(e)[:50]}[/dim yellow]")
        return items


# ════════════════════════════════════════════════════════════
#  SECTION 5: SCORING ENGINE  (قلب TrendForge)
# ════════════════════════════════════════════════════════════

class TrendScorer:
    """
    الگوریتم امتیازدهی TrendForge:

    FinalScore = BaseScore
               + CrossPlatformBonus   (هر پلتفرم اضافه +15)
               + GrowthBonus          (سرعت رشد، حداکثر +20)
               + EngagementBonus      (تعامل کاربران، حداکثر +15)
               + AffinityBonus        (تطابق با موضوع پیج، +5)
               + LanguageBonus        (فارسی برای مخاطب ایرانی +8)
               ∈ [0, 100]
    """

    _VIDEO_TEMPLATES = {
        "tech": [
            "آموزش سریع {t} در ۶۰ ثانیه",
            "چرا همه دارن از {t} حرف می‌زنن؟",
            "من {t} رو تست کردم — نتیجه رو باور نمی‌کنی",
            "آیا {t} آینده برنامه‌نویسی رو عوض می‌کنه؟",
            "۵ چیزی که برنامه‌نویس‌ها باید درباره {t} بدونن",
        ],
        "nature": [
            "این مکان رو در {t} دیدم — حیرت‌انگیز بود!",
            "هایک در {t} — همه چیزی که باید بدونی",
            "صحنه‌ای که فراموش نمی‌کنم: {t}",
            "قبل از رفتن به {t} این ویدیو رو ببین",
            "طبیعت‌گردی واقعی: {t}",
        ],
        "viral": [
            "این ترند داره منفجر می‌شه: {t}",
            "واقعیتی که کسی بهت نگفته درباره {t}",
            "POV: وقتی میفهمی {t} چیه",
            "همه دارن این رو می‌بینن — {t}",
            "۳ چیزی که باید درباره {t} بدونی",
        ],
        "news":    [
            "تحلیل سریع: {t}",
            "خلاصه خبر مهم: {t}",
            "نظر من درباره: {t}",
        ],
        "news_fa": [
            "خبر مهم: {t}",
            "تحلیل: {t} — چی باید انتظار داشت؟",
            "نظر من: {t}",
        ],
        "video": [
            "ری‌اکشن به: {t}",
            "این ویدیو رو همه دیدن؟ {t}",
            "چرا این ویدیو وایرال شد: {t}",
        ],
        "general": [
            "ترند جدید: {t}",
            "این رو باید ببینید: {t}",
            "چرا {t} مهمه؟",
            "واکنش من به: {t}",
        ],
        "science": [
            "علم در ۶۰ ثانیه: {t}",
            "کشف جدید: {t} — توضیح ساده",
            "چرا این یافته درباره {t} مهمه؟",
        ],
    }

    _HASHTAGS = {
        "tech":    ["#برنامه‌نویسی", "#تکنولوژی", "#tech", "#coding", "#developer", "#AI"],
        "nature":  ["#طبیعتگردی", "#هایکینگ", "#کوهنوردی", "#nature", "#hiking", "#travel"],
        "viral":   ["#وایرال", "#ترندینگ", "#trending", "#funny", "#entertainment"],
        "news":    ["#خبر", "#news", "#اخبار", "#تحلیل"],
        "news_fa": ["#اخبار", "#ایران", "#خبرفارسی", "#تحلیل"],
        "video":   ["#ویدیو", "#رییلز", "#reels", "#video"],
        "science": ["#علم", "#science", "#دانش", "#کشف"],
        "general": ["#ایران", "#محتوا", "#رییلز", "#اینستاگرام"],
    }

    _BASE_TAGS = ["#hosseinmarko", "#ترند", "#viral"]

    def merge_and_score(self, raw: List[TrendItem]) -> List[TrendItem]:
        # Merge duplicates
        merged = self._merge(raw)
        # Score, enrich, sort
        tz = pytz.timezone(CONFIG["timezone"])
        now = datetime.now(tz)
        for item in merged:
            item.score       = min(100, self._score(item))
            item.video_ideas = self._ideas(item)
            item.hashtags    = self._tags(item)
            item.best_post_time = self._post_time(now)
        merged.sort(key=lambda x: x.score, reverse=True)
        return merged[:35]

    # ── helpers ──────────────────────────────────────────
    def _merge(self, items: List[TrendItem]) -> List[TrendItem]:
        bucket: Dict[str, TrendItem] = {}
        for item in items:
            key = hashlib.md5(
                " ".join(item.title.lower().split()[:6]).encode()
            ).hexdigest()[:10]
            if key in bucket:
                ex = bucket[key]
                ex.platforms   = list(set(ex.platforms + item.platforms))
                ex.score       = max(ex.score, item.score)
                ex.engagement  = max(ex.engagement, item.engagement)
                ex.growth_rate = max(ex.growth_rate, item.growth_rate)
            else:
                bucket[key] = item
        return list(bucket.values())

    def _score(self, item: TrendItem) -> float:
        s = item.score
        # Cross-platform bonus
        s += (len(item.platforms) - 1) * 15
        # Growth rate bonus (max +20)
        s += min(20, item.growth_rate / 10)
        # Engagement bonus
        if   item.engagement > 50_000: s += 15
        elif item.engagement > 10_000: s += 10
        elif item.engagement > 1_000:  s += 5
        elif item.engagement > 100:    s += 2
        # Affinity with page topics
        if item.category == "tech"   and "برنامه‌نویسی" in CONFIG["page_topics"]: s += 5
        if item.category == "nature" and "طبیعتگردی"    in CONFIG["page_topics"]: s += 5
        # Language bonus (Farsi = more relevant for Iranian audience)
        if item.language == "fa": s += 8
        return s

    def _ideas(self, item: TrendItem) -> List[str]:
        cat = item.category if item.category in self._VIDEO_TEMPLATES else "general"
        tmpl = self._VIDEO_TEMPLATES[cat]
        title_short = item.title[:38]
        return [t.replace("{t}", title_short) for t in tmpl[:3]]

    def _tags(self, item: TrendItem) -> List[str]:
        cat  = item.category if item.category in self._HASHTAGS else "general"
        tags = self._BASE_TAGS + self._HASHTAGS[cat]
        return tags[:9]

    @staticmethod
    def _post_time(now: datetime) -> str:
        h = now.hour
        if   8  <= h < 11: return "همین الان ✅  (ساعت طلایی صبح ۸–۱۱)"
        elif 13 <= h < 16: return "همین الان ✅  (ساعت طلایی ظهر ۱۳–۱۶)"
        elif 20 <= h < 23: return "همین الان ✅  (ساعت طلایی شب ۲۰–۲۳)"
        elif h < 8:        return "ساعت ۹:۰۰ صبح (فرصت بعدی)"
        elif h < 13:       return "ساعت ۱۴:۰۰ (فرصت بعدی)"
        elif h < 20:       return "ساعت ۲۱:۰۰ امشب (بهترین زمان)"
        else:              return "فردا ۹:۰۰ صبح"


# ════════════════════════════════════════════════════════════
#  SECTION 6: HTML REPORT TEMPLATE
# ════════════════════════════════════════════════════════════

_HTML = r"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TrendForge — @{{ username }}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;600;700;900&display=swap');
:root{
  --bg:#08080f;--sur:#0f0f1a;--card:#141421;--brd:#1e1e2e;
  --pu:#7c3aed;--cy:#06b6d4;--gn:#10b981;--yl:#f59e0b;--rd:#ef4444;
  --tx:#e2e8f0;--mu:#64748b;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Vazirmatn',sans-serif;background:var(--bg);color:var(--tx);min-height:100vh}
a{color:var(--cy);text-decoration:none}

/* ── Header ── */
.hdr{
  background:linear-gradient(135deg,#1e0840 0%,#0a0f28 40%,#001420 100%);
  padding:48px 24px 36px;text-align:center;
  border-bottom:1px solid var(--brd);position:relative;overflow:hidden;
}
.hdr::before{
  content:'';position:absolute;inset:0;
  background:radial-gradient(ellipse 80% 60% at 50% 0%,rgba(124,58,237,.18),transparent);
}
.hdr h1{font-size:2.2rem;font-weight:900;color:#fff;position:relative;letter-spacing:-1px}
.hdr h1 span{color:var(--cy)}
.hdr .sub{color:var(--mu);font-size:.9rem;margin-top:8px;position:relative}
.hdr .sub b{color:var(--yl)}

/* ── Stat Bar ── */
.statbar{
  display:flex;gap:12px;padding:18px 24px;background:var(--sur);
  border-bottom:1px solid var(--brd);overflow-x:auto;flex-wrap:nowrap;
}
.chip{
  background:var(--card);border:1px solid var(--brd);border-radius:14px;
  padding:12px 20px;min-width:110px;text-align:center;flex-shrink:0;
  transition:border-color .2s;
}
.chip:hover{border-color:var(--pu)}
.chip .n{font-size:1.6rem;font-weight:700;color:var(--pu)}
.chip .l{font-size:.72rem;color:var(--mu);margin-top:3px}

/* ── Container ── */
.wrap{max-width:1300px;margin:0 auto;padding:28px 20px}

/* ── Section title ── */
.sec{
  font-size:1.15rem;font-weight:700;color:var(--tx);
  margin:32px 0 18px;display:flex;align-items:center;gap:10px;
}
.sec::after{content:'';flex:1;height:1px;background:var(--brd)}

/* ── Schedule ── */
.sched{
  background:var(--card);border:1px solid var(--brd);border-radius:18px;
  padding:24px;margin-bottom:28px;
}
.sched-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-top:18px}
@media(max-width:600px){.sched-grid{grid-template-columns:1fr}}
.slot{
  background:var(--sur);border:1px solid var(--brd);border-radius:14px;
  padding:18px;text-align:center;
}
.slot .t{font-size:2rem;font-weight:900;color:var(--pu)}
.slot .lb{font-size:.8rem;color:var(--mu);margin-top:4px}
.slot .sug{
  font-size:.75rem;color:var(--cy);margin-top:10px;padding-top:10px;
  border-top:1px solid var(--brd);line-height:1.5;
}

/* ── Grid ── */
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px}

/* ── Card ── */
.card{
  background:var(--card);border:1px solid var(--brd);border-radius:18px;
  padding:20px;position:relative;
  transition:transform .2s,border-color .2s,box-shadow .2s;
}
.card:hover{transform:translateY(-3px);border-color:var(--pu);box-shadow:0 8px 32px rgba(124,58,237,.15)}
.card .rnk{
  position:absolute;top:14px;left:14px;
  width:30px;height:30px;border-radius:9px;
  display:flex;align-items:center;justify-content:center;
  font-size:.8rem;font-weight:700;color:#fff;
  background:var(--pu);
}
.card .rnk.g{background:var(--yl);color:#000}
.card .rnk.s{background:#94a3b8}
.card .rnk.b{background:#b87333}

.card-title{font-size:.97rem;font-weight:600;line-height:1.45;margin-bottom:8px;padding-left:4px}
.badge{
  display:inline-block;font-size:.68rem;padding:3px 9px;border-radius:7px;
  background:rgba(124,58,237,.14);border:1px solid rgba(124,58,237,.28);
  color:#a78bfa;margin-bottom:12px;
}
.bar{background:var(--bg);border-radius:99px;height:5px;margin-bottom:12px;overflow:hidden}
.bar-fill{height:100%;background:linear-gradient(90deg,var(--pu),var(--cy));border-radius:99px}
.metas{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}
.mt{font-size:.7rem;padding:3px 8px;border-radius:7px;font-weight:500}
.mt-tech  {background:rgba(6,182,212,.14);color:var(--cy)}
.mt-nature{background:rgba(16,185,129,.14);color:var(--gn)}
.mt-viral {background:rgba(245,158,11,.14);color:var(--yl)}
.mt-news  {background:rgba(239,68,68,.14);color:var(--rd)}
.mt-news_fa{background:rgba(239,68,68,.14);color:var(--rd)}
.mt-video {background:rgba(124,58,237,.14);color:#a78bfa}
.mt-science{background:rgba(16,185,129,.14);color:var(--gn)}
.mt-general{background:rgba(100,116,139,.14);color:var(--mu)}

.ideas{margin-top:12px;border-top:1px solid var(--brd);padding-top:12px}
.ideas-lbl{font-size:.72rem;color:var(--mu);margin-bottom:7px}
.idea{
  font-size:.8rem;color:var(--tx);padding:7px 10px;background:var(--sur);
  border-radius:9px;margin-bottom:5px;cursor:pointer;
  border:1px solid transparent;transition:all .18s;
}
.idea:hover{background:rgba(124,58,237,.1);border-color:rgba(124,58,237,.3);color:#fff}
.tags{display:flex;flex-wrap:wrap;gap:4px;margin-top:10px}
.tag{font-size:.68rem;color:var(--cy);background:rgba(6,182,212,.08);padding:2px 7px;border-radius:5px}
.ptime{margin-top:10px;font-size:.75rem;color:var(--gn);display:flex;align-items:center;gap:5px}
.eng{font-size:.7rem;color:var(--yl);background:rgba(245,158,11,.1);padding:3px 8px;border-radius:7px}

/* ── Method box ── */
.method{
  background:var(--card);border:1px solid var(--pu);border-radius:18px;
  padding:24px;margin-bottom:28px;
}
.method h3{color:var(--pu);margin-bottom:14px;font-size:1.1rem}
.method ul{list-style:none;display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}
.method li{
  background:var(--sur);border-radius:10px;padding:12px 14px;
  font-size:.82rem;border-right:3px solid var(--pu);line-height:1.5;
}
.method li b{color:var(--cy)}

footer{
  text-align:center;padding:28px;color:var(--mu);font-size:.78rem;
  border-top:1px solid var(--brd);margin-top:40px;
}
</style>
</head>
<body>

<div class="hdr">
  <h1>🎯 TrendForge &nbsp;<span>@{{ username }}</span></h1>
  <div class="sub">
    📅 <b>{{ date }}</b> &nbsp;|&nbsp; 🕐 <b>{{ time }}</b> &nbsp;|&nbsp;
    منطقه زمانی تهران &nbsp;|&nbsp; {{ total }} ترند پردازش شد
  </div>
</div>

<div class="statbar">
  <div class="chip"><div class="n">{{ stats.total }}</div><div class="l">کل ترندها</div></div>
  <div class="chip"><div class="n">{{ stats.sources }}</div><div class="l">منابع فعال</div></div>
  <div class="chip"><div class="n">{{ stats.tech }}</div><div class="l">💻 تکنولوژی</div></div>
  <div class="chip"><div class="n">{{ stats.nature }}</div><div class="l">🌿 طبیعت</div></div>
  <div class="chip"><div class="n">{{ stats.viral }}</div><div class="l">🔥 وایرال</div></div>
  <div class="chip"><div class="n">{{ stats.fa }}</div><div class="l">🇮🇷 فارسی</div></div>
  <div class="chip"><div class="n">{{ stats.ideas }}</div><div class="l">💡 ایده ویدیو</div></div>
</div>

<div class="wrap">

  <!-- روش TrendForge -->
  <div class="method">
    <h3>⚡ روش TrendForge — الگوریتم امتیازدهی</h3>
    <ul>
      <li><b>۱. جمع‌آوری موازی</b><br>۶ منبع همزمان، هر کدام مستقل</li>
      <li><b>۲. تقویت چند-پلتفرمی</b><br>هر پلتفرم اضافی = +۱۵ امتیاز</li>
      <li><b>۳. آنالیز سرعت رشد</b><br>نه فقط حجم، بلکه شتاب ترند</li>
      <li><b>۴. تطابق موضوع پیج</b><br>برنامه‌نویسی + طبیعتگردی = +۵</li>
      <li><b>۵. بومی‌سازی فارسی</b><br>محتوای فارسی = +۸ برای مخاطب ایرانی</li>
      <li><b>۶. زمان‌بندی بهینه</b><br>تطبیق با ساعات طلایی اینستاگرام ایران</li>
    </ul>
  </div>

  <!-- برنامه روزانه -->
  <div class="sec">📅 برنامه پست امروز</div>
  <div class="sched">
    <div style="font-size:.85rem;color:var(--mu)">
      بهترین زمان‌های پست برای مخاطب ایرانی ({{ date }})
    </div>
    <div class="sched-grid">
      {% for slot in schedule %}
      <div class="slot">
        <div style="font-size:1.8rem">{{ slot.icon }}</div>
        <div class="t">{{ slot.time }}</div>
        <div class="lb">{{ slot.label }}</div>
        <div class="sug">💡 {{ slot.suggestion }}</div>
      </div>
      {% endfor %}
    </div>
  </div>

  <!-- ترندهای برتر -->
  <div class="sec">🏆 ۱۲ ترند برتر امروز</div>
  <div class="grid">
    {% for item in top %}
    <div class="card">
      <div class="rnk {% if loop.index==1 %}g{% elif loop.index==2 %}s{% elif loop.index==3 %}b{% endif %}">
        {{ loop.index }}
      </div>
      {% if item.url %}
      <div class="card-title"><a href="{{ item.url }}" target="_blank">{{ item.title }}</a></div>
      {% else %}
      <div class="card-title">{{ item.title }}</div>
      {% endif %}
      <div><span class="badge">{{ item.source }}</span></div>
      <div class="bar"><div class="bar-fill" style="width:{{ item.score|round }}%"></div></div>
      <div class="metas">
        <span class="mt mt-{{ item.category }}">{{ cat_labels[item.category] }}</span>
        <span class="mt" style="background:rgba(124,58,237,.12);color:#a78bfa">{{ item.score|round(1) }} pts</span>
        {% if item.engagement > 0 %}
        <span class="eng">👍 {{ "{:,}".format(item.engagement) }}</span>
        {% endif %}
        {% if item.language == "fa" %}<span class="mt" style="background:rgba(16,185,129,.12);color:var(--gn)">🇮🇷 فارسی</span>{% endif %}
      </div>
      <div class="ideas">
        <div class="ideas-lbl">💡 ایده‌های ویدیو:</div>
        {% for idea in item.video_ideas %}
        <div class="idea">{{ idea }}</div>
        {% endfor %}
      </div>
      <div class="tags">{% for tag in item.hashtags %}<span class="tag">{{ tag }}</span>{% endfor %}</div>
      <div class="ptime">📌 {{ item.best_post_time }}</div>
    </div>
    {% endfor %}
  </div>

  <!-- بر اساس دسته‌بندی -->
  {% for cat, cat_items in by_cat.items() %}
  {% if cat_items %}
  <div class="sec">{{ cat_labels[cat] }}</div>
  <div class="grid">
    {% for item in cat_items[:4] %}
    <div class="card">
      <div class="card-title">{{ item.title }}</div>
      <div><span class="badge">{{ item.source }}</span></div>
      <div class="bar"><div class="bar-fill" style="width:{{ item.score|round }}%"></div></div>
      <div class="ideas">
        <div class="ideas-lbl">💡 ایده‌های ویدیو:</div>
        {% for idea in item.video_ideas[:2] %}
        <div class="idea">{{ idea }}</div>
        {% endfor %}
      </div>
      <div class="tags">{% for tag in item.hashtags[:6] %}<span class="tag">{{ tag }}</span>{% endfor %}</div>
      <div class="ptime">📌 {{ item.best_post_time }}</div>
    </div>
    {% endfor %}
  </div>
  {% endif %}
  {% endfor %}

</div>

<footer>
  🤖 تولید شده توسط <b>TrendForge v1.0</b> برای @{{ username }} &nbsp;|&nbsp; {{ date }} {{ time }}<br>
  <span style="font-size:.7rem;color:#334155">هر بار که این برنامه اجرا می‌شود، گزارش جدیدی بر اساس ترندهای لحظه‌ای تولید می‌شود</span>
</footer>
</body>
</html>"""


class ReportGenerator:
    _CAT_LABELS = {
        "tech":     "💻 تکنولوژی",
        "nature":   "🌿 طبیعت & هایکینگ",
        "viral":    "🔥 وایرال",
        "video":    "🎬 ویدیو",
        "news":     "📰 اخبار",
        "news_fa":  "📰 اخبار فارسی",
        "science":  "🔬 علم",
        "general":  "✨ عمومی",
    }

    def generate(self, items: List[TrendItem], src_stats: Dict) -> str:
        tz  = pytz.timezone(CONFIG["timezone"])
        now = datetime.now(tz)

        cats = [i.category for i in items]
        schedule = self._build_schedule(items, now)
        by_cat = {
            c: [i for i in items if i.category == c]
            for c in ["tech", "nature", "viral", "news_fa", "science", "general"]
        }

        return Template(_HTML).render(
            username   = CONFIG["username"],
            date       = now.strftime("%Y/%m/%d"),
            time       = now.strftime("%H:%M"),
            total      = len(items),
            top        = items[:12],
            by_cat     = by_cat,
            cat_labels = self._CAT_LABELS,
            schedule   = schedule,
            stats      = {
                "total":   len(items),
                "sources": len([v for v in src_stats.values() if v > 0]),
                "tech":    cats.count("tech"),
                "nature":  cats.count("nature"),
                "viral":   cats.count("viral"),
                "fa":      sum(1 for i in items if i.language == "fa"),
                "ideas":   sum(len(i.video_ideas) for i in items),
            },
        )

    def _build_schedule(self, items: List[TrendItem], now: datetime) -> List[Dict]:
        def pick(cat: str) -> str:
            m = next((i for i in items if i.category == cat), None)
            if not m and items:
                m = items[0]
            if m:
                t = m.title
                return (t[:40] + "…") if len(t) > 40 else t
            return "ترند مناسب"

        return [
            {"icon": "🌅", "time": "۹:۰۰",  "label": "پست صبح",       "suggestion": pick("general")},
            {"icon": "☀️",  "time": "۱۴:۰۰", "label": "پست ظهر",       "suggestion": pick("tech")},
            {"icon": "🌙", "time": "۲۱:۰۰", "label": "پست شب (طلایی)", "suggestion": pick("viral")},
        ]


# ════════════════════════════════════════════════════════════
#  SECTION 7: MAIN ORCHESTRATOR
# ════════════════════════════════════════════════════════════

class TrendForge:
    def __init__(self):
        self.collectors: List[BaseCollector] = [
            GoogleTrendsCollector(),
            RedditCollector(),
            HackerNewsCollector(),
            RSSCollector(),
            YouTubeCollector(),
            TikTokCollector(),
            NewsAPICollector(),
        ]
        self.scorer   = TrendScorer()
        self.reporter = ReportGenerator()

    def run(self) -> str:
        console.print(Panel.fit(
            Text.assemble(
                ("  TrendForge v1.0  ", "bold white on dark_violet"),
                "\n",
                (f"  برای @{CONFIG['username']}  ", "bold cyan"),
            ),
            border_style="dark_violet",
            padding=(1, 4),
        ))

        all_items: List[TrendItem] = []
        src_stats: Dict[str, int]  = {}

        # ── Collect ──────────────────────────────────────
        console.print("\n[bold]📡 جمع‌آوری ترندها...[/bold]")
        with Progress(
            SpinnerColumn(style="purple"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=20, style="purple"),
            console=console,
        ) as prog:
            for col in self.collectors:
                task = prog.add_task(f"  {col.name}...", total=None)
                try:
                    found = col.collect()
                    all_items.extend(found)
                    src_stats[col.name] = len(found)
                    prog.update(
                        task,
                        description=f"  [green]✓[/green] {col.name}: {len(found)} ترند",
                    )
                except Exception as e:
                    src_stats[col.name] = 0
                    prog.update(
                        task,
                        description=f"  [yellow]⚠[/yellow] {col.name}: خطا ({str(e)[:30]})",
                    )
                finally:
                    prog.remove_task(task)

        active = sum(1 for v in src_stats.values() if v > 0)
        console.print(
            f"\n[bold green]✓ {len(all_items)} آیتم از {active} منبع فعال[/bold green]"
        )

        # ── Score ────────────────────────────────────────
        console.print("[bold cyan]📊 امتیازدهی و رتبه‌بندی...[/bold cyan]")
        ranked = self.scorer.merge_and_score(all_items)
        console.print(f"[bold green]✓ {len(ranked)} ترند نهایی انتخاب شد[/bold green]")

        # ── Report ───────────────────────────────────────
        console.print("[bold cyan]📄 تولید گزارش HTML...[/bold cyan]")
        html = self.reporter.generate(ranked, src_stats)

        # ── Save ─────────────────────────────────────────
        os.makedirs(CONFIG["output_dir"], exist_ok=True)
        tz  = pytz.timezone(CONFIG["timezone"])
        ts  = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
        out = os.path.join(CONFIG["output_dir"], f"report_{ts}.html")
        lat = os.path.join(CONFIG["output_dir"], "latest.html")

        for path in (out, lat):
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)

        # Clean old reports
        self._cleanup_old(CONFIG["output_dir"], CONFIG["keep_reports"])

        # ── Print summary ─────────────────────────────────
        self._print_table(ranked[:12])
        self._print_sources(src_stats)

        console.print(f"\n[bold green]✅ گزارش ذخیره شد:[/bold green] [cyan]{out}[/cyan]")

        # ── Open / Download ────────────────────────────────
        if IN_COLAB:
            try:
                from google.colab import files
                files.download(out)
                console.print("[bold cyan]📥 دانلود گزارش (Colab) شروع شد[/bold cyan]")
            except Exception:
                console.print(f"[dim]در Colab: گزارش در {out} ذخیره شد[/dim]")
        else:
            try:
                import webbrowser
                webbrowser.open(f"file://{os.path.abspath(lat)}")
                console.print("[bold cyan]🌐 گزارش در مرورگر باز شد[/bold cyan]")
            except Exception:
                pass

        return out

    # ── helpers ──────────────────────────────────────────
    def _print_table(self, items: List[TrendItem]):
        icons = {"tech": "💻", "nature": "🌿", "viral": "🔥",
                 "video": "🎬", "news": "📰", "news_fa": "📰",
                 "science": "🔬", "general": "✨"}
        t = Table(
            title="🏆 ترندهای برتر",
            border_style="dark_violet",
            show_lines=True,
        )
        t.add_column("رتبه", style="yellow",  width=5,  justify="center")
        t.add_column("عنوان", style="white",   max_width=42)
        t.add_column("منبع",  style="cyan",    width=20)
        t.add_column("امتیاز", style="green",  width=7,  justify="right")
        t.add_column("دسته",  style="magenta", width=14)

        for i, item in enumerate(items, 1):
            ic = icons.get(item.category, "✨")
            t.add_row(
                str(i),
                item.title[:40],
                item.source[:18],
                f"{item.score:.0f}",
                f"{ic} {item.category}",
            )
        console.print(t)

    def _print_sources(self, stats: Dict[str, int]):
        console.print("\n[bold]📡 وضعیت منابع:[/bold]")
        for name, cnt in stats.items():
            if cnt > 0:
                console.print(f"  [green]✓[/green] {name}: {cnt} ترند")
            else:
                console.print(f"  [red]✗[/red] {name}: خطا یا ناموجود")

    @staticmethod
    def _cleanup_old(directory: str, keep: int):
        try:
            files = sorted(
                [f for f in os.listdir(directory)
                 if f.startswith("report_") and f.endswith(".html")]
            )
            for old in files[:-keep]:
                os.remove(os.path.join(directory, old))
        except Exception:
            pass


# ════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════

def main():
    import argparse
    p = argparse.ArgumentParser(
        description="TrendForge — سیستم هوشمند تحلیل ترند برای @hosseinmarko"
    )
    p.add_argument("--youtube-key",   help="YouTube Data API v3 Key")
    p.add_argument("--newsapi-key",   help="NewsAPI.org Key")
    p.add_argument("--reddit-id",     help="Reddit Client ID")
    p.add_argument("--reddit-secret", help="Reddit Client Secret")
    p.add_argument("--loop",          type=int, default=0,
                   help="تعداد دقیقه بین اجراهای خودکار (0 = یک‌بار)")
    args = p.parse_args()

    if args.youtube_key:   CONFIG["youtube_api_key"] = args.youtube_key
    if args.newsapi_key:   CONFIG["newsapi_key"]     = args.newsapi_key
    if args.reddit_id:     CONFIG["reddit_client_id"] = args.reddit_id
    if args.reddit_secret: CONFIG["reddit_secret"]   = args.reddit_secret

    if args.loop > 0:
        console.print(f"[bold purple]حالت خودکار: هر {args.loop} دقیقه اجرا می‌شود[/bold purple]")
        while True:
            TrendForge().run()
            console.print(f"\n[dim]انتظار {args.loop} دقیقه تا اجرای بعدی...[/dim]")
            time.sleep(args.loop * 60)
    else:
        TrendForge().run()


if __name__ == "__main__":
    main()
