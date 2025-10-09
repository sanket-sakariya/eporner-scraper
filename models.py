from pydantic import BaseModel
from typing import List, Optional
import json

class URLMessage(BaseModel):
    url: str
    depth: int = 0

class VideoData(BaseModel):
    video_url: str
    view_count: str
    like_count: str
    mp4_links: List[str]
    jpg_links: List[str]

class ScrapedData(BaseModel):
    url: str
    video_data: Optional[VideoData] = None
    internal_links: List[str] = []