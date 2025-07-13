from pydantic import BaseModel
from typing import List, Optional, Any

class TopProducts(BaseModel):
    product_name: str
    mention_count: int

class ProductAvailability(BaseModel):
    channel_name: str
    mentions: int
    is_available_mention: bool
    has_price_mention: bool

class ChannelVisualContent(BaseModel):
    channel_name: str
    total_messages: int
    messages_with_media: int
    total_detected_objects: int
    distinct_detected_classes: List[str]

class DailyWeeklyTrends(BaseModel):
    trend_period: str
    posting_volume: int