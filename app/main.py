from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
from typing import List, Optional
from .models import TopProducts, ProductAvailability, ChannelVisualContent, DailyWeeklyTrends

# Load environment variables
load_dotenv()

app = FastAPI(
    title="Ethiopian Medical Data API",
    description="API for analytical insights into Ethiopian medical businesses from Telegram data.",
    version="1.0.0"
)

# Database connection details
PG_USER = os.getenv('POSTGRES_USER')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
PG_DB = os.getenv('POSTGRES_DB')
PG_HOST = os.getenv('POSTGRES_HOST')
PG_PORT = os.getenv('POSTGRES_PORT')

DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Helper function to execute raw SQL and fetch results
def fetch_data_from_db(db, query, params=None):
    try:
        result = db.execute(text(query), params).fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.get("/", tags=["Health Check"])
async def read_root():
    return {"message": "Welcome to the Ethiopian Medical Data API! Visit /docs for API documentation."}

@app.get("/top-products", response_model=List[TopProducts], tags=["Analytics"])
async def get_top_products(db: SessionLocal = Depends(get_db)):
    """
    Returns the top 10 most frequently mentioned medical products or drugs.
    This is a simplified example; actual product extraction would need NLP.
    """
    query = """
    SELECT
        LOWER(SUBSTRING(message_content FROM '[A-Za-z]+ drug|product|medication')) AS product_name,
        COUNT(*) AS mention_count
    FROM fct_messages
    WHERE message_content ILIKE '%drug%' OR message_content ILIKE '%product%' OR message_content ILIKE '%medication%'
    GROUP BY product_name
    ORDER BY mention_count DESC
    LIMIT 10;
    """
    # NOTE: This query is a very basic example.
    # A robust solution would involve NLP techniques (NER, keyword extraction)
    # on `message_content` to identify actual product names.
    results = fetch_data_from_db(db, query)
    return [{"product_name": r[0].strip() if r[0] else "Unknown", "mention_count": r[1]} for r in results]

@app.get("/product-availability", response_model=List[ProductAvailability], tags=["Analytics"])
async def get_product_availability(
    product_name: str = Query(..., description="Name of the medical product/drug to query."),
    db: SessionLocal = Depends(get_db)
):
    """
    Shows how the price or availability of a specific product might vary across channels.
    This is a placeholder, as parsing prices/availability from free text is complex.
    """
    query = """
    SELECT
        dc.channel_name,
        COUNT(fm.message_id) AS mentions,
        MAX(CASE WHEN fm.message_content ILIKE '%available%' THEN 1 ELSE 0 END) AS is_available_mention,
        MAX(CASE WHEN fm.message_content ILIKE '%price%' OR fm.message_content ~* '\$\d+' THEN 1 ELSE 0 END) AS has_price_mention
    FROM fct_messages fm
    JOIN dim_channels dc ON fm.channel_sk = dc.channel_sk
    WHERE fm.message_content ILIKE :product_name
    GROUP BY dc.channel_name
    ORDER BY mentions DESC;
    """
    results = fetch_data_from_db(db, query, {'product_name': f'%{product_name}%'})
    return [
        {
            "channel_name": r[0],
            "mentions": r[1],
            "is_available_mention": bool(r[2]),
            "has_price_mention": bool(r[3])
        } for r in results
    ]

@app.get("/channel-visual-content", response_model=List[ChannelVisualContent], tags=["Analytics"])
async def get_channel_visual_content(db: SessionLocal = Depends(get_db)):
    """
    Returns which channels have the most visual content and a breakdown of detected objects.
    Assumes `detected_objects` in `fct_messages` is populated by the YOLO enrichment script.
    """
    query = """
    SELECT
        dc.channel_name,
        COUNT(fm.message_id) AS total_messages,
        COUNT(CASE WHEN fm.has_media = TRUE THEN fm.message_id END) AS messages_with_media,
        SUM(jsonb_array_length(fm.detected_objects)) AS total_detected_objects,
        jsonb_agg(DISTINCT jsonb_array_elements(fm.detected_objects)->>'class_name') AS distinct_detected_classes
    FROM fct_messages fm
    JOIN dim_channels dc ON fm.channel_sk = dc.channel_sk
    WHERE fm.has_media = TRUE AND fm.detected_objects IS NOT NULL
    GROUP BY dc.channel_name
    ORDER BY messages_with_media DESC;
    """
    results = fetch_data_from_db(db, query)
    return [
        {
            "channel_name": r[0],
            "total_messages": r[1],
            "messages_with_media": r[2],
            "total_detected_objects": r[3] if r[3] else 0,
            "distinct_detected_classes": r[4] if r[4] else []
        } for r in results
    ]

@app.get("/posting-trends", response_model=List[DailyWeeklyTrends], tags=["Analytics"])
async def get_posting_trends(
    time_grain: str = Query("day", description="Time grain for trends: 'day' or 'week'."),
    db: SessionLocal = Depends(get_db)
):
    """
    Returns daily and weekly trends in posting volume for health-related topics.
    """
    if time_grain not in ["day", "week"]:
        raise HTTPException(status_code=400, detail="Invalid time_grain. Must be 'day' or 'week'.")

    if time_grain == "day":
        query = """
        SELECT
            dd.date_day AS trend_period,
            COUNT(fm.message_id) AS posting_volume
        FROM fct_messages fm
        JOIN dim_dates dd ON fm.date_sk = dd.date_sk
        GROUP BY dd.date_day
        ORDER BY dd.date_day;
        """
    else: # week
        query = """
        SELECT
            dd.year || '-' || dd.week_of_year AS trend_period,
            COUNT(fm.message_id) AS posting_volume
        FROM fct_messages fm
        JOIN dim_dates dd ON fm.date_sk = dd.date_sk
        GROUP BY dd.year, dd.week_of_year
        ORDER BY dd.year, dd.week_of_year;
        """
    results = fetch_data_from_db(db, query)
    return [{"trend_period": str(r[0]), "posting_volume": r[1]} for r in results]