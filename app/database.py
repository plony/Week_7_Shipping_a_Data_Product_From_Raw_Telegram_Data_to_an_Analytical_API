# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from dotenv import load_dotenv
# import os

# load_dotenv()

# PG_USER = os.getenv('POSTGRES_USER')
# PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
# PG_DB = os.getenv('POSTGRES_DB')
# PG_HOST = os.getenv('POSTGRES_HOST')
# PG_PORT = os.getenv('POSTGRES_PORT')

# DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
# engine = create_engine(DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()