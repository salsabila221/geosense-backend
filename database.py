from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Untuk sekarang pakai SQLite lokal dulu. 
# Pas mau deploy ke cloud Postgres, tinggal ganti baris di bawah ini!
DATABASE_URL = "sqlite:///./geosense.db"

# connect_args hanya wajib untuk SQLite
engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Fungsi pembantu untuk buka-tutup koneksi otomatis di FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()