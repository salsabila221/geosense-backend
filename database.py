import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

# Baca .env (kalau ada). Tanpa .env, otomatis pakai SQLite lokal.
load_dotenv()

raw_url = (os.getenv("DATABASE_URL") or "sqlite:///./geosense.db").strip()

# Supabase / Heroku kadang memberi skema "postgres://" yang tidak dikenali SQLAlchemy 1.4+
if raw_url.startswith("postgres://"):
    raw_url = raw_url.replace("postgres://", "postgresql://", 1)

DATABASE_URL = raw_url
IS_SQLITE = DATABASE_URL.startswith("sqlite")

# Supabase transaction pooler berjalan di port 6543 (PgBouncer mode transaction).
# Di mode itu koneksi tidak boleh di-cache, jadi pakai NullPool.
IS_POOLER = ":6543" in DATABASE_URL

engine_kwargs = {"pool_pre_ping": True, "echo": os.getenv("SQL_ECHO", "0") == "1"}
if IS_SQLITE:
    # connect_args hanya wajib untuk SQLite
    engine_kwargs["connect_args"] = {"check_same_thread": False}
elif IS_POOLER:
    engine_kwargs["poolclass"] = NullPool
    # Supabase butuh SSL; biasanya sudah ada di connection string (?sslmode=require)
    engine_kwargs["connect_args"] = {"sslmode": os.getenv("PGSSLMODE", "require")}

engine = create_engine(DATABASE_URL, **engine_kwargs)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """Buka-tutup koneksi otomatis per request FastAPI."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def ensure_schema():
    """
    Migrasi ringan: tambahkan kolom baru kalau tabel users sudah ada dari versi lama.
    (create_all() hanya membuat tabel baru, tidak mengubah tabel yang sudah ada.)
    """
    inspector = inspect(engine)
    if "users" not in inspector.get_table_names():
        return

    existing = {column["name"] for column in inspector.get_columns("users")}
    wanted = {
        "google_sub": "VARCHAR",
        "picture": "VARCHAR",
        "phone_number": "VARCHAR",
        "role": "VARCHAR",
        "is_active": "BOOLEAN",
        "created_at": "TIMESTAMP",
        "last_login": "TIMESTAMP",
    }

    with engine.begin() as conn:
        for column, ddl in wanted.items():
            if column in existing:
                continue
            if engine.dialect.name == "postgresql":
                conn.execute(text(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {column} {ddl}"))
            else:
                conn.execute(text(f"ALTER TABLE users ADD COLUMN {column} {ddl}"))


def database_info() -> dict:
    """Info singkat buat endpoint /health (tanpa membocorkan credential)."""
    if IS_SQLITE:
        safe_url = DATABASE_URL
    else:
        # Sembunyikan user & password
        safe_url = DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else DATABASE_URL
    return {
        "dialect": engine.dialect.name,
        "target": safe_url,
        "pooler": IS_POOLER,
    }
