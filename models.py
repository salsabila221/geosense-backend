from sqlalchemy import Column, String, Boolean, Integer, DateTime
from database import Base
import datetime

class User(Base):
    __tablename__ = "users"

    email = Column(String, primary_key=True, index=True)
    fullname = Column(String)
    telegram = Column(String, nullable=True) # Tetap ada, bisa buat simpen nomor HP atau username
    role = Column(String, default="user")    # nilainya 'user' atau 'admin'
    is_active = Column(Boolean, default=True)

class SystemStatus(Base):
    __tablename__ = "system_status"
    
    # ID dibuat 1 supaya datanya selalu ter-update di baris yang sama
    id = Column(Integer, primary_key=True, index=True, default=1)
    status = Column(String, default="Normal")  # Normal, Siaga, atau Warning
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)