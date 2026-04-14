import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal
from models.user import User
from services.security import get_password_hash

def create_verification_user():
    db = SessionLocal()
    try:
        username = "antigravity"
        existing = db.query(User).filter(User.username == username).first()
        if not existing:
            user = User(
                username=username,
                email="antigravity@test.com",
                password_hash=get_password_hash("password123")
            )
            db.add(user)
            db.commit()
            print(f"User {username} created successfully.")
        else:
            print(f"User {username} already exists.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    create_verification_user()
