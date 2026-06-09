#!/usr/bin/env python3
"""
Database initialization script for the audio masking streaming system.
This script creates the SQLite database and required tables.
"""

from db import init_db

if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    print("Database initialized successfully.")
