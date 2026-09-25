"""Web dashboard. A separate process that only reads the SQLite database; it never
touches the trading core. Run: uvicorn spr_analytics.dashboard.app:app --port 8000"""
