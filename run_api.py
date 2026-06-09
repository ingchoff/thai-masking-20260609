#!/usr/bin/env python3
"""
Script to run the FastAPI server for the audio masking streaming system.
"""

import uvicorn
from Masking.logging_utils import LOGGER

# Bind the name and ensure service_name is set
logger = LOGGER.bind(name="api", service_name="api")

if __name__ == "__main__":
    # Initialize logging configuration
    logger.info("Logging initialized")
    logger.info("Starting Audio Masking API server...")
    
    # Configure UVicorn to use our logging
    config = uvicorn.Config(
        "api:app",
        host="0.0.0.0",
        port=7969,
        reload=False,
        log_config=None  # Disable UVicorn's default logging
    )
    server = uvicorn.Server(config)
    server.run()