import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from config import (
    CORS_ALLOW_HEADERS,
    CORS_ALLOW_METHODS,
    CORS_ALLOW_ORIGINS,
    STATIC_DIR,
)
from api import health, jobs, upload


def create_app() -> FastAPI:
    application = FastAPI(
        title="ServiceNow Bulk Uploader",
        description="Upload CSV rows with binary attachments to any ServiceNow table.",
        version="2.0.0",
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ALLOW_ORIGINS,
        allow_methods=CORS_ALLOW_METHODS,
        allow_headers=CORS_ALLOW_HEADERS,
        allow_credentials=True,
    )

    application.include_router(health.router)
    application.include_router(jobs.router)
    application.include_router(upload.router)

    application.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @application.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index() -> str:
        index_path = os.path.join(STATIC_DIR, "index.html")
        with open(index_path, encoding="utf-8") as f:
            return f.read()

    return application


app = create_app()