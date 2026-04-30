from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import get_settings
from app.core.auth import decode_session_token
from app.services.user_store import UserStore


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _is_authenticated(request: Request) -> bool:
    settings = get_settings()
    token = request.cookies.get("pqs_session")
    if not token:
        return False
    payload = decode_session_token(token, settings.secret_key)
    if not payload:
        return False
    user_store = UserStore(settings.users_file, settings.admin_username, settings.admin_password_hash)
    return user_store.get_user(str(payload.get("sub", ""))) is not None


@router.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/login", status_code=302)

    settings = get_settings()
    return templates.TemplateResponse(
        request=request,
        name="app.html",
        context={
            "app_name": settings.app_name,
            "parquet_path": str(settings.parquet_path),
            "parquet_root": str(settings.parquet_root),
            "hdfs_base_path": settings.hdfs_base_path,
            "environment": settings.app_env,
        },
    )


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    if _is_authenticated(request):
        return RedirectResponse(url="/", status_code=302)

    settings = get_settings()
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"app_name": settings.app_name},
    )
