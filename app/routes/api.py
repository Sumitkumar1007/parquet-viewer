from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from app.config import Settings, get_settings
from app.core.auth import create_session_token, decode_session_token, verify_password
from app.services.query_engine import QueryEngine, QueryValidationError


router = APIRouter(prefix="/api")


class LoginPayload(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class QueryPayload(BaseModel):
    query: str = Field(min_length=1)
    selected_file: str | None = None
    root_path: str | None = None


def get_query_engine(settings: Settings = Depends(get_settings)) -> QueryEngine:
    return QueryEngine(settings.parquet_path, settings.parquet_root, settings.max_preview_rows)


def require_user(request: Request, settings: Settings = Depends(get_settings)) -> str:
    token = request.cookies.get("pqs_session")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")

    payload = decode_session_token(token, settings.secret_key)
    if not payload or payload.get("sub") != settings.admin_username:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired.")
    return settings.admin_username


@router.get("/health")
def health(settings: Settings = Depends(get_settings)) -> dict[str, object]:
    return {
        "status": "ok",
        "app": settings.app_name,
        "environment": settings.app_env,
        "parquet_path": str(settings.parquet_path),
        "parquet_root": str(settings.parquet_root),
    }


@router.post("/auth/login")
def login(payload: LoginPayload, response: Response, settings: Settings = Depends(get_settings)) -> dict[str, str]:
    if not settings.admin_password_hash:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ADMIN_PASSWORD_HASH not configured.",
        )

    valid_user = payload.username == settings.admin_username
    valid_password = verify_password(payload.password, settings.admin_password_hash)
    if not (valid_user and valid_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")

    token = create_session_token(payload.username, settings.secret_key, settings.session_ttl_minutes)
    response.set_cookie(
        key="pqs_session",
        value=token,
        httponly=True,
        secure=settings.app_env != "development",
        samesite="lax",
        max_age=settings.session_ttl_minutes * 60,
    )
    return {"message": "authenticated"}


@router.post("/auth/logout")
def logout(response: Response) -> dict[str, str]:
    response.delete_cookie("pqs_session")
    return {"message": "signed_out"}


@router.get("/auth/me")
def me(username: str = Depends(require_user)) -> dict[str, str]:
    return {"username": username}


@router.get("/schema")
def schema(
    selected_file: str | None = None,
    root_path: str | None = None,
    _: str = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
) -> dict[str, object]:
    try:
        return {
            "items": engine.describe_schema(selected_file, root_path),
            "selected_file": selected_file,
            "root_path": str(engine.resolve_root(root_path)),
        }
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@router.get("/files")
def files(
    root_path: str | None = None,
    _: str = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
) -> dict[str, object]:
    try:
        return engine.list_files(root_path)
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/preview")
def preview(
    selected_file: str | None = None,
    root_path: str | None = None,
    _: str = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
) -> dict[str, object]:
    try:
        result = engine.preview(selected_file, root_path)
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    return result.__dict__


@router.post("/query")
def query(
    payload: QueryPayload,
    _: str = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
) -> dict[str, object]:
    try:
        result = engine.run_query(
            payload.query,
            selected_file=payload.selected_file,
            root_path=payload.root_path,
        )
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    return result.__dict__
