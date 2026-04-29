from __future__ import annotations

from fastapi import APIRouter, Depends, File, HTTPException, Request, Response, UploadFile, status
from pydantic import BaseModel, Field
from typing import Optional

from app.config import Settings, get_settings
from app.core.auth import create_session_token, decode_session_token
from app.services.audit import AuditLogger
from app.services.query_engine import QueryEngine, QueryValidationError
from app.services.user_store import UserRecord, UserStore


router = APIRouter(prefix="/api")


class LoginPayload(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class QueryPayload(BaseModel):
    query: str = Field(min_length=1)
    selected_file: Optional[str] = None
    root_path: Optional[str] = None
    recursive: Optional[bool] = None
    page: int = Field(default=1, ge=1)
    page_size: Optional[int] = Field(default=None, ge=1)


class PasswordChangePayload(BaseModel):
    current_password: str = Field(min_length=1)
    new_password: str = Field(min_length=8)


def get_query_engine(settings: Settings = Depends(get_settings)) -> QueryEngine:
    return QueryEngine(
        settings.parquet_path,
        settings.parquet_root,
        settings.max_preview_rows,
        settings.default_page_size,
        settings.max_page_size,
        settings.max_upload_mb,
        settings.allow_recursive_scan,
    )

def get_user_store(settings: Settings = Depends(get_settings)) -> UserStore:
    return UserStore(settings.users_file, settings.admin_username, settings.admin_password_hash)


def get_audit_logger(settings: Settings = Depends(get_settings)) -> AuditLogger:
    return AuditLogger(settings.audit_log_path)


def require_user(
    request: Request,
    settings: Settings = Depends(get_settings),
    user_store: UserStore = Depends(get_user_store),
) -> UserRecord:
    token = request.cookies.get("pqs_session")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")

    payload = decode_session_token(token, settings.secret_key)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired.")
    user = user_store.get_user(str(payload.get("sub", "")))
    if not user or user.disabled:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired.")
    return user


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
def login(
    payload: LoginPayload,
    request: Request,
    response: Response,
    settings: Settings = Depends(get_settings),
    user_store: UserStore = Depends(get_user_store),
    audit: AuditLogger = Depends(get_audit_logger),
) -> dict[str, str]:
    user = user_store.authenticate(payload.username, payload.password)
    if not user:
        audit.log("login_failed", payload.username, request.client.host if request.client else None, {})
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
    audit.log("login_success", user.username, request.client.host if request.client else None, {"role": user.role})
    return {"message": "authenticated"}


@router.post("/auth/logout")
def logout(
    request: Request,
    response: Response,
    user: UserRecord = Depends(require_user),
    audit: AuditLogger = Depends(get_audit_logger),
) -> dict[str, str]:
    audit.log("logout", user.username, request.client.host if request.client else None, {})
    response.delete_cookie("pqs_session")
    return {"message": "signed_out"}


@router.get("/auth/me")
def me(user: UserRecord = Depends(require_user)) -> dict[str, str]:
    return {"username": user.username, "role": user.role}


@router.post("/auth/change-password")
def change_password(
    payload: PasswordChangePayload,
    request: Request,
    user: UserRecord = Depends(require_user),
    user_store: UserStore = Depends(get_user_store),
    audit: AuditLogger = Depends(get_audit_logger),
) -> dict[str, str]:
    try:
        user_store.change_password(user.username, payload.current_password, payload.new_password)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    audit.log("password_changed", user.username, request.client.host if request.client else None, {})
    return {"message": "password_updated"}


@router.get("/schema")
def schema(
    selected_file: Optional[str] = None,
    root_path: Optional[str] = None,
    recursive: Optional[bool] = None,
    _: UserRecord = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
) -> dict[str, object]:
    try:
        return {
            "items": engine.describe_schema(selected_file, root_path, recursive),
            "selected_file": selected_file,
            "root_path": str(engine.resolve_root(root_path)),
            "recursive": recursive,
        }
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@router.get("/files")
def files(
    root_path: Optional[str] = None,
    recursive: Optional[bool] = None,
    _: UserRecord = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
) -> dict[str, object]:
    try:
        return engine.list_files(root_path, recursive)
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/upload")
async def upload(
    request: Request,
    root_path: Optional[str] = None,
    file: UploadFile = File(...),
    user: UserRecord = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
    audit: AuditLogger = Depends(get_audit_logger),
) -> dict[str, object]:
    try:
        payload = await file.read()
        result = engine.upload_parquet(root_path, file.filename or "upload.parquet", payload)
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    audit.log(
        "file_uploaded",
        user.username,
        request.client.host if request.client else None,
        {"filename": result["filename"], "root_path": root_path},
    )
    return result


@router.get("/preview")
def preview(
    selected_file: Optional[str] = None,
    root_path: Optional[str] = None,
    recursive: Optional[bool] = None,
    page: int = 1,
    page_size: Optional[int] = None,
    _: UserRecord = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
) -> dict[str, object]:
    try:
        result = engine.preview(selected_file, root_path, recursive, page, page_size)
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    return result.__dict__


@router.post("/query")
def query(
    payload: QueryPayload,
    request: Request,
    user: UserRecord = Depends(require_user),
    engine: QueryEngine = Depends(get_query_engine),
    audit: AuditLogger = Depends(get_audit_logger),
) -> dict[str, object]:
    try:
        result = engine.run_query(
            payload.query,
            selected_file=payload.selected_file,
            root_path=payload.root_path,
            recursive=payload.recursive,
            page=payload.page,
            page_size=payload.page_size,
        )
    except QueryValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    audit.log(
        "query_executed",
        user.username,
        request.client.host if request.client else None,
        {
            "selected_file": payload.selected_file,
            "root_path": payload.root_path,
            "page": result.page,
            "page_size": result.page_size,
            "row_count": result.row_count,
            "total_rows": result.total_rows,
        },
    )
    return result.__dict__
