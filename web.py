from asyncio import CancelledError, Semaphore, create_task, shield
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from html import escape
from logging import getLogger
from os import environ
from pathlib import PurePosixPath
from re import sub
from secrets import compare_digest
from urllib.parse import quote, urlparse

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from httpx import AsyncClient, HTTPError, Response, Timeout

from source import XHS
from source.module import USERAGENT

PAGE = """<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Téléchargeur RedNote</title>
<style>
:root { color-scheme: light dark; font-family: system-ui, sans-serif; }
body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: #f4f5f7; color: #17191c; }
main { width: min(92vw, 620px); padding: 2rem; border-radius: 1.25rem; background: #fff; box-shadow: 0 1rem 3rem #0001; }
h1 { margin-top: 0; }
label { display: block; margin: 1rem 0 .4rem; font-weight: 650; }
input { box-sizing: border-box; width: 100%; padding: .8rem; border: 1px solid #aeb4bc; border-radius: .6rem; background: #fff; color: #17191c; }
button { width: 100%; margin-top: 1.25rem; padding: .9rem; border: 0; border-radius: .6rem; background: #ff2442; color: #fff; font-size: 1rem; font-weight: 700; cursor: pointer; }
small, .hint { color: #5c626b; }
.error { padding: .8rem; border-radius: .6rem; background: #ffe8eb; color: #9b1026; }
@media (prefers-color-scheme: dark) {
body { background: #111317; color: #f4f5f7; }
main { background: #1c2026; }
input { background: #111317; color: #f4f5f7; border-color: #555d68; }
small, .hint { color: #b2b8c0; }
}
</style>
</head>
<body>
<main>
<h1>Téléchargeur RedNote</h1>
<p>Collez le lien d’une publication Xiaohongshu/RedNote pour télécharger son média sans passer par la galerie de l’application.</p>
{{ERROR}}
<form method="post" action="/download/">
<label for="url">Lien de la publication</label>
<input id="url" name="url" type="url" required placeholder="https://www.xiaohongshu.com/explore/...">
<label for="index">Numéro de l’image</label>
<input id="index" name="index" type="number" min="1" value="1">
<p class="hint">Pour une vidéo, laisse 1. Pour plusieurs images, choisis leur numéro.</p>
<label for="token">Code d’accès</label>
<input id="token" name="token" type="password" autocomplete="current-password">
<button type="submit">Télécharger le média</button>
</form>
<p><small>Sur Android, le fichier est placé dans Téléchargements puis détecté par la galerie. Limite actuelle : {{LIMIT}} Mo par fichier.</small></p>
</main>
</body>
</html>
"""

MAX_MEDIA_SIZE_MB = max(1, int(environ.get("MAX_MEDIA_SIZE_MB", "250")))
MAX_MEDIA_BYTES = MAX_MEDIA_SIZE_MB * 1024 * 1024
MAX_REQUEST_BODY_BYTES = 64 * 1024
MAX_CONCURRENT_DOWNLOADS = max(1, int(environ.get("MAX_CONCURRENT_DOWNLOADS", "1")))
APP_TOKEN = environ.get("APP_TOKEN", "")
XHS_COOKIE = environ.get("XHS_COOKIE", "")
XHS_USER_AGENT = environ.get("XHS_USER_AGENT", USERAGENT)
XHS_PROXY = environ.get("XHS_PROXY") or None
MEDIA_DOMAINS = ("xhscdn.com", "xiaohongshu.com")
MEDIA_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/avif": ".avif",
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "video/x-m4v": ".m4v",
}
DOWNLOAD_SLOTS = Semaphore(MAX_CONCURRENT_DOWNLOADS)
logger = getLogger(__name__)


class MediaTooLargeError(RuntimeError):
    pass


class RequestBodyTooLarge(Exception):
    pass


def apply_security_headers(response):
    response.headers["Cache-Control"] = "no-store"
    response.headers["Content-Security-Policy"] = (
            "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
            "base-uri 'none'; frame-ancestors 'none'"
    )
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Robots-Tag"] = "noindex, nofollow"
    return response


class RequestBodyLimitMiddleware:
    def __init__(self, app, max_bytes: int):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] not in {"POST", "PUT", "PATCH"}:
            await self.app(scope, receive, send)
            return
        content_length = dict(scope.get("headers", [])).get(b"content-length")
        if content_length:
            try:
                declared_length = int(content_length)
            except ValueError:
                response = PlainTextResponse("Longueur de requête invalide.", status_code=400)
                await apply_security_headers(response)(scope, receive, send)
                return
            if declared_length < 0:
                response = PlainTextResponse("Longueur de requête invalide.", status_code=400)
                await apply_security_headers(response)(scope, receive, send)
                return
            if declared_length > self.max_bytes:
                response = PlainTextResponse("Requête trop volumineuse.", status_code=413)
                await apply_security_headers(response)(scope, receive, send)
                return
        received = 0
        response_started = False

        async def limited_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise RequestBodyTooLarge
            return message

        async def tracked_send(message):
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except RequestBodyTooLarge:
            if response_started:
                raise
            response = PlainTextResponse("Requête trop volumineuse.", status_code=413)
            await apply_security_headers(response)(scope, receive, send)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with XHS(
            work_path="/tmp",
            user_agent=XHS_USER_AGENT,
            cookie=XHS_COOKIE,
            proxy=XHS_PROXY,
            timeout=30,
            chunk=256 * 1024,
            max_retry=2,
            record_data=False,
            download_record=False,
            _print=False,
    ) as downloader:
        app.state.downloader = downloader
        yield


app = FastAPI(
    title="Téléchargeur RedNote",
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)
app.add_middleware(RequestBodyLimitMiddleware, max_bytes=MAX_REQUEST_BODY_BYTES)


def render_page(message: str = "") -> str:
    error = f'<p class="error">{escape(message)}</p>' if message else ""
    return PAGE.replace("{{ERROR}}", error).replace("{{LIMIT}}", str(MAX_MEDIA_SIZE_MB))


def check_token(value: str) -> None:
    if not APP_TOKEN or len(APP_TOKEN) > 512:
        raise HTTPException(
            status_code=503,
            detail="Le code d’accès du serveur n’est pas configuré.",
        )
    if not compare_digest(value[:512].encode("utf-8"), APP_TOKEN.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Code d’accès incorrect.")


def normalize_page_url(value: str) -> str:
    value = value.strip()
    if not value or len(value) > 2048:
        raise HTTPException(status_code=400, detail="Lien invalide.")
    try:
        parsed = urlparse(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="Lien invalide.")
    host = (parsed.hostname or "").lower()
    allowed = (
            parsed.scheme in {"http", "https"}
            and not parsed.username
            and not parsed.password
            and host == "www.xiaohongshu.com"
            and (
                parsed.path.startswith("/explore/")
                or parsed.path.startswith("/discovery/item/")
            )
    ) or (
            parsed.scheme in {"http", "https"}
            and not parsed.username
            and not parsed.password
            and host == "xhslink.com"
            and parsed.path != "/"
    )
    if not allowed:
        raise HTTPException(
            status_code=400,
            detail="Utilise un lien www.xiaohongshu.com ou xhslink.com.",
        )
    return value


def is_allowed_media_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
    except ValueError:
        return False
    host = (parsed.hostname or "").lower()
    return (
            parsed.scheme == "https"
            and not parsed.username
            and not parsed.password
            and any(
                host == domain or host.endswith(f".{domain}")
                for domain in MEDIA_DOMAINS
            )
    )


def select_media(data: dict, index: int) -> tuple[str, bool]:
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Données de publication invalides.")
    urls = data.get("下载地址")
    if not isinstance(urls, list) or not urls:
        raise HTTPException(
            status_code=404,
            detail="Aucun média téléchargeable n’a été trouvé.",
        )
    if index < 1 or index > len(urls):
        raise HTTPException(
            status_code=400,
            detail=f"Cette publication contient {len(urls)} média(s).",
        )
    media_url = urls[index - 1]
    if not isinstance(media_url, str) or not is_allowed_media_url(media_url):
        raise HTTPException(status_code=502, detail="Adresse média non valide.")
    return media_url, data.get("作品类型") == "视频"


def safe_text(value, fallback: str) -> str:
    value = sub(r"[^\w.-]+", "_", str(value or "")).strip(" ._")
    return value[:90].rstrip(" ._") or fallback


def make_filename(data: dict, extension: str) -> tuple[str, str]:
    note_id = safe_text(data.get("作品ID"), "note")
    title = safe_text(data.get("作品标题"), note_id)
    filename = safe_text(f"{note_id}_{title}", "rednote") + extension
    fallback_stem = sub(r"[^A-Za-z0-9_-]+", "_", str(data.get("作品ID") or "note"))
    fallback = f"rednote_{fallback_stem[:80] or 'note'}{extension}"
    return filename, fallback


def content_disposition(filename: str, fallback: str) -> str:
    return (
            f'attachment; filename="{fallback}"; '
            f"filename*=UTF-8''{quote(filename, safe='')}"
    )


def media_extension(content_type: str, media_url: str, is_video: bool) -> str:
    content_type = content_type.partition(";")[0].strip().lower()
    if content_type in MEDIA_EXTENSIONS:
        return MEDIA_EXTENSIONS[content_type]
    suffix = PurePosixPath(urlparse(media_url).path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".avif", ".mp4", ".mov", ".m4v"}:
        return ".jpg" if suffix == ".jpeg" else suffix
    return ".mp4" if is_video else ".png"


async def open_media(media_url: str) -> tuple[AsyncClient, Response]:
    client = AsyncClient(
        headers={
            "referer": "https://www.xiaohongshu.com/",
            "user-agent": XHS_USER_AGENT,
        },
        timeout=Timeout(connect=30, read=120, write=30, pool=30),
        follow_redirects=True,
        proxy=XHS_PROXY,
    )
    response = None

    async def close_partial():
        try:
            if response is not None:
                await response.aclose()
        except (CancelledError, Exception):
            pass
        try:
            await client.aclose()
        except (CancelledError, Exception):
            pass

    try:
        response = await client.send(client.build_request("GET", media_url), stream=True)
        if response.status_code >= 400:
            raise HTTPException(status_code=502, detail="Le serveur média a refusé la requête.")
        if not is_allowed_media_url(str(response.url)):
            raise HTTPException(status_code=502, detail="Redirection média non autorisée.")
        content_length = response.headers.get("content-length")
        if content_length:
            length = int(content_length)
            if length < 0:
                raise ValueError("Invalid content length")
            if length > MAX_MEDIA_BYTES:
                raise HTTPException(status_code=413, detail="Ce fichier dépasse la limite autorisée.")
        return client, response
    except HTTPException:
        await close_partial()
        raise
    except (HTTPError, ValueError):
        await close_partial()
        raise HTTPException(status_code=502, detail="Impossible de joindre le serveur média.")
    except Exception:
        await close_partial()
        raise HTTPException(status_code=502, detail="Impossible de préparer le téléchargement.")
    except BaseException:
        await close_partial()
        raise


class ManagedMediaStream:
    def __init__(self, client: AsyncClient, upstream: Response, release):
        self.client = client
        self.upstream = upstream
        self.release = release
        self.iterator = upstream.aiter_bytes(chunk_size=256 * 1024)
        self.downloaded = 0
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.closed:
            raise StopAsyncIteration
        try:
            chunk = await self.iterator.__anext__()
            self.downloaded += len(chunk)
            if self.downloaded > MAX_MEDIA_BYTES:
                raise MediaTooLargeError
            return chunk
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException:
            await self.aclose()
            raise

    async def cleanup(self):
        try:
            for operation in (
                    self.iterator.aclose,
                    self.upstream.aclose,
                    self.client.aclose,
            ):
                try:
                    await operation()
                except (CancelledError, Exception):
                    pass
        finally:
            self.release()

    async def aclose(self):
        if self.closed:
            return
        self.closed = True
        cleanup_task = create_task(self.cleanup())
        try:
            await shield(cleanup_task)
        except CancelledError:
            pass


class ManagedStreamingResponse(StreamingResponse):
    def __init__(self, stream: ManagedMediaStream, **kwargs):
        self.media_stream = stream
        super().__init__(stream, **kwargs)

    async def stream_response(self, send):
        try:
            await super().stream_response(send)
        finally:
            await self.media_stream.aclose()


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    return apply_security_headers(response)


@app.exception_handler(HTTPException)
async def http_exception(request: Request, exception: HTTPException):
    if request.method == "POST":
        return HTMLResponse(
            render_page(str(exception.detail)),
            status_code=exception.status_code,
        )
    return JSONResponse(
        {"detail": exception.detail},
        status_code=exception.status_code,
    )


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(render_page())


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/download/")
async def download(
        request: Request,
        url: str = Form(...),
        index: int = Form(1, ge=1, le=100),
        token: str = Form(""),
):
    check_token(token)
    page_url = normalize_page_url(url)
    await DOWNLOAD_SLOTS.acquire()
    try:
        results = await request.app.state.downloader.extract(
            page_url,
            download=False,
            data=True,
        )
    except Exception as error:
        DOWNLOAD_SLOTS.release()
        logger.error("Extraction impossible: %s", type(error).__name__)
        raise HTTPException(status_code=502, detail="Impossible d’analyser cette publication.")
    except BaseException:
        DOWNLOAD_SLOTS.release()
        raise
    if not isinstance(results, list) or len(results) != 1 or not results[0]:
        DOWNLOAD_SLOTS.release()
        raise HTTPException(status_code=404, detail="Cette publication est introuvable.")
    try:
        media_url, is_video = select_media(results[0], index)
        client, upstream = await open_media(media_url)
    except BaseException:
        DOWNLOAD_SLOTS.release()
        raise
    content_type = upstream.headers.get("content-type", "")
    extension = media_extension(content_type, str(upstream.url), is_video)
    filename, fallback = make_filename(results[0], extension)
    headers = {
        "Content-Disposition": content_disposition(filename, fallback),
        "X-Content-Type-Options": "nosniff",
    }
    content_length = upstream.headers.get("content-length")
    if content_length and not upstream.headers.get("content-encoding"):
        headers["Content-Length"] = content_length
    stream = ManagedMediaStream(client, upstream, DOWNLOAD_SLOTS.release)
    response_type = content_type.partition(";")[0].strip() or "application/octet-stream"
    try:
        return ManagedStreamingResponse(
            stream,
            media_type=response_type,
            headers=headers,
        )
    except BaseException:
        await stream.aclose()
        raise
