import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
from httpx import URL

import web
from source.expansion import file_folder
from source.module import manager as manager_module


class FakeDownloader:
    async def extract(self, url, download=False, data=True):
        return [{
            "作品ID": "abc123",
            "作品标题": "Ma vidéo",
            "作品类型": "视频",
            "下载地址": ["https://sns-video-bd.xhscdn.com/video.mp4"],
        }]


class FakeClient:
    def __init__(self):
        self.closed = False

    async def aclose(self):
        self.closed = True


class FakeUpstream:
    def __init__(self, content_encoding=None):
        self.url = URL("https://sns-video-bd.xhscdn.com/video.mp4")
        self.headers = {
            "content-type": "video/mp4",
            "content-length": "9",
        }
        if content_encoding:
            self.headers["content-encoding"] = content_encoding
        self.closed = False

    async def aiter_bytes(self, chunk_size):
        yield b"video"
        yield b"file"

    async def aclose(self):
        self.closed = True


class SlowUpstream(FakeUpstream):
    def __init__(self):
        super().__init__()
        self.iterator_closed = False

    async def aiter_bytes(self, chunk_size):
        try:
            yield b"video"
            await asyncio.Event().wait()
        finally:
            self.iterator_closed = True


class WebHelperTests(unittest.TestCase):
    def setUp(self):
        self.previous_token = web.APP_TOKEN
        web.APP_TOKEN = "test-token"

    def tearDown(self):
        web.APP_TOKEN = self.previous_token

    def test_normalize_page_url_accepts_supported_links(self):
        url = "https://www.xiaohongshu.com/explore/abc123?xsec_token=test"
        self.assertEqual(web.normalize_page_url(url), url)
        self.assertEqual(
            web.normalize_page_url("https://xhslink.com/a/abc123"),
            "https://xhslink.com/a/abc123",
        )

    def test_normalize_page_url_rejects_other_hosts(self):
        with self.assertRaises(HTTPException):
            web.normalize_page_url("https://example.com/video")

    def test_media_url_allowlist(self):
        self.assertTrue(web.is_allowed_media_url(
            "https://sns-video-bd.xhscdn.com/video.mp4",
        ))
        self.assertFalse(web.is_allowed_media_url(
            "https://cdn.example.com/video.mp4",
        ))
        self.assertFalse(web.is_allowed_media_url(
            "http://sns-video-bd.xhscdn.com/video.mp4",
        ))

    def test_select_media_uses_one_based_index(self):
        data = {
            "作品类型": "图文",
            "下载地址": [
                "https://sns-img.xhscdn.com/one.png",
                "https://sns-img.xhscdn.com/two.png",
            ],
        }
        url, is_video = web.select_media(data, 2)
        self.assertEqual(url, "https://sns-img.xhscdn.com/two.png")
        self.assertFalse(is_video)

    def test_filename_and_disposition(self):
        filename, fallback = web.make_filename({
            "作品ID": "abc123",
            "作品标题": "旅行 à Paris",
        }, ".mp4")
        disposition = web.content_disposition(filename, fallback)
        self.assertIn("attachment", disposition)
        self.assertIn(fallback, disposition)
        self.assertIn("filename*=UTF-8''", disposition)

    def test_index_and_health(self):
        with TestClient(web.app) as client:
            index = client.get("/")
            health = client.get("/health")
            self.assertEqual(index.status_code, 200)
            self.assertIn("Téléchargeur RedNote", index.text)
            self.assertEqual(health.status_code, 200)
            self.assertEqual(health.json(), {"status": "ok"})

    def test_invalid_form_returns_html_error(self):
        client = TestClient(web.app)
        response = client.post("/download/", data={
            "url": "https://example.com/video",
            "index": "1",
            "token": "test-token",
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn("www.xiaohongshu.com", response.text)

    def test_access_token_is_enforced(self):
        web.APP_TOKEN = "secret"
        client = TestClient(web.app)
        response = client.post("/download/", data={
            "url": "https://www.xiaohongshu.com/explore/abc123",
            "index": "1",
            "token": "wrong",
        })
        self.assertEqual(response.status_code, 401)
        self.assertIn("Code d’accès incorrect", response.text)

    def test_access_token_must_be_configured(self):
        web.APP_TOKEN = ""
        client = TestClient(web.app)
        response = client.post("/download/", data={
            "url": "https://www.xiaohongshu.com/explore/abc123",
            "index": "1",
            "token": "",
        })
        self.assertEqual(response.status_code, 503)
        self.assertIn("n’est pas configuré", response.text)

    def test_unicode_access_token_is_supported(self):
        web.APP_TOKEN = "clé-privée"
        client = TestClient(web.app)
        response = client.post("/download/", data={
            "url": "https://example.com/video",
            "index": "1",
            "token": "clé-privée",
        })
        self.assertEqual(response.status_code, 400)

    def test_request_body_limit(self):
        client = TestClient(web.app)
        response = client.post("/download/", data={
            "url": "x" * (web.MAX_REQUEST_BODY_BYTES + 1),
            "index": "1",
            "token": "test-token",
        })
        self.assertEqual(response.status_code, 413)
        self.assertIn("Cache-Control", response.headers)

    def test_proxy_dictionary_preserves_schemes(self):
        instance = object.__new__(manager_module.Manager)
        instance.message = lambda text, *args: text
        response = Mock()
        proxy = {
            "http://": "http://proxy.example:8080",
            "https://": "http://secure-proxy.example:8443",
        }
        with patch.object(manager_module, "get", return_value=response) as request:
            result = instance._Manager__check_proxy(proxy)
        self.assertEqual(set(result["mounts"]), {"http://", "https://"})
        self.assertEqual(request.call_args.kwargs["proxy"], proxy["http://"])

        async def close_mounts():
            for transport in result["mounts"].values():
                await transport.aclose()

        asyncio.run(close_mounts())

    def test_empty_directory_cleanup_is_bounded(self):
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            empty = root / "temp" / "empty"
            kept = root / "temp" / "kept"
            empty.parent.mkdir()
            empty.mkdir()
            kept.mkdir()
            (kept / "file.txt").touch()
            file_folder.remove_empty_directories(root / "temp")
            self.assertFalse(empty.exists())
            self.assertTrue((kept / "file.txt").exists())
            self.assertTrue(root.exists())

    def test_cancelled_stream_releases_resources(self):
        async def scenario():
            upstream = SlowUpstream()
            upstream_client = FakeClient()
            released = []
            stream = web.ManagedMediaStream(
                upstream_client,
                upstream,
                lambda: released.append(True),
            )
            self.assertEqual(await anext(stream), b"video")
            pending = asyncio.create_task(anext(stream))
            await asyncio.sleep(0)
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            await asyncio.sleep(0)
            self.assertTrue(upstream.iterator_closed)
            self.assertTrue(upstream.closed)
            self.assertTrue(upstream_client.closed)
            self.assertEqual(released, [True])

        asyncio.run(scenario())

    def test_forced_video_download(self):
        upstream = FakeUpstream()
        upstream_client = FakeClient()
        with TestClient(web.app) as client:
            web.app.state.downloader = FakeDownloader()
            with patch.object(
                    web,
                    "open_media",
                    new=AsyncMock(return_value=(upstream_client, upstream)),
            ):
                response = client.post("/download/", data={
                    "url": "https://www.xiaohongshu.com/explore/abc123",
                    "index": "1",
                    "token": "test-token",
                })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"videofile")
        self.assertEqual(response.headers["content-type"], "video/mp4")
        self.assertEqual(response.headers["content-length"], "9")
        self.assertIn("attachment;", response.headers["content-disposition"])
        self.assertIn(".mp4", response.headers["content-disposition"])
        self.assertTrue(upstream.closed)
        self.assertTrue(upstream_client.closed)

    def test_encoded_media_omits_inconsistent_length(self):
        upstream = FakeUpstream(content_encoding="gzip")
        upstream_client = FakeClient()
        with TestClient(web.app) as client:
            web.app.state.downloader = FakeDownloader()
            with patch.object(
                    web,
                    "open_media",
                    new=AsyncMock(return_value=(upstream_client, upstream)),
            ):
                response = client.post("/download/", data={
                    "url": "https://www.xiaohongshu.com/explore/abc123",
                    "index": "1",
                    "token": "test-token",
                })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"videofile")
        self.assertNotIn("content-length", response.headers)
        self.assertNotIn("content-encoding", response.headers)


if __name__ == "__main__":
    unittest.main()
