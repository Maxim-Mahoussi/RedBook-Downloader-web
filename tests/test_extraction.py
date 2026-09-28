import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import quote

from source.application.app import XHS
from source.application.request import Html
from source.application.video import Video
from source.expansion import Namespace


class ShortLinkHtml:
    def __init__(self, resolved_url):
        self.resolved_url = resolved_url
        self.requests = []

    async def request_url(self, url, content, log):
        self.requests.append((url, content))
        return self.resolved_url


class ResponseClient:
    def __init__(self, url):
        self.url = url
        self.get_calls = []
        self.head_calls = []

    async def get(self, url, headers, **kwargs):
        self.get_calls.append((url, headers))
        return SimpleNamespace(
            url=self.url,
            raise_for_status=lambda: None,
        )

    async def head(self, url, headers, **kwargs):
        self.head_calls.append((url, headers))
        return SimpleNamespace(url=self.url)


class ExtractionTests(unittest.TestCase):
    def test_short_link_uses_get_and_login_redirect_path(self):
        real_url = (
            "http://www.xiaohongshu.com/discovery/item/abc123"
            "?xsec_source=app_share&xsec_token=token%3D"
        )
        login_url = (
            "https://www.xiaohongshu.com/login?redirectPath="
            f"{quote(real_url, safe='')}"
        )
        fake_html = ShortLinkHtml(login_url)
        application = object.__new__(XHS)
        application.html = fake_html
        links = asyncio.run(application._XHS__extract_links(
            "http://xhslink.com/o/abc123",
            None,
        ))
        self.assertEqual(fake_html.requests, [(
            "http://xhslink.com/o/abc123",
            "url",
        )])
        self.assertEqual(links, [real_url])

    def test_url_resolution_uses_get_not_head(self):
        final_url = "https://www.xiaohongshu.com/discovery/item/abc123"
        client = ResponseClient(final_url)
        html = Html(SimpleNamespace(
            retry=0,
            message=lambda text, *args: text,
            request_client=client,
            headers={},
            blank_headers={},
        ))
        with patch(
                "source.application.request.sleep_time",
                new=AsyncMock(),
        ):
            resolved = asyncio.run(html.request_url(
                "http://xhslink.com/o/abc123",
                "url",
                None,
            ))
        self.assertEqual(resolved, final_url)
        self.assertEqual(len(client.get_calls), 1)
        self.assertEqual(client.head_calls, [])

    def test_new_video_stream_url(self):
        data = Namespace({
            "video": {
                "media": {
                    "stream": {
                        "h264": [{
                            "masterUrl": "http://sns-video-v27.xhscdn.com/stream/video.mp4?sign=abc",
                        }],
                    },
                },
            },
        })
        self.assertEqual(Video.get_video_link(data), [
            "https://sns-video-v27.xhscdn.com/stream/video.mp4?sign=abc",
        ])

    def test_legacy_video_origin_key(self):
        data = Namespace({
            "video": {
                "consumer": {
                    "originVideoKey": "stream/legacy.mp4",
                },
            },
        })
        self.assertEqual(Video.get_video_link(data), [
            "https://sns-video-bd.xhscdn.com/stream/legacy.mp4",
        ])


if __name__ == "__main__":
    unittest.main()
