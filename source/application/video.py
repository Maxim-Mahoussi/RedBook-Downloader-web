from source.expansion import Namespace
from .request import Html

__all__ = ['Video']


class Video:
    VIDEO_LINK = (
        "video",
        "consumer",
        "originVideoKey",
    )
    STREAM_LINKS = (
        "video.media.stream.h264[0].masterUrl",
        "video.media.stream.h265[0].masterUrl",
        "video.media.stream.av1[0].masterUrl",
        "video.media.stream.h264[0].backupUrls[0]",
    )

    @classmethod
    def get_video_link(cls, data: Namespace) -> list:
        if origin_key := data.safe_extract(".".join(cls.VIDEO_LINK)):
            return [f"https://sns-video-bd.xhscdn.com/{origin_key}"]
        for attribute in cls.STREAM_LINKS:
            if media_url := data.safe_extract(attribute):
                return [cls.__secure_media_url(media_url)]
        return []

    @staticmethod
    def __secure_media_url(media_url: str) -> str:
        media_url = Html.format_url(media_url)
        if media_url.startswith("http://"):
            return f"https://{media_url.removeprefix("http://")}"
        return media_url
