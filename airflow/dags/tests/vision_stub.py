"""A fake vision model for the image readers (kg/template_entities.py and
kg/frame_images.py): replies are queued, validated by the reader's own
validator as the real client would, and every call is recorded."""
import io
from types import SimpleNamespace

from PIL import Image


def jpeg(size=(1200, 800), color=(200, 100, 50)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, color).save(buf, "JPEG")
    return buf.getvalue()


class VisionClient:
    def __init__(self, replies):
        self.replies, self.calls = list(replies), []

    def chat(self, messages, request, *, purpose=None, format=None, options=None, think=None, validate=None,
             retry_temperature=None):
        self.calls.append({"messages": messages, "purpose": purpose, "think": think, "format": format,
                           "retry_temperature": retry_temperature})
        content = self.replies.pop(0)
        try:
            parsed = validate(content)
        except ValueError as exc:
            return SimpleNamespace(ok=False, error_kind="invalid", error=str(exc), attempts=3)
        return SimpleNamespace(ok=True, parsed=parsed, model="qwen3-vl:32b", digest="ff2e46876908",
                               host="ollama-ccdd", attempts=1)
