import os

os.environ.setdefault("LAKEBASE_HOST", "localhost")
os.environ.setdefault("LAKEBASE_PASSWORD", "unused")

from fastapi.testclient import TestClient  # noqa: E402

import app.main as main  # noqa: E402


class BrokenEndpoints:
    def query(self, **kwargs):
        raise RuntimeError("429 REQUEST_LIMIT_EXCEEDED: rate limit")


class FakeClient:
    serving_endpoints = BrokenEndpoints()


def test_draft_returns_json_502_when_model_endpoint_fails(monkeypatch):
    monkeypatch.setattr(main, "WorkspaceClient", lambda: FakeClient())
    res = TestClient(main.app).post("/api/rules/draft", json={"text": "anything"})
    assert res.status_code == 502
    assert "REQUEST_LIMIT_EXCEEDED" in res.json()["detail"]
