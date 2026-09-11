import os
from datetime import datetime, timezone
from typing import Any

import dlt

from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

API_URL = "https://api.fireflies.ai/graphql"
SOURCE_NAME = "fireflies"
TABLE_NAME = "fireflies_transcripts"

TRANSCRIPTS_QUERY = """
query Transcripts($limit: Int, $skip: Int) {
  transcripts(limit: $limit, skip: $skip) {
    id
    title
    date
  }
}
"""

TRANSCRIPT_QUERY = """
query Transcript($id: String!) {
  transcript(id: $id) {
    id
    title
    date
    transcript_url
    speakers {
      id
      name
    }
    sentences {
      index
      speaker_name
      speaker_id
      text
      start_time
      end_time
    }
    summary {
      keywords
      action_items
      outline
      shorthand_bullet
      overview
      bullet_gist
      gist
      short_summary
      short_overview
      meeting_type
      topics_discussed
      transcript_chapters
    }
  }
}
"""


class FirefliesClient:
    def __init__(self, api_key: str, http_client: Any = None):
        self.api_key = api_key
        self.http_client = http_client

        if self.http_client is None:
            import httpx

            self.http_client = httpx.Client(timeout=60.0)

    def execute(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.http_client.post(
            API_URL,
            json={
                "query": query,
                "variables": variables or {},
            },
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )

        response.raise_for_status()
        payload = response.json()

        if payload.get("errors"):
            raise RuntimeError(str(payload["errors"]))

        return payload.get("data", {})

    def list_transcripts(self, limit: int = 50) -> list[dict[str, Any]]:
        transcripts = []
        skip = 0

        while True:
            data = self.execute(
                TRANSCRIPTS_QUERY,
                {
                    "limit": limit,
                    "skip": skip,
                },
            )

            batch = data.get("transcripts") or []

            if not batch:
                break

            transcripts.extend(batch)

            if len(batch) < limit:
                break

            skip += limit

        return transcripts

    def get_transcript(self, transcript_id: str) -> dict[str, Any] | None:
        data = self.execute(
            TRANSCRIPT_QUERY,
            {"id": transcript_id},
        )

        return data.get("transcript")


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)

    text = str(value).strip()

    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_after_cursor(value: Any, cursor: str | None) -> bool:
    if cursor is None:
        return True

    current = _parse_datetime(value)
    previous = _parse_datetime(cursor)

    if current is None or previous is None:
        return str(value) > str(cursor)

    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)

    if previous.tzinfo is None:
        previous = previous.replace(tzinfo=timezone.utc)

    return current > previous


def _format_value(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, list):
        return "\n".join(f"- {_format_value(item)}" for item in value)

    if isinstance(value, dict):
        lines = []

        for key, item in value.items():
            if item is None:
                continue

            lines.append(f"{key}: {_format_value(item)}")

        return "\n".join(lines)

    return str(value)


def _transcript_to_row(
    transcript: dict[str, Any],
    ingest_transcripts: bool,
    ingest_summaries: bool,
    ingest_action_items: bool,
    ingest_speakers: bool,
) -> dict[str, Any]:
    sections = []

    title = transcript.get("title") or "Untitled meeting"

    if ingest_transcripts:
        sentences = transcript.get("sentences") or []
        transcript_lines = []

        for sentence in sentences:
            speaker = (
                sentence.get("speaker_name")
                or sentence.get("speaker_id")
                or "Unknown speaker"
            )
            text = sentence.get("text") or ""

            if text:
                transcript_lines.append(f"{speaker}: {text}")

        if transcript_lines:
            sections.append(
                "## Transcript\n\n" + "\n".join(transcript_lines)
            )

    summary = transcript.get("summary") or {}

    if ingest_summaries and summary:
        summary_parts = []

        for key in (
            "overview",
            "short_summary",
            "short_overview",
            "gist",
            "bullet_gist",
            "outline",
            "keywords",
            "topics_discussed",
            "meeting_type",
            "transcript_chapters",
        ):
            value = summary.get(key)

            if value:
                summary_parts.append(
                    f"### {key.replace('_', ' ').title()}\n\n"
                    f"{_format_value(value)}"
                )

        if summary_parts:
            sections.append(
                "## Summary\n\n" + "\n\n".join(summary_parts)
            )

    if ingest_action_items:
        action_items = summary.get("action_items")

        if action_items:
            sections.append(
                "## Action Items\n\n"
                + _format_value(action_items)
            )

    if ingest_speakers:
        speakers = transcript.get("speakers") or []

        if speakers:
            speaker_lines = []

            for speaker in speakers:
                name = speaker.get("name") or "Unknown speaker"
                speaker_id = speaker.get("id")

                if speaker_id:
                    speaker_lines.append(f"- {name} ({speaker_id})")
                else:
                    speaker_lines.append(f"- {name}")

            sections.append(
                "## Speakers\n\n" + "\n".join(speaker_lines)
            )

    content = f"# {title}\n\n"

    if sections:
        content += "\n\n".join(sections)

    return {
        "id": transcript.get("id"),
        "date": transcript.get("date"),
        "title": title,
        "url": transcript.get("transcript_url"),
        "content": content,
    }


@dlt.resource(
    name=TABLE_NAME,
    primary_key="id",
    write_disposition="replace",
)
def _fireflies_transcripts(
    api_key: str,
    ingest_transcripts: bool = True,
    ingest_summaries: bool = True,
    ingest_action_items: bool = True,
    ingest_speakers: bool = True,
    client: FirefliesClient | None = None,
):
    fireflies_client = client or FirefliesClient(api_key)

    state = dlt.current.resource_state()ú

    records = state.setdefault("records", {})
    date_cursor = state.get("date_cursor")

    metadata = fireflies_client.list_transcripts()

    current_ids = {
        item["id"]
        for item in metadata
        if item.get("id")
    }

    current_rows = {}

    newest_date = date_cursor

    for item in metadata:
        transcript_id = item.get("id")

        if not transcript_id:
            continue

        transcript_date = item.get("date")

        should_fetch = (
            transcript_id not in records
            or _is_after_cursor(transcript_date, date_cursor)
        )

        if should_fetch:
            transcript = fireflies_client.get_transcript(transcript_id)

            if transcript is not None:
                row = _transcript_to_row(
                    transcript,
                    ingest_transcripts
