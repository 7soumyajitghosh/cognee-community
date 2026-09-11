# Fireflies Connector

A Fireflies data-source connector for Cognee.

This connector allows Fireflies meeting data to be synchronized into Cognee for search and knowledge processing.

## Features

- Connect to Fireflies using an API key
- Uses the Fireflies GraphQL API
- Ingest meeting transcripts
- Ingest meeting summaries
- Ingest action items
- Preserve speaker attribution
- Incremental synchronization using transcript dates
- Detect and handle upstream deletions
- Select which data to ingest

## Installation

```bash
uv sync --all-extras

Or install the package directly:

pip install -e .

Configuration

Set your Fireflies API key as an environment variable:

export FIREFLIES_API_KEY="your_api_key"

Do not commit your API key or other credentials to GitHub.

Usage

from cognee_community_connector_fireflies import fireflies_source

source = fireflies_source(
    ingest_transcripts=True,
    ingest_summaries=True,
    ingest_action_items=True,
    ingest_speakers=True,
)

Supported Data

The connector can ingest:

Meeting transcripts

Meeting titles

Meeting dates

Transcript URLs

Speaker names and attribution

Meeting summaries

Action items


Incremental Synchronization

The connector uses the Fireflies transcript date as a synchronization cursor.

Newer records are fetched during subsequent synchronizations while previously processed unchanged records can be reused.

Upstream records that no longer exist are omitted from the current snapshot so they can be removed during synchronization.

Examples

Run the example with:

uv run python ./examples/example.py

Testing

Run the tests with:

pytest

Requirements

Python 3.11–3.13

Cognee

Fireflies API key


License

Apache-2.0
