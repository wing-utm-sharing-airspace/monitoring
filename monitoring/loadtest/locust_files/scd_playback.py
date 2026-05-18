import argparse
from collections import defaultdict
import csv
from datetime import datetime, timezone
import json
import os
import sys
import client
from dateutil import parser as date_parser
import gevent
import locust
from utils import format_time
from uuid_extension import uuid7

# Increase CSV field size limit to handle large request bodies
csv.field_size_limit(sys.maxsize)


def _get_timestamp() -> str:
  """Get current absolute timestamp with milliseconds."""
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _extract_id(url: str) -> str:
  """Extract intent or subscription ID from URL."""
  parts = url.split("/")
  if "operational_intent_references" in parts:
    idx = parts.index("operational_intent_references")
    if len(parts) > idx + 1:
      return parts[idx + 1]
  if "subscriptions" in parts:
    idx = parts.index("subscriptions")
    if len(parts) > idx + 1:
      return parts[idx + 1]
  return "unknown"


def _load_csv(path):
  events = []
  expanded_path = os.path.expanduser(path)
  if os.path.exists(expanded_path):
    with open(expanded_path, "r") as f:
      reader = csv.DictReader(f)
      for row in reader:
        rel_start = float(row["relative_start_time"])
        rel_start_nanos = float(row.get("relative_start_time_nanos", 0))
        events.append({
            "relative_start_time": (
                rel_start + (rel_start_nanos / 1_000_000_000)
            ),
            "method": row["method"],
            "url": row["request_url"],
            "body": row["request_body"],
        })
  events.sort(key=lambda x: x["relative_start_time"])
  return events


@locust.events.init_command_line_parser.add_listener
def init_parser(parser: argparse.ArgumentParser):
  """Setup config params, populated by locust.conf."""

  parser.add_argument(
      "--uss-base-url",
      type=str,
      help="Base URL of the USS",
      dest="uss_urls",
      action="append",
      required=True,
  )
  parser.add_argument(
      "--csv-file",
      type=str,
      help=(
          "Path to CSV file containing requests for one USS, multiple files can"
          " be specified (e.g. --csv-file=file1.csv --csv-file=file2.csv). One"
          " User will execute the requests in each file."
      ),
      dest="csv_files",
      action="append",
      required=True,
  )
  parser.add_argument(
      "--original-data-start-time",
      type=int,
      help=(
          "Unix timestamp (in seconds) of the original start time of the"
          " recorded data in the CSV file."
      ),
      required=True,
  )
  parser.add_argument(
      "--csv-start-delay",
      type=int,
      help="Buffer (in seconds) to wait before replaying the first CSV request",
      default=5,
  )
  parser.add_argument(
      "--sub-csv-file",
      type=str,
      help="Path to CSV file containing subscriptions to pre-seed",
  )


class SCD(client.USS):

  def on_start(self):
    # each user takes one USS URL
    if self.environment.parsed_options.uss_urls:
      self.uss_base_url = self.environment.parsed_options.uss_urls.pop()
    else:
      self.uss_base_url = "unknown"
    # each user takes one csv file
    if self.environment.parsed_options.csv_files:
      self.csv_file = self.environment.parsed_options.csv_files.pop()
      self.csv_events = _load_csv(self.csv_file)
    else:
      self.csv_file = None
      self.csv_events = []
    print("New USS")
    print(f"CSV file: {self.csv_file}")
    print(f"USS base URL: {self.uss_base_url}")
    self.original_data_start_time = (
        self.environment.parsed_options.original_data_start_time
    )
    self.csv_start_delay = self.environment.parsed_options.csv_start_delay
    self.sub_csv_file = self.environment.parsed_options.sub_csv_file
    self.test_start_time = datetime.now(timezone.utc)
    if self.sub_csv_file:
      self._preseed_subscriptions()

  def _preseed_subscriptions(self):
    print(
        f"[{_get_timestamp()}] DEBUG: Pre-seeding subscriptions from"
        f" {self.sub_csv_file}"
    )
    sub_events = _load_csv(self.sub_csv_file)
    for event in sub_events:
      self._execute_csv_event(event)
  
  def on_stop(self):
    # Best effort lean up remaining intents.
    for entity_id in self.oi_dict:
      resp = self.client.delete(
          f"/dss/v1/operational_intent_references/{entity_id}/{self.oi_dict[entity_id]}",
          name="/dss/v1/operational_intent_references/[id]/[ovn]",
      )
      if resp.status_code == 200 or resp.status_code == 404:
        with self.lock:
          del self.oi_dict[entity_id]
    # Best effort clean up remaining subscriptions.
    for entity_id in self.sub_dict:
      resp = self.client.delete(
          f"/dss/v1/subscriptions/{entity_id}/{self.sub_dict[entity_id]}",
          name="/dss/v1/subscriptions/[id]/[version]",
      )
      if resp.status_code == 200 or resp.status_code == 404:
        with self.lock:
          del self.sub_dict[entity_id]

  @locust.task
  def task_csv_replay(self):
    if not self.csv_events:
      return

    # Sort and group parsed events.
    grouped_events = defaultdict(list)
    for event in self.csv_events:
      grouped_events[event["relative_start_time"]].append(event)

    # Schedule playback events.
    all_main_greenlets = []
    for rel_time, events in sorted(grouped_events.items()):
      delay = rel_time + self.csv_start_delay
      event_details = ", ".join(
          [f"{e['method']} {_extract_id(e['url'])}" for e in events]
      )
      all_main_greenlets.append(
          gevent.spawn_later(delay, self._execute_csv_events, events)
      )
    gevent.joinall(all_main_greenlets)
    print(
        f"[{_get_timestamp()}] DEBUG: CSV replay completed for user {id(self)}"
    )
    # Only execute tasks for this user once, wait for test to be ended manually.
    while True:
      gevent.sleep(1000)

  def _execute_csv_events(self, events):
    batch_greenlets = []
    for event in events:
      batch_greenlets.append(gevent.spawn(self._execute_csv_event, event))
    gevent.joinall(batch_greenlets)

  def _execute_csv_event(self, event):
    method = event["method"]
    url = event["url"]
    body_str = event["body"]

    is_scd_oi = "/dss/v1/operational_intent_references/" in url
    is_sub = "/dss/v1/subscriptions/" in url
    entity_id = _extract_id(url)
    # Handle using OVNs generated during test.
    if is_scd_oi and method in ("PUT", "DELETE"):
      with self.lock:
        stored_ovn = self.oi_dict.get(entity_id)
        parts = url.split("/")
        if stored_ovn:
          if len(parts) <= 5:
            # URL was .../operational_intent_references/{id}
            url = f"{url}/{stored_ovn}"
          else:
            # URL was .../operational_intent_references/{id}/{old_ovn}
            parts[-1] = stored_ovn
            url = "/".join(parts)
        else:
          if len(parts) >= 6:
            # URL was .../operational_intent_references/{id}/{old_ovn}
            # need to strip ovn.
            url = "/".join(parts[:-1])

    # Process Request body.
    if body_str:
      try:
        body_json = json.loads(body_str)
        # Replace uss_base_url with the one provided for this test
        if "uss_base_url" in body_json:
          body_json["uss_base_url"] = self.uss_base_url
        if (
            "new_subscription" in body_json
            and "uss_base_url" in body_json["new_subscription"]
        ):
          body_json["new_subscription"]["uss_base_url"] = self.uss_base_url

        # Shift extents if present
        if "extents" in body_json:
          if is_sub:
            body_json["extents"] = self._shift_sub_extents(body_json["extents"])
          else:
            body_json["extents"] = self._shift_extents(body_json["extents"])

        if method == "PUT":
          if is_scd_oi:
            # Include all known current OVNs in the 'key'
            body_json["key"] = list(self.oi_dict.values())
            # Generate new requested ovn if there was one.
            if "requested_ovn_suffix" in body_json:
              body_json["requested_ovn_suffix"] = uuid7().hex

        body_str = json.dumps(body_json)
      except (json.JSONDecodeError, TypeError) as e:
        print(
            f"[{_get_timestamp()}] DEBUG: Failed to process request body for"
            f" {method} {url}: {body_str} with error: {e}"
        )

    # Normalize name for Locust reporting
    name = event["url"]
    if "/dss/v1/operational_intent_references/" in name:
      name = "/dss/v1/operational_intent_references/..."
    elif "/dss/v1/subscriptions/" in name:
      name = "/dss/v1/subscriptions/..."

    resp = self.client.request(method=method, url=url, data=body_str, name=name)

    # Track created/updated intents/subscriptions for OVN Key/cleanup.
    if method == "PUT" and resp.status_code in (200, 201):
      try:
        data = resp.json()
        if is_scd_oi:
          resp_id = data["operational_intent_reference"]["id"]
          ovn = data["operational_intent_reference"]["ovn"]
          with self.lock:
            self.oi_dict[resp_id] = ovn
        elif is_sub:
          sub = data.get("subscription")
          if sub:
            resp_id = sub["id"]
            version = sub.get("version")
            with self.lock:
              self.sub_dict[resp_id] = version
      except (ValueError, KeyError):
        pass

    # Untrack deleted intents/subscriptions
    if method == "DELETE" and resp.status_code == 200:
      if is_scd_oi:
        parts = url.split("/")
        if len(parts) >= 2:
          # parts[-2] is the ID for /references/{id}/{ovn}
          entity_id = parts[-2]
          with self.lock:
            if entity_id in self.oi_dict:
              del self.oi_dict[entity_id]
      elif is_sub:
        entity_id = _extract_id(url)
        with self.lock:
          if entity_id in self.sub_dict:
            del self.sub_dict[entity_id]

  def _shift_extents(self, extents):
    offset = (
        self.test_start_time.timestamp()
        + self.csv_start_delay
        - self.environment.parsed_options.original_data_start_time
    )

    new_extents = []
    for extent in extents:
      new_extent = extent.copy()
      try:
        ts = date_parser.isoparse(extent["time_start"]["value"]).timestamp()
        te = date_parser.isoparse(extent["time_end"]["value"]).timestamp()

        new_extent["time_start"] = {
            "value": format_time(
                datetime.fromtimestamp(ts + offset, timezone.utc)
            ),
            "format": "RFC3339",
        }
        new_extent["time_end"] = {
            "value": format_time(
                datetime.fromtimestamp(te + offset, timezone.utc)
            ),
            "format": "RFC3339",
        }
      except (KeyError, ValueError, TypeError) as e:
        print(
            f"[{_get_timestamp()}] DEBUG: Failed to shift extents for extent:"
            f" {extent} with error: {e}"
        )
        pass
      new_extents.append(new_extent)
    return new_extents

  def _shift_sub_extents(self, extent):
    offset = (
        self.test_start_time.timestamp()
        - self.environment.parsed_options.original_data_start_time
    )

    new_extent = extent.copy()
    try:
      ts = date_parser.isoparse(extent["time_start"]["value"]).timestamp()
      te = date_parser.isoparse(extent["time_end"]["value"]).timestamp()

      # If subscriptions started before test data, start them at the beginning
      # of the test.
      new_ts = ts + offset
      if new_ts < self.test_start_time.timestamp():
        new_ts = self.test_start_time.timestamp()

      new_extent["time_start"] = {
          "value": format_time(datetime.fromtimestamp(new_ts, timezone.utc)),
          "format": "RFC3339",
      }
      new_extent["time_end"] = {
          "value": format_time(
              datetime.fromtimestamp(te + offset, timezone.utc)
          ),
          "format": "RFC3339",
      }
    except (KeyError, ValueError, TypeError):
      pass
    return new_extent

