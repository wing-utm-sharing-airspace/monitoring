import argparse
import datetime
import random
import uuid

import client
from geo_utils import create_random_flight_path_volume, create_volume, merge_flight_paths
import gevent
import locust


@locust.events.init_command_line_parser.add_listener
def init_parser(parser: argparse.ArgumentParser):
  """Setup config params, populated by locust.conf."""

  parser.add_argument(
      "--uss-base-url",
      type=str,
      help="Base URL of the USS",
      required=True,
  )
  parser.add_argument(
      "--area-lat",
      type=float,
      help="Latitude of the center of the area in which to create flights",
      required=True,
  )
  parser.add_argument(
      "--area-lng",
      type=float,
      help="Longitude of the center of the area in which to create flights",
      required=True,
  )
  parser.add_argument(
      "--area-radius",
      type=int,
      help="Radius (in meters) of the area in which to create flights",
      required=True,
  )
  parser.add_argument(
      "--max-flight-distance",
      type=int,
      help="Maximum distance to cover for an individual flight",
      required=True,
  )
  parser.add_argument(
      "--oi-duration",
      type=int,
      help="Duration (in seconds) of the operational intent",
      default=10,
  )


class SCD(client.USS):
  wait_time = locust.between(0.01, 0.1)

  def on_start(self):
    self.uss_base_url = self.environment.parsed_options.uss_base_url
    self.lat = self.environment.parsed_options.area_lat
    self.lng = self.environment.parsed_options.area_lng
    self.radius = self.environment.parsed_options.area_radius
    self.max_flight_distance = (
        self.environment.parsed_options.max_flight_distance
    )
    self.oi_duration = self.environment.parsed_options.oi_duration

  @locust.task
  def task_put_intent(self):
    entity_id = uuid.uuid4().hex
    with self.lock:
      key = list(self.oi_dict.values())

    altitude_lower = random.randint(0, 10000)
    altitude_upper = altitude_lower + 1

    start_time = datetime.datetime.now()
    end_time = start_time + datetime.timedelta(seconds=self.oi_duration)

    rects = create_random_flight_path_volume(
        self.lat,
        self.lng,
        self.radius,
        self.max_flight_distance,
        self.oi_duration,
        altitude_lower,
        altitude_upper,
        start_time,
        end_time,
    )
    extents = [
        create_volume(
            r,
            altitude_lower,
            altitude_upper,
            start_time,
            end_time,
        )
        for r in rects.geoms
    ]
    isa_poly = merge_flight_paths(rects)
    isa_extent = create_volume(
        isa_poly,
        altitude_lower,
        altitude_upper,
        start_time,
        end_time,
    )

    body = {
        "state": "Accepted",
        "uss_base_url": self.uss_base_url,
        "new_subscription": {
            "uss_base_url": self.uss_base_url,
        },
        "extents": extents,
        "key": key,
    }

    def put_oi():
      resp = self.client.put(
          f"/dss/v1/operational_intent_references/{entity_id}",
          json=body,
          name="/dss/v1/operational_intent_references/[id]",
      )
      if resp.status_code in (200, 201):
        ovn = resp.json()["operational_intent_reference"]["ovn"]
        with self.lock:
          self.oi_dict[entity_id] = ovn

    def put_isa():
      resp = self.client.put(
          f"/rid/v2/dss/identification_service_areas/{entity_id}",
          json={
              "extents": isa_extent,
              "uss_base_url": self.uss_base_url,
          },
          name="/identification_service_areas/[entity_id]",
      )

    tasks = [
        gevent.spawn(put_oi),
        gevent.spawn(put_isa),
    ]

    # Wait for all greenlets to finish and get results
    gevent.joinall(tasks)

