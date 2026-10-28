# AutoPulse vehicle simulator

Simulates 10 fleet vehicles driving on New York City's real streets and sends
one telemetry event per vehicle every 5 seconds to Kafka (`vehicle.telemetry.raw`).

| File | What it is |
|---|---|
| `simulate_vehicle_telemetry.py` | The simulator (Kafka producer). Runs on the EC2 streaming host as a service. |
| `road_network/road_network.py` | Loads the street map and finds the fastest route between two intersections. |
| `road_network/build_road_network.py` | Builds the street map from OpenStreetMap. Run once, on a laptop. |
| `road_network/nyc_roads.bin.gz` | The street map (all five boroughs), created by the script above. Commit it. |

## How the vehicles drive

- **Trips**: each vehicle picks a destination 2-8 km away (and at least 1 km
  from its last 5 stops, so routes don't keep revisiting the same places) and
  follows the **fastest legal route** there: real streets only, one-way streets
  respected, never through buildings, parks or water.
- **Speed**: the road's speed limit (posted limit from OpenStreetMap, otherwise
  NYC's 25 mph default, 40-50 mph on highways) x the driver's pace for that trip.
  15% of trips are driven 10-30% over the limit (these show up as speeding).
  Speed changes gradually (acceleration/braking per 5-second step).
- **Red lights**: at 15% of the intersections passed, the vehicle waits 10-40 s
  (engine `idling`).
- **Delivery stops**: at each destination the vehicle parks 2-8 minutes (engine
  `off`), then plans the next trip.
- **Working hours** (on EC2): Mon-Fri 8:00-17:00 New York time; vehicles stay
  parked outside those hours.

Each event also carries `speed_limit_mph`, the limit of the road the vehicle is
on, so the Gold layer judges speeding per road (`is_speeding = speed > limit`).

## Building the street map (once)

From the project root, with the virtualenv active:

    python sources/vehicle_simulator/road_network/build_road_network.py

It downloads the five boroughs' drivable roads from OpenStreetMap's Overpass
API (a few minutes; retries automatically if the free server is busy), keeps
the connected street network, and writes `road_network/nyc_roads.bin.gz`.
Map data (c) OpenStreetMap contributors (ODbL).
