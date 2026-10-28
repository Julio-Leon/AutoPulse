"""
AutoPulse - build the New York City street map for the vehicle simulator.

Run ONCE on your laptop (it downloads from the internet; the EC2 host doesn't):

    python sources/vehicle_simulator/road_network/build_road_network.py

What it does:
  1. Downloads every drivable road in the five boroughs from OpenStreetMap
     (the free, public world map) through its Overpass API, in ~4 x 4 km tiles
     (small requests; each finished tile is saved, so a re-run continues where
     it stopped): highways, avenues, streets and their ramps. Not footpaths, parks,
     private/service roads, ferries or roads closed to cars.
  2. Turns them into a graph: intersections are vertices, the road between two
     intersections is an edge, in each direction traffic may drive (one-way
     streets get one edge). Each edge keeps its length, speed limit (posted
     limit if mapped, else NYC defaults below) and shape.
  3. Keeps only the part of the network where every intersection can reach
     every other one (drops dead-end fragments, e.g. bridge stubs ending in
     New Jersey), so a route always exists.
  4. Writes sources/vehicle_simulator/road_network/nyc_roads.bin.gz (a few MB).
     Commit it; it's uploaded to EC2 with the rest of sources/.

Takes ~5-20 minutes (132 small downloads, 4 at a time, one per server). Uses only the Python standard library.
Map data (c) OpenStreetMap contributors, ODbL (openstreetmap.org/copyright).
"""

import json
import sys
import time
import urllib.parse
import urllib.request
from array import array
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from pathlib import Path

# Import the shared file format from road_network/road_network.py (the same way the simulator does).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from road_network.road_network import ARRAYS, distance_m, save  # noqa: E402

OUTPUT_FILE = Path(__file__).resolve().parent / "nyc_roads.bin.gz"
# Each downloaded tile is kept here, so a re-run only downloads the tiles still
# missing (data/ is git-ignored). Delete the folder to download fresh map data.
CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "osm_cache"

# New York City (the five boroughs), by its Wikidata ID: the most reliable way to
# name an area in OpenStreetMap. Roads outside it (New Jersey, Long Island) are left out.
NYC_WIKIDATA_ID = "Q60"

# The city is downloaded in tiles of about 4 x 4 km: small requests that the
# free servers rarely refuse. Tiles over water simply come back empty.
NYC_BOUNDS = (40.49, -74.27, 40.92, -73.68)          # south, west, north, east
TILE_DEGREES = (0.04, 0.05)                          # latitude, longitude

# Drivable road types, and the speed limit (mph) used when a road has none mapped.
# 25 mph is New York City's default limit on city streets.
DEFAULT_SPEED_MPH = {
    "motorway": 50, "motorway_link": 30,
    "trunk": 40, "trunk_link": 25,
    "primary": 25, "primary_link": 25,
    "secondary": 25, "secondary_link": 25,
    "tertiary": 25, "tertiary_link": 25,
    "unclassified": 25,
    "residential": 20,
    "living_street": 10,
}

REQUEST_TIMEOUT_SECONDS = 90
MAX_ATTEMPTS = 16                 # 4 rounds over the 4 servers
PARALLEL_DOWNLOADS = 4            # one per server

# Public Overpass servers (free, sometimes busy: "502/504" errors). Tried in turn.
OVERPASS_SERVERS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]


# --------------------------------------------------
# 1. Download
# --------------------------------------------------

def tiles():
    """(name, south, west, north, east) for every tile covering the city."""
    south, west, north, east = NYC_BOUNDS
    tile_lat, tile_lon = TILE_DEGREES
    row, lat = 0, south
    while lat < north:
        column, lon = 0, west
        while lon < east:
            yield (f"tile_{row:02d}_{column:02d}", round(lat, 4), round(lon, 4),
                   round(min(lat + tile_lat, north), 4), round(min(lon + tile_lon, east), 4))
            column, lon = column + 1, lon + tile_lon
        row, lat = row + 1, lat + tile_lat


def overpass_query(south, west, north, east):
    road_types = "|".join(DEFAULT_SPEED_MPH)
    return f"""
        [out:json][timeout:60];
        area["wikidata"="{NYC_WIKIDATA_ID}"]->.nyc;
        way(area.nyc)({south},{west},{north},{east})["highway"~"^({road_types})$"]
            ["area"!="yes"]["access"!~"^(private|no)$"]["motor_vehicle"!~"^(private|no)$"];
        out body;
        >;
        out skel qt;
    """


def download_tile(number, name, south, west, north, east):
    """
    One tile's roads and points (from the cache if downloaded before).
    Tries the servers in turn, starting with a different one per tile so the
    parallel downloads spread over all servers. Only after every server has
    failed once does it wait before the next round.
    """
    cache_file = CACHE_DIR / f"{name}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())

    body = urllib.parse.urlencode({"data": overpass_query(south, west, north, east)}).encode()
    for attempt in range(1, MAX_ATTEMPTS + 1):
        server = OVERPASS_SERVERS[(number + attempt) % len(OVERPASS_SERVERS)]
        try:
            request = urllib.request.Request(server, data=body,
                                             headers={"User-Agent": "AutoPulse road network builder"})
            # A tile answers in seconds; a server that hasn't answered in 90 s is overloaded.
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                elements = json.loads(response.read())["elements"]
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            temp_file = cache_file.with_suffix(".tmp")      # complete files only, even on Ctrl+C
            temp_file.write_text(json.dumps(elements))
            temp_file.replace(cache_file)
            return elements
        except Exception as error:
            round_failed = attempt % len(OVERPASS_SERVERS) == 0
            wait = 60 if round_failed else 2
            print(f"    {name}: {server.split('/')[2]} failed ({error})"
                  + ("; all servers busy, waiting 60s" if round_failed else "; trying the next server"),
                  flush=True)
            time.sleep(wait)
    raise SystemExit(f"Could not download {name}. Run the script again later: "
                     f"tiles already downloaded are kept.")


# --------------------------------------------------
# 2. Graph
# --------------------------------------------------

def speed_limit(tags):
    """Posted limit in mph ("25 mph"; a plain number is km/h in OpenStreetMap), else the default."""
    text = tags.get("maxspeed", "").strip().lower()
    try:
        if text.endswith("mph"):
            mph = int(float(text[:-3]))
        elif text:
            mph = round(float(text) / 1.609)
        else:
            raise ValueError
    except ValueError:
        mph = DEFAULT_SPEED_MPH[tags["highway"]]
    return max(5, min(mph, 65))


def travel_directions(tags):
    """(forward allowed, backward allowed) along the way's drawing direction."""
    oneway = tags.get("oneway", "").lower()
    if oneway in ("yes", "true", "1"):
        return True, False
    if oneway in ("-1", "reverse"):
        return False, True
    if oneway == "no":
        return True, True
    implied = tags["highway"] in ("motorway", "motorway_link") or tags.get("junction") in ("roundabout", "circular")
    return (True, False) if implied else (True, True)


def build_edges(ways, coordinates):
    """
    Split every way at intersections. Returns edges as
    (from_node, to_node, length_m, speed_mph, [shape points between them]).
    """
    uses = defaultdict(int)
    for way in ways:
        for node in way["nodes"]:
            uses[node] += 1
        uses[way["nodes"][0]] += 2            # road ends are vertices too
        uses[way["nodes"][-1]] += 2

    edges = []
    for way in ways:
        nodes = [n for n in way["nodes"] if n in coordinates]
        if len(nodes) < 2:
            continue
        forward, backward = travel_directions(way["tags"])
        speed = speed_limit(way["tags"])

        piece = [nodes[0]]
        for node in nodes[1:]:
            piece.append(node)
            if uses[node] >= 2 or node == nodes[-1]:          # reached an intersection
                start, end = piece[0], piece[-1]
                if start != end:
                    length = sum(distance_m(*coordinates[a], *coordinates[b]) for a, b in zip(piece, piece[1:]))
                    shape = [coordinates[n] for n in piece[1:-1]]
                    if forward:
                        edges.append((start, end, length, speed, shape))
                    if backward:
                        edges.append((end, start, length, speed, shape[::-1]))
                piece = [node]
    return edges


def largest_connected_part(edges):
    """
    The nodes of the largest strongly connected component: the biggest set of
    intersections that can all reach each other, respecting one-way streets
    (Kosaraju's algorithm, written without recursion).
    """
    forward, backward = defaultdict(list), defaultdict(list)
    for start, end, *_ in edges:
        forward[start].append(end)
        backward[end].append(start)
    nodes = set(forward) | set(backward)

    # Pass 1: order nodes by when their depth-first search finishes.
    order, visited = [], set()
    for root in nodes:
        if root in visited:
            continue
        visited.add(root)
        stack = [(root, iter(forward[root]))]
        while stack:
            node, neighbors = stack[-1]
            for neighbor in neighbors:
                if neighbor not in visited:
                    visited.add(neighbor)
                    stack.append((neighbor, iter(forward[neighbor])))
                    break
            else:
                stack.pop()
                order.append(node)

    # Pass 2: walk the reversed graph in reverse finishing order; each walk is one component.
    best, assigned = set(), set()
    for root in reversed(order):
        if root in assigned:
            continue
        component, stack = {root}, [root]
        assigned.add(root)
        while stack:
            for neighbor in backward[stack.pop()]:
                if neighbor not in assigned:
                    assigned.add(neighbor)
                    component.add(neighbor)
                    stack.append(neighbor)
        if len(component) > len(best):
            best = component
    return best


# --------------------------------------------------
# 3. Packing into arrays
# --------------------------------------------------

def pack(edges, keep, coordinates):
    edges = [e for e in edges if e[0] in keep and e[1] in keep]
    vertex_ids = sorted(keep)
    index = {node: i for i, node in enumerate(vertex_ids)}
    edges.sort(key=lambda e: index[e[0]])

    arrays = {name: array(typecode) for name, typecode in ARRAYS}
    for node in vertex_ids:
        lat, lon = coordinates[node]
        arrays["vertex_lat_e6"].append(round(lat * 1e6))
        arrays["vertex_lon_e6"].append(round(lon * 1e6))

    counts = defaultdict(int)
    for start, *_ in edges:
        counts[index[start]] += 1
    position = 0
    for vertex in range(len(vertex_ids)):
        arrays["edge_start"].append(position)
        position += counts[vertex]
    arrays["edge_start"].append(position)

    arrays["shape_start"].append(0)
    for start, end, length, speed, shape in edges:
        arrays["edge_target"].append(index[end])
        arrays["edge_length_m"].append(length)
        arrays["edge_speed_mph"].append(speed)
        for lat, lon in shape:
            arrays["shape_lat_e6"].append(round(lat * 1e6))
            arrays["shape_lon_e6"].append(round(lon * 1e6))
        arrays["shape_start"].append(len(arrays["shape_lat_e6"]))
    return arrays, edges


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    ways, coordinates = {}, {}
    all_tiles = list(tiles())
    done = sum((CACHE_DIR / f"{name}.json").exists() for name, *_ in all_tiles)
    print(f"Downloading New York City's roads in {len(all_tiles)} tiles "
          f"({done} already downloaded, {PARALLEL_DOWNLOADS} at a time) ...")
    with ThreadPoolExecutor(max_workers=PARALLEL_DOWNLOADS) as pool:
        downloads = pool.map(lambda tile: download_tile(tile[0], *tile[1]), enumerate(all_tiles))
        for number, elements in enumerate(downloads, start=1):
            for element in elements:
                if element["type"] == "node":
                    coordinates[element["id"]] = (element["lat"], element["lon"])
                elif element["type"] == "way" and element.get("tags", {}).get("highway") in DEFAULT_SPEED_MPH:
                    ways[element["id"]] = {"nodes": element["nodes"], "tags": element["tags"]}  # roads crossing
            if number % 10 == 0 or number == len(all_tiles):                                   # tiles: once
                print(f"  {number}/{len(all_tiles)} tiles: {len(ways):,} roads, {len(coordinates):,} points",
                      flush=True)
    if not ways:
        raise SystemExit("No roads downloaded: the New York City area was not found in OpenStreetMap.")

    edges = build_edges(list(ways.values()), coordinates)
    keep = largest_connected_part(edges)
    arrays, kept_edges = pack(edges, keep, coordinates)
    save(OUTPUT_FILE, "New York City (5 boroughs)", arrays)

    kilometers = sum(e[2] for e in kept_edges) / 1000
    print(f"\nIntersections: {len(keep):,}   road edges: {len(kept_edges):,}   "
          f"lanes-of-travel: {kilometers:,.0f} km")
    print(f"Wrote {OUTPUT_FILE} ({OUTPUT_FILE.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
