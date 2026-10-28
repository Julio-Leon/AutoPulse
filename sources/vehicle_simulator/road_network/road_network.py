"""
AutoPulse - the street map the simulated vehicles drive on, and route finding.

The map is a graph: VERTICES are intersections (and road ends), EDGES are the
stretches of road between them, in the direction traffic may drive (a two-way
street is two edges, a one-way street one). Each edge has its length, speed
limit and shape (the points along the road, so a curved road is drawn curved).

It's built once by build_road_network.py from OpenStreetMap and saved as a
compact binary file (nyc_roads.bin.gz). For all of New York City that is
tens of thousands of intersections; kept as packed number arrays (Python's
`array` module) instead of Python objects it takes ~15 MB of memory, which
fits on the 1 GB streaming host.

    network = RoadNetwork.load("sources/vehicle_simulator/road_network/nyc_roads.bin.gz")
    route = network.fastest_route(start_vertex, goal_vertex)        # list of edges
    points, speed_limits, intersections = network.route_geometry(route)

File layout (gzip): one line of JSON (format, area, array lengths), then the
arrays below as raw little-endian bytes, in this order. Edges are stored
grouped by start vertex ("compressed sparse row"): the edges leaving vertex v
are edge_start[v] .. edge_start[v+1]-1. Coordinates are stored as integers in
millionths of a degree (~0.1 m precision).
"""

import gzip
import heapq
import json
import math
import sys
from array import array

FORMAT = "autopulse-road-network-1"

ARRAYS = [                      # (name, array typecode: i = int32, I = uint32, f = float32, B = uint8)
    ("vertex_lat_e6", "i"),     # per vertex
    ("vertex_lon_e6", "i"),
    ("edge_start", "I"),        # per vertex + 1: where its outgoing edges start
    ("edge_target", "I"),       # per edge: the vertex it leads to
    ("edge_length_m", "f"),
    ("edge_speed_mph", "B"),    # speed limit
    ("shape_start", "I"),       # per edge + 1: where its shape points start
    ("shape_lat_e6", "i"),      # shape points (between the two vertices, in driving order)
    ("shape_lon_e6", "i"),
]

EARTH_RADIUS_M = 6_371_000
MPH_TO_METERS_PER_SECOND = 0.44704


def distance_m(lat1, lon1, lat2, lon2):
    """Distance in meters between two points (flat-earth approximation, exact enough within a city)."""
    x = math.radians(lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    y = math.radians(lat2 - lat1)
    return EARTH_RADIUS_M * math.hypot(x, y)


# --------------------------------------------------
# Saving and loading
# --------------------------------------------------

def save(path, area, arrays):
    """Write the arrays (dict name -> array) to a gzip file with a JSON header."""
    header = {"format": FORMAT, "area": area, "lengths": {name: len(arrays[name]) for name, _ in ARRAYS}}
    with gzip.open(path, "wb", compresslevel=9) as out:
        out.write(json.dumps(header).encode() + b"\n")
        for name, _ in ARRAYS:
            values = arrays[name]
            if sys.byteorder == "big":
                values = array(values.typecode, values)
                values.byteswap()
            out.write(values.tobytes())


class RoadNetwork:
    def __init__(self, area, arrays):
        self.area = area
        for name, _ in ARRAYS:
            setattr(self, name, arrays[name])
        self.vertex_count = len(self.vertex_lat_e6)
        self.edge_count = len(self.edge_target)
        self.max_speed_mps = max(self.edge_speed_mph) * MPH_TO_METERS_PER_SECOND

    @classmethod
    def load(cls, path):
        with gzip.open(path, "rb") as source:
            data = source.read()
        header_end = data.index(b"\n")
        header = json.loads(data[:header_end])
        if header.get("format") != FORMAT:
            raise ValueError(f"{path} is not an AutoPulse road network file")

        arrays, position = {}, header_end + 1
        for name, typecode in ARRAYS:
            values = array(typecode)
            size = header["lengths"][name] * values.itemsize
            values.frombytes(data[position:position + size])
            if sys.byteorder == "big":
                values.byteswap()
            arrays[name] = values
            position += size
        return cls(header["area"], arrays)

    # ----------------------------------------------
    # Vertices
    # ----------------------------------------------

    def position(self, vertex):
        """(latitude, longitude) of a vertex."""
        return self.vertex_lat_e6[vertex] / 1e6, self.vertex_lon_e6[vertex] / 1e6

    def distance_between(self, vertex_a, vertex_b):
        return distance_m(*self.position(vertex_a), *self.position(vertex_b))

    def random_vertex(self, rng):
        return rng.randrange(self.vertex_count)

    # ----------------------------------------------
    # Routing
    # ----------------------------------------------

    def fastest_route(self, start, goal):
        """
        The quickest legal route from start to goal (one-way streets respected),
        as a list of edge numbers; None if there is none. A* search: explores
        roads in order of (time so far + straight-line time still needed).
        """
        if start == goal:
            return []
        goal_lat, goal_lon = self.position(goal)

        def time_left(vertex):
            return distance_m(*self.position(vertex), goal_lat, goal_lon) / self.max_speed_mps

        best_time = {start: 0.0}
        arrived_by = {}                       # vertex -> (edge used to reach it, vertex it came from)
        queue = [(time_left(start), start)]
        while queue:
            estimate, vertex = heapq.heappop(queue)
            if estimate > best_time[vertex] + time_left(vertex) + 1e-9:
                continue                      # an outdated queue entry: a faster way was found since
            if vertex == goal:
                route = []
                while vertex != start:
                    edge, vertex = arrived_by[vertex]
                    route.append(edge)
                return route[::-1]
            time_here = best_time[vertex]
            for edge in range(self.edge_start[vertex], self.edge_start[vertex + 1]):
                neighbor = self.edge_target[edge]
                time_there = time_here + self.edge_length_m[edge] / (
                    self.edge_speed_mph[edge] * MPH_TO_METERS_PER_SECOND)
                if time_there < best_time.get(neighbor, math.inf):
                    best_time[neighbor] = time_there
                    arrived_by[neighbor] = (edge, vertex)
                    heapq.heappush(queue, (time_there + time_left(neighbor), neighbor))
        return None

    def route_geometry(self, route, start):
        """
        The route as points to drive through:
          points          [[lat, lon], ...] from start to goal, following the road shapes
          speed_limits    speed limit (mph) of each stretch between two consecutive points
          intersections   indexes in `points` that are intersections (where lights may be red)
        """
        points = [list(self.position(start))]
        speed_limits, intersections = [], []
        for edge in route:
            limit = self.edge_speed_mph[edge]
            for shape in range(self.shape_start[edge], self.shape_start[edge + 1]):
                points.append([self.shape_lat_e6[shape] / 1e6, self.shape_lon_e6[shape] / 1e6])
                speed_limits.append(limit)
            points.append(list(self.position(self.edge_target[edge])))
            speed_limits.append(limit)
            intersections.append(len(points) - 1)
        if intersections:
            intersections.pop()               # the last one is the destination itself
        return points, speed_limits, intersections
