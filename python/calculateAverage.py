import sys
import multiprocessing as mp

MEASUREMENT_FILE = "data/measurements.txt"
MEASUREMENT_COUNT = 1_000_000_000
CORE_MULTIPLIER = 4
CHUNK_MULTIPLIER = 4
N_CORES = mp.cpu_count() * CORE_MULTIPLIER

class Station:
    def __init__(self, name):
        self.name = name
        self.count = 0
        self.min_value = float("inf")
        self.sum_value = 0
        self.max_value = float("-inf")

    def observe(self, value):
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)
        self.sum_value += value
        self.count += 1

    def combine(self, other):
        self.min_value = min(self.min_value, other.min_value)
        self.max_value = max(self.max_value, other.max_value)
        self.sum_value += other.sum_value
        self.count += other.count

def read_file_in_chunks(filename, chunk_size):
    with open(filename, "rt", encoding="utf8") as file:
        while chunk := file.readlines(chunk_size):
            yield chunk

def worker(input_queue, output_queue):
    stations = {}
    while chunk := input_queue.get():
        for line in chunk:
            key, value = line.split(";")
            value = float(value)
            if key not in stations:
                stations[key] = Station(key)
            stations[key].observe(value)
    for station in stations.values():
        output_queue.put(station)
    output_queue.put(None)

def collector(output_queue):
    stations = {}
    finished_workers = 0
    while finished_workers < N_CORES:
        station = output_queue.get()
        if station is None:
            finished_workers += 1
        else:
            if station.name not in stations:
                stations[station.name] = station
            else:
                stations[station.name].combine(station)
    return stations

def produce_chunks(input_queue, filename, chunk_size):
    for chunk in read_file_in_chunks(filename, chunk_size):
        input_queue.put(chunk)

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')

    chunk_size = MEASUREMENT_COUNT // (N_CORES * CHUNK_MULTIPLIER)
    input_queue = mp.Queue()
    output_queue = mp.Queue()

    processes = [mp.Process(target=worker, args=(input_queue, output_queue)) for _ in range(N_CORES)]
    for p in processes:
        p.start()

    producer = mp.Process(target=produce_chunks, args=(input_queue, MEASUREMENT_FILE, chunk_size))
    producer.start()
    producer.join()

    for _ in range(N_CORES):
        input_queue.put(None)

    stations = collector(output_queue)
    for p in processes:
        p.join()

    results = [
        f"{station.name};{station.min_value};{station.sum_value / station.count:.1f};{station.max_value}"
        for station in sorted(stations.values(), key=lambda s: s.name)
    ]
    print("{" + ",".join(results) + "}")
