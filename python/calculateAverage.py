import sys
import multiprocessing as mp

MEASUREMENT_FILE = "data/measurements.txt"
CHUNK_SIZE = 2 * 1024 * 1024
CORE_MULTIPLIER = 2
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

def worker(input_queue, shared_dict, lock):
    while True:
        chunk = input_queue.get()
        if chunk is None:
            break
        
        local_dict = {}
        
        for line in chunk:
            key, value = line.split(";")
            value = float(value)
            if key not in local_dict:
                local_dict[key] = Station(key)
            local_dict[key].observe(value)
        
        with lock:
            for key, station in local_dict.items():
                if key not in shared_dict:
                    shared_dict[key] = station
                else:
                    shared_dict[key].combine(station)

def produce_chunks(input_queue, filename, chunk_size):
    for chunk in read_file_in_chunks(filename, chunk_size):
        input_queue.put(chunk)

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')

    input_queue = mp.Queue()
    with mp.Manager() as manager:
        shared_dict = manager.dict()
        lock = manager.Lock()

        processes = [mp.Process(target=worker, args=(input_queue, shared_dict, lock)) for _ in range(N_CORES)]
        for p in processes:
            p.start()

        producer = mp.Process(target=produce_chunks, args=(input_queue, MEASUREMENT_FILE, CHUNK_SIZE))
        producer.start()

        producer.join()

        for _ in range(N_CORES):
            input_queue.put(None)

        for p in processes:
            p.join()

        results = [
            f"{station.name};{station.min_value};{station.sum_value / station.count:.1f};{station.max_value}"
            for station in sorted(shared_dict.values(), key=lambda s: s.name)
        ]
        print("{" + ",".join(results) + "}")
