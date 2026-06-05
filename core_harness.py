import threading
import time
from typing import Dict, Any, Optional

class ThreadSafeRingBuffer:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = [None] * capacity
        self.head = 0
        self.tail = 0
        self.size = 0
        self._lock = threading.Lock()

    def enqueue(self, telemetry_packet: Dict[str, Any]) -> bool:
        with self._lock:
            self.buffer[self.tail] = telemetry_packet
            if self.size == self.capacity:
                self.head = (self.head + 1) % self.capacity
            else:
                self.size += 1
            self.tail = (self.tail + 1) % self.capacity
            return True

    def dequeue(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self.size == 0:
                return None
            packet = self.buffer[self.head]
            self.buffer[self.head] = None
            self.head = (self.head + 1) % self.capacity
            self.size -= 1
            return packet

    def clear_active_stream(self):
        with self._lock:
            self.buffer = [None] * self.capacity
            self.head = 0
            self.tail = 0
            self.size = 0

class SimulationWorkerThread(threading.Thread):
    def __init__(self, thread_id: int, cell_simulator: Any, shared_buffer: ThreadSafeRingBuffer):
        super().__init__()
        self.thread_id = thread_id
        self.simulator = cell_simulator
        self.buffer = shared_buffer
        self.is_active = False
        self.daemon = True

    def run(self):
        self.is_active = True
        target_time_slice_ms = 0.01
        
        while self.is_active:
            loop_start = time.perf_counter()
            telemetry_packet = self.simulator.compute_next_simulation_tick(
                external_stimulus_mA=0.0, 
                dt_ms=target_time_slice_ms
            )
            self.buffer.enqueue(telemetry_packet)
            elapsed = time.perf_counter() - loop_start
            sleep_needed = max(0.0, (1.0 / 112.0) - elapsed)
            if sleep_needed > 0:
                time.sleep(sleep_needed)

    def request_graceful_shutdown(self):
        self.is_active = False