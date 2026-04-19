from typing import Any, List
import sys
import os

sys.path.append(os.path.abspath("../ex0"))

from data_processor import (
    DataProcessor,
    NumericProcessor,
    TextProcessor,
    LogProcessor,
)

class DataStream:
    def __init__(self):
        self.processors: List[DataProcessor] = []
        self.stats = {}

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)
        self.stats[proc] = 0

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            handled = False

            for proc in self.processors:
                if proc.validate(element):
                    try:
                        proc.ingest(element)

                        if isinstance(element, list):
                            self.stats[proc] += len(element)
                        else:
                            self.stats[proc] += 1

                        handled = True
                        break
                    except Exception:
                        pass

            if not handled:
                print(f"DataStream error - Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self.processors:
            print("No processor found, no data")
            return

        for proc in self.processors:
            total = self.stats.get(proc, 0)
            remaining = len(proc._data)

            name = proc.__class__.__name__.replace("Processor", " Processor")
            print(f"{name}: total {total} items processed, remaining {remaining} on processor")

if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")

    print("Initialize Data Stream...")
    ds = DataStream()
    ds.print_processors_stats()

    print("Registering Numeric Processor")
    num = NumericProcessor()
    ds.register_processor(num)

    stream = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING", "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {stream}")
    ds.process_stream(stream)

    ds.print_processors_stats()

    print("Registering other data processors")
    txt = TextProcessor()
    log = LogProcessor()

    ds.register_processor(txt)
    ds.register_processor(log)

    print("Send the same batch again")
    ds.process_stream(stream)

    ds.print_processors_stats()

    print("Consume some elements from the data processors: Numeric 3, Text 2, Log 1")

    for _ in range(3):
        num.output()

    for _ in range(2):
        txt.output()

    for _ in range(1):
        log.output()

    ds.print_processors_stats()
