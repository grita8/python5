import sys
import os

sys.path.append(os.path.abspath("../ex1"))
sys.path.append(os.path.abspath("../ex0"))

from data_stream import DataStream
from data_processor import NumericProcessor, TextProcessor, LogProcessor

class CSVExportPlugin:
    def process_output(self, data):
        print("CSV Output:")
        values = []
        for _, v in data:
            values.append(v)
        print(",".join(values))

class JSONExportPlugin:
    def process_output(self, data):
        print("JSON Output:")
        result = []
        for i, (rank, value) in enumerate(data):
            result.append(f'"item_{rank}": "{value}"')
        print("{" + ", ".join(result) + "}")

def output_pipeline(self, nb, plugin):
    for proc in self.processors:
        collected = []

        for _ in range(nb):
            try:
                collected.append(proc.output())
            except:
                break

        if collected:
            plugin.process_output(collected)

DataStream.output_pipeline = output_pipeline

if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")

    ds = DataStream()

    print("Initialize Data Stream...")
    ds.print_processors_stats()

    print("Registering Processors")

    ds.register_processor(NumericProcessor())
    ds.register_processor(TextProcessor())
    ds.register_processor(LogProcessor())

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

    print("Send 3 processed data from each processor to a CSV plugin:")
    ds.output_pipeline(3, CSVExportPlugin())

    ds.print_processors_stats()

    stream2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {"log_level": "NOTICE", "log_message": "Certificate expires in 10 days"},
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]

    print(f"Send another batch of data: {stream2}")
    ds.process_stream(stream2)

    ds.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")
    ds.output_pipeline(5, JSONExportPlugin())

    ds.print_processors_stats()