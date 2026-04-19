from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    def __init__(self):
        self._data = []
        self._counter = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise Exception("No data available")
        value = self._data.pop(0)
        rank = self._counter
        self._counter += 1
        return rank, value

class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")

        if isinstance(data, list):
            for x in data:
                self._data.append(str(x))
        else:
            self._data.append(str(data))

class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")

        if isinstance(data, list):
            self._data.extend(data)
        else:
            self._data.append(data)

class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        def is_valid_log(d):
            return isinstance(d, dict) and all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in d.items()
            )

        if is_valid_log(data):
            return True
        if isinstance(data, list):
            return all(is_valid_log(x) for x in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")

        def format_log(d):
            return f"{d.get('log_level')}: {d.get('log_message')}"

        if isinstance(data, list):
            for d in data:
                self._data.append(format_log(d))
        else:
            self._data.append(format_log(data))


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")

    # ---- Numeric ----
    print("Testing Numeric Processor...")
    num = NumericProcessor()

    print(f"Trying to validate input '42': {num.validate(42)}")
    print(f"Trying to validate input 'Hello': {num.validate('Hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num.ingest("foo")
    except Exception as e:
        print(f"Got exception: {e}")

    print("Processing data: [1, 2, 3, 4, 5]")
    num.ingest([1, 2, 3, 4, 5])

    print("Extracting 3 values...")
    for _ in range(3):
        rank, value = num.output()
        print(f"Numeric value {rank}: {value}")

    print("Testing Text Processor...")
    txt = TextProcessor()

    print(f"Trying to validate input '42': {txt.validate(42)}")

    print("Processing data: ['Hello', 'Nexus', 'World']")
    txt.ingest(['Hello', 'Nexus', 'World'])

    print("Extracting 1 value...")
    rank, value = txt.output()
    print(f"Text value {rank}: {value}")

    print("Testing Log Processor...")
    log = LogProcessor()

    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")

    logs = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]

    print(f"Processing data: {logs}")
    log.ingest(logs)

    print("Extracting 2 values...")
    for _ in range(2):
        rank, value = log.output()
        print(f"Log entry {rank}: {value}")