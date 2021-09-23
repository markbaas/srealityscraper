from pathlib import Path

from scrapy.exporters import BaseItemExporter
from scrapy.utils.python import to_bytes
from scrapy.utils.serialize import ScrapyJSONEncoder


class SplittedJsonItemExporter(BaseItemExporter):
    """
    A modified version of JsonItemExporter to allow outputting to splitted files.
    """

    def __init__(self, output_path, batch_size=100, **kwargs):
        super().__init__(dont_fail=True, **kwargs)
        self.output_path: Path = Path(output_path)
        self.batch_size = batch_size

        self.output_path.mkdir(parents=True, exist_ok=True)

        # there is a small difference between the behaviour or JsonItemExporter.indent
        # and ScrapyJSONEncoder.indent. ScrapyJSONEncoder.indent=None is needed to prevent
        # the addition of newlines everywhere
        json_indent = (
            self.indent if self.indent is not None and self.indent > 0 else None
        )
        self._kwargs.setdefault("indent", json_indent)
        self._kwargs.setdefault("ensure_ascii", not self.encoding)
        self.encoder = ScrapyJSONEncoder(**self._kwargs)
        self.items_written = 0
        self.file = None
        self.buffer = []

    def finish_exporting_batch(self):
        first_item_id = self.buffer[0]["_id"]
        last_item_id = self.buffer[-1]["_id"]

        with open(self.output_path / f"{first_item_id}_{last_item_id}.json", "w") as f:
            f.write(self.encoder.encode(self.buffer))

        self.buffer = []
        self.items_written = 0

    def finish_exporting(self):
        self.finish_exporting_batch()

    def export_item(self, item):
        self.buffer.append(item)

        self.items_written += 1

        if self.items_written == self.batch_size:
            self.finish_exporting_batch()
