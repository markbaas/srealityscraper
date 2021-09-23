from pathlib import Path

from scrapy.exporters import BaseItemExporter
from scrapy.utils.python import to_bytes
from scrapy.utils.serialize import ScrapyJSONEncoder


class SplittedJsonItemExporter(BaseItemExporter):
    """
    A modified version of JsonItemExporter to allow outputting to splitted files.
    """

    def __init__(self, output_path, batch_size=1000, **kwargs):
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

    def _beautify_newline(self):
        if self.indent is not None:
            self.file.write(b"\n")

    def start_exporting_batch(self, item):
        self.file = open(self.output_path / (str(item["_id"]) + ".json"), "wb")
        self.file.write(b"[")

    def finish_exporting_batch(self):
        self._beautify_newline()
        self.file.write(b"]")

        self.file.close()
        self.items_written = 0

    def export_item(self, item):
        if self.items_written == 0:
            self.start_exporting_batch(item)
        else:
            self.file.write(b",")

        self._beautify_newline()

        itemdict = dict(self._get_serialized_fields(item))
        data = self.encoder.encode(itemdict)
        
        self.file.write(to_bytes(data, self.encoding))
        self._beautify_newline()

        self.items_written += 1

        if self.items_written == self.batch_size:
            self.finish_exporting_batch()
