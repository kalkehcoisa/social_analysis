import json
import zstandard as zstd

def peek_fields(filepath):
    dctx = zstd.ZstdDecompressor()
    with open(filepath, "rb") as fh:
        with dctx.stream_reader(fh) as reader:
            first_line = b""
            while b"\n" not in first_line:
                first_line += reader.read(4096)
            first_line = first_line.split(b"\n")[0]
    obj = json.loads(first_line)
    for k, v in obj.items():
        print(f"{k}: {type(v).__name__} = {repr(v)[:80]}")

peek_fields("dataset/RC_2019-04.zst")
peek_fields("dataset/RS_2019-04.zst")
