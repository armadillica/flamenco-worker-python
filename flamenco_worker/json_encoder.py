import json

from . import timing


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, timing.Timing):
            return o.to_json_compat()
        return super().default(o)
