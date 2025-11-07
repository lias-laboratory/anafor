
class defaultkeydict(dict):
    def __init__(self, fn):
        self.fn = fn

    def __missing__(self, key):
        value = self[key] = self.fn(key)
        return value
