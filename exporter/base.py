import time
from functools import lru_cache


class Exporter():
    def __init__(self, config, timestamp=False):
        self.timestamp = time.strftime('%Y%m%d-%H%M%S') if timestamp else None
        self.name = self.__class__.__name__
        self.config = config
        self.folder = f'./export/{self.config.name}'


@lru_cache(maxsize=None)
def base_class_name(obj):
    bases = type.mro(obj.__class__)
    base = [c.__module__.split('.')[-1]
            for c in bases].index('base')
    return bases[base].__name__


class FunExporter(Exporter):
    def export(self, tool, obj, fn, hook, *args):
        fn_name = '_'.join((
            tool.__class__.__name__,
            base_class_name(obj),
            fn,
            hook
        ))
        if hasattr(self, fn_name):
            getattr(self, fn_name)(obj, *args)


class DispatchExporter(Exporter):
    def export(self, tool, obj, fn, hook, *args):
        self.dispatch(tool.__class__.__name__,
                      base_class_name(obj),
                      fn,
                      hook,
                      obj,
                      *args)
