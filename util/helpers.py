class MaxFinder():
    def __init__(self, desc='Maximum value', unit=None, err=1e-7):
        self.unit = ' (%s)' % unit if unit else ''
        self.desc = desc
        self.times = []
        self.value = None
        self.err = err

    def check(self, value, t):
        if self.value is not None and abs(self.value - value) < self.err:
            self.times.append(t) 
        elif self.value is None or value > self.value:
            self.value = value
            self.times = [t]

    def to(self, fn, unit = None):
        ret = MaxFinder(self.desc, unit)
        ret.value = fn(self.value)
        ret.times = self.times[:]
        return ret

    def __repr__(self):
        return '%s: %s%s at %s' % (self.desc, self.value, self.unit, self.times)


def list_str(iterable, formater='{:g}', sep=','):
    return sep.join(map(formater.format, iterable))
