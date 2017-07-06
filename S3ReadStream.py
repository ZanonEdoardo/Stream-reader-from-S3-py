class S3ReadStreamInner(object):

    def __init__(self, stream):
        self.stream = stream
        self.unused_buffer = b''
        self.closed = False
        self.finished = False

    def read_until_eof(self):
        buf = b""
        while not self.finished:
            raw = self.stream.read(io.DEFAULT_BUFFER_SIZE)
            if len(raw) > 0:
                buf += raw
            else:
                self.finished = True
        return buf

    def read_from_buffer(self, size):
        part = self.unused_buffer[:size]
        self.unused_buffer = self.unused_buffer[size:]
        return part

    def read(self, size=None):
        if not size or size < 0:
            return self.read_from_buffer(
                len(self.unused_buffer)) + self.read_until_eof()

        # Use unused data first
        if len(self.unused_buffer) >= size:
            return self.read_from_buffer(size)

        # If the stream is finished and no unused raw data, return what we have
        if self.stream.closed or self.finished:
            self.finished = True
            return self.read_from_buffer(size)

        # Consume new data in chunks and return it.
        while len(self.unused_buffer) < size:
            raw = self.stream.read(io.DEFAULT_BUFFER_SIZE)
            if len(raw):
                self.unused_buffer += raw
            else:
                self.finished = True
                break

        return self.read_from_buffer(size)

    def readinto(self, b):
        data = self.read(len(b))
        if not data:
            return None
        b[:len(data)] = data
        return len(data)

    def readable(self):
        # io.BufferedReader needs us to appear readable
        return True

    def _checkReadable(self, msg=None):
        # This is required to satisfy io.BufferedReader on Python 2.6.
        return True


class S3ReadStream(io.BufferedReader):

    def __init__(self, key):
        self.stream = S3ReadStreamInner(key)
        super(S3ReadStream, self).__init__(self.stream)

    def read(self, *args, **kwargs):
        result = super(S3ReadStream, self).read(*args, **kwargs)
        if result is None:
            return ""
        return result


    def readline(self, *args, **kwargs):
        # Patch readline to return '' instead of raise Value Error
        try:
            result = super(S3ReadStream, self).readline(*args, **kwargs)
            return result
        except ValueError:
            return ''

    def endOf(self):
        return self.stream.finished

