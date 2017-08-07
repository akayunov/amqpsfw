class SfwException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return str(type(self)) + ': ' + str(self.code) + ' - ' + self.msg
