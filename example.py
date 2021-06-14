class Test:
    def __init__(self):
        pass

    def __call__(self, *args):
        return 42


t1 = Test()
print(t1())
