import os

class DataBoxGlobal:
    def __init__(self):
        self.PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))

    def get_relative_path(self, path):
        return os.path.join(self.PROJECT_ROOT, path)


if __name__ == '__main__':
    g = DataBoxGlobal()
    print(g.get_relative_path('12.xml'))