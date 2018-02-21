import redis

class LogRedis:
    def __init__(self, ip, port, service_name, col_name):
        self.col_name = col_name
        self.relation_redis_db = redis.Redis(host=ip, port=port, db=0)
        self.data_redis = redis.Redis(host=ip, port=port, db=service_name)
        self.data_pipeline = self.data_redis.pipeline()

    def update_detail_value(self, pk, new_value):
        self.data_pipeline.hset(self.col_name + "_detail", pk, new_value)

    def get_detail_value(self, pk):
        return self.data_redis.hget(self.col_name + '_detail', pk)

    # redis中 db num 表示service name; HASH表示表名
    def increase_frequency(self, pk):
        self.data_pipeline.hincrby(self.col_name, pk, 1)

    def submit(self):
        self.data_pipeline.execute()
