from redis import Redis


class BaseTest(object):
    @classmethod
    def setupClass(cls):
        cls.redis = Redis()

    def setup(self):
        for k in self.redis.keys('tgext.relationship.tests:*'):
            self.redis.delete(k)
