import time
import math


class RelationshipsManager(object):
    DEFAULT_PAGE_SIZE = 15

    def __init__(self, client, namespace, relation_name,
                 requires_acceptance=False, bidirectional=False):
        self._redis = client
        self._namespace = namespace
        self._relation_name = relation_name
        self._requires_acceptance = requires_acceptance
        self._bidirectional = bidirectional

        self._requested_to_key = '%s:%s:%s' % (self._namespace, self._relation_name, 'req_to')
        self._requested_from_key = '%s:%s:%s' % (self._namespace, self._relation_name, 'req_from')
        self._following_key = '%s:%s:%s' % (self._namespace, self._relation_name, 'following')
        self._followers_key = '%s:%s:%s' % (self._namespace, self._relation_name, 'followers')
        self._reciprocal_key = '%s:%s:%s' % (self._namespace, self._relation_name, 'reciprocal')
        self._blocking_key = '%s:%s:%s' % (self._namespace, self._relation_name, 'blocking')
        self._blocked_by_key = '%s:%s:%s' % (self._namespace, self._relation_name, 'blocked_by')

    @classmethod
    def _key(cls, prepend, obj_id):
        return '%s:%s' % (prepend, obj_id)

    @classmethod
    def _now(cls):
        return int(time.time())

    def add(self, from_id, to_id):
        if from_id == to_id:
            return

        if not self.is_allowed(from_id, to_id):
            return

        if self._requires_acceptance:
            if self.is_pending(from_id, to_id):
                return
            else:
                with self._redis.pipeline() as multi:
                    now = self._now()
                    multi.zadd(self._key(self._requested_to_key, from_id), to_id, now)
                    multi.zadd(self._key(self._requested_from_key, to_id), from_id, now)
                    multi.execute()
        else:
            self.add_accepted(from_id, to_id)

    def add_accepted(self, from_id, to_id):
        now = self._now()

        with self._redis.pipeline() as multi:
            multi.zadd(self._key(self._following_key, from_id), to_id, now)
            multi.zadd(self._key(self._followers_key, to_id), from_id, now)
            multi.zrem(self._key(self._requested_to_key, from_id), to_id)
            multi.zrem(self._key(self._requested_from_key, to_id), from_id)

            if self._bidirectional:
                multi.zadd(self._key(self._following_key, to_id), from_id, now)
                multi.zadd(self._key(self._followers_key, from_id), to_id, now)
                multi.zrem(self._key(self._requested_to_key, to_id), from_id)
                multi.zrem(self._key(self._requested_from_key, from_id), to_id)

            multi.execute()

        if self.is_reciprocal(from_id, to_id):
            with self._redis.pipeline() as multi:
                multi.zadd(self._key(self._reciprocal_key, from_id), to_id, now)
                multi.zadd(self._key(self._reciprocal_key, to_id), from_id, now)
                multi.execute()

    def remove(self, from_id, to_id):
        if from_id == to_id:
            return

        with self._redis.pipeline() as multi:
            multi.zrem(self._key(self._following_key, from_id), to_id)
            multi.zrem(self._key(self._followers_key, to_id), from_id)
            multi.zrem(self._key(self._reciprocal_key, from_id), to_id)
            multi.zrem(self._key(self._reciprocal_key, to_id), from_id)
            multi.zrem(self._key(self._requested_from_key, to_id), from_id)
            multi.zrem(self._key(self._requested_to_key, from_id), to_id)
            multi.execute()

    def accept(self, from_id, to_id):
        if from_id == to_id:
            return

        self.add_accepted(from_id, to_id)

    def deny(self, from_id, to_id):
        if from_id == to_id:
            return

        with self._redis.pipeline() as multi:
            multi.zrem(self._key(self._requested_to_key, from_id), to_id)
            multi.zrem(self._key(self._requested_from_key, to_id), from_id)
            multi.execute()

    def block(self, from_id, to_id):
        if from_id == to_id:
            return

        now = self._now()
        with self._redis.pipeline() as multi:
            multi.zrem(self._key(self._following_key, from_id), to_id)
            multi.zrem(self._key(self._following_key, to_id), from_id)
            multi.zrem(self._key(self._followers_key, to_id), from_id)
            multi.zrem(self._key(self._followers_key, from_id), to_id)
            multi.zrem(self._key(self._reciprocal_key, from_id), to_id)
            multi.zrem(self._key(self._reciprocal_key, to_id), from_id)
            multi.zrem(self._key(self._requested_to_key, from_id), to_id)
            multi.zrem(self._key(self._requested_from_key, to_id), from_id)
            multi.zadd(self._key(self._blocking_key, to_id), now, from_id)
            multi.zadd(self._key(self._blocked_by_key, from_id), now, to_id)
            multi.execute()

    def unblock(self, from_id, to_id):
        if from_id == to_id:
            return

        with self._redis.pipeline() as multi:
            multi.zrem(self._key(self._blocked_by_key, from_id), to_id)
            multi.zrem(self._key(self._blocking_key, to_id), from_id)
            multi.execute()

    def is_allowed(self, from_id, to_id):
        return self._redis.zscore(self._key(self._blocking_key, to_id), from_id) is None

    def is_pending(self, from_id, to_id):
        return self._redis.zscore(self._key(self._requested_from_key, to_id), from_id)

    def is_reciprocal(self, from_id, to_id):
        return self.is_ongoing(from_id, to_id) and self.is_ongoing(to_id, from_id)

    def is_ongoing(self, from_id, to_id):
        return self._redis.zscore(self._key(self._following_key, from_id), to_id)

    def _remove_list(self, entity_id, relation_key, backref_relation_key=None):
        if backref_relation_key is not None:
            for related_id in self._redis.zcard(self._key(relation_key, entity_id), 0, -1):
                self._redis.zrem(self._key(backref_relation_key, related_id), entity_id)
        self._redis.delete(self._key(relation_key, entity_id))

    def clear(self, entity_id):
        self._remove_list(entity_id, self._following_key, self._followers_key)
        self._remove_list(entity_id, self._followers_key, self._following_key)
        self._remove_list(entity_id, self._reciprocal_key, self._reciprocal_key)
        self._remove_list(entity_id, self._blocking_key, self._blocked_by_key)
        self._remove_list(entity_id, self._blocked_by_key, self._blocking_key)
        self._remove_list(entity_id, self._requested_from_key, self._requested_to_key)
        self._remove_list(entity_id, self._requested_to_key, self._requested_from_key)

    def related_count(self, to_id):
        """Number of entities related to ``to_id``.

        In case of follow relation those would be the followers.
        """
        return self._count('followers', to_id)

    def relating_count(self, from_id):
        """Number of entities ``from_id`` relates to.

        In case of follow relation those would be the followed entities.
        """
        return self._count('following', from_id)

    def reciprocated_count(self, entity_id):
        """Number of entities that have the reciprocal relation."""
        return self._count('reciprocal', entity_id)

    def requested_from_count(self, to_id):
        return self._count('requested_from', to_id)

    def requested_to_count(self, from_id):
        return self._count('requested_to', from_id)

    def blocked_count(self, entity_id):
        return self._count('blocking', entity_id)

    def _count(self, kind, target):
        keyname = ''.join(('_', kind, '_key'))
        return self._redis.zcard(self._key(getattr(self, keyname), target))

    def related_page_count(self, to_id, page_size=DEFAULT_PAGE_SIZE):
        """Pages of entities related to ``to_id``.

        In case of follow relation those would be the followers.
        """
        return self._page_count('followers', to_id, page_size)

    def relating_page_count(self, from_id, page_size=DEFAULT_PAGE_SIZE):
        """Pages of entities ``from_id`` relates to.

        In case of follow relation those would be the followed entities.
        """
        return self._page_count('following', from_id, page_size)

    def reciprocated_page_count(self, entity_id, page_size=DEFAULT_PAGE_SIZE):
        """Pages of entities that have the reciprocal relation."""
        return self._page_count('reciprocal', entity_id, page_size)

    def requested_from_page_count(self, to_id, page_size=DEFAULT_PAGE_SIZE):
        return self._page_count('requested_from', to_id, page_size)

    def requested_to_page_count(self, from_id, page_size=DEFAULT_PAGE_SIZE):
        return self._page_count('requested_to', from_id, page_size)

    def blocked_page_count(self, entity_id, page_size=DEFAULT_PAGE_SIZE):
        return self._page_count('blocking', entity_id, page_size)

    def _page_count(self, kind, target, page_size):
        return math.ceil(self._count(kind, target) / page_size)

    def _paginate(self, kind, target, page, page_size):
        page = max(page, 1)
        page = min(page, self._page_count(kind, target, page_size))

        redis_index = page - 1
        range_begin_offset = max(redis_index * page_size, 0)
        range_end_offset = range_begin_offset + page_size -1

        keyname = ''.join(('_', kind, '_key'))
        return self._redis.zrevrange(self._key(getattr(self, keyname), target),
                                     range_begin_offset, range_end_offset, withscores=False)

    def related(self, to_id, page=0, page_size=DEFAULT_PAGE_SIZE):
        return self._paginate('followers', to_id, page, page_size)

    def relating(self, from_id, page=0, page_size=DEFAULT_PAGE_SIZE):
        return self._paginate('following', from_id, page, page_size)

    def reciprocated(self, entity_id, page=0, page_size=DEFAULT_PAGE_SIZE):
        return self._paginate('reciprocal', entity_id, page, page_size)

    def requested_from(self, to_id, page=0, page_size=DEFAULT_PAGE_SIZE):
        return self._paginate('requested_from', to_id, page, page_size)

    def requested_to(self, from_id, page=0, page_size=DEFAULT_PAGE_SIZE):
        return self._paginate('requested_to', from_id, page, page_size)

    def blocked(self, entity_id, page=0, page_size=DEFAULT_PAGE_SIZE):
        return self._paginate('blocking', entity_id, page, page_size)
