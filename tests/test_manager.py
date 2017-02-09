from .base import BaseTest
from tgext.relationships.manager import RelationshipsManager


class TestFollowing(BaseTest):
    @classmethod
    def setupClass(cls):
        super(TestFollowing, cls).setupClass()
        cls.manager = RelationshipsManager(cls.redis, 'tgext.relationship.tests', 'follow')

    def test_follow(self):
        self.manager.add(1, 2)
        assert self.manager.is_ongoing(1, 2)
        assert not self.manager.is_ongoing(2, 1)
        assert not self.manager.is_reciprocal(1, 2)

    def test_reciprocal(self):
        self.manager.add(1, 2)
        assert self.manager.is_ongoing(1, 2)

        self.manager.add(2, 1)
        assert self.manager.is_ongoing(2, 1)

        assert self.manager.is_reciprocal(1, 2)
        assert self.manager.is_reciprocal(2, 1)


class TestFollowingWithAcceptance(BaseTest):
    @classmethod
    def setupClass(cls):
        super(TestFollowingWithAcceptance, cls).setupClass()
        cls.manager = RelationshipsManager(cls.redis, 'tgext.relationship.tests', 'follow',
                                           requires_acceptance=True)

    def test_follow_not_accepted(self):
        self.manager.add(1, 2)
        assert not self.manager.is_ongoing(1, 2)
        assert self.manager.is_pending(1, 2)

    def test_follow_accepted(self):
        self.manager.add(1, 2)
        assert not self.manager.is_ongoing(1, 2)

        self.manager.accept(1, 2)
        assert self.manager.is_ongoing(1, 2)
        assert not self.manager.is_pending(1, 2)

    def test_follow_reciprocal(self):
        self.manager.add(1, 2)
        self.manager.accept(1, 2)
        assert self.manager.is_ongoing(1, 2)
        assert not self.manager.is_reciprocal(1, 2)

        self.manager.add(2, 1)
        self.manager.accept(2, 1)
        assert self.manager.is_ongoing(2, 1)
        assert self.manager.is_reciprocal(1, 2)

    def test_accept_is_follow(self):
        assert not self.manager.is_ongoing(1, 2)

        self.manager.accept(1, 2)
        assert self.manager.is_ongoing(1, 2)
        assert not self.manager.is_pending(1, 2)

    def test_deny_acceptance(self):
        self.manager.add(1, 2)
        assert self.manager.is_pending(1, 2)

        self.manager.deny(1, 2)
        assert not self.manager.is_pending(1, 2)
        assert not self.manager.is_ongoing(1, 2)


class TestFriendship(BaseTest):
    @classmethod
    def setupClass(cls):
        super(TestFriendship, cls).setupClass()
        cls.manager = RelationshipsManager(cls.redis, 'tgext.relationship.tests', 'friendship',
                                           requires_acceptance=True, bidirectional=True)

    def test_friendship(self):
        self.manager.add(1, 2)
        self.manager.accept(1, 2)
        assert self.manager.is_ongoing(1, 2)
        assert self.manager.is_ongoing(2, 1)
        assert self.manager.is_reciprocal(1, 2)