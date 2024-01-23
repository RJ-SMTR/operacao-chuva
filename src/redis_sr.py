import pickle
from typing import Union
import dill
import redis
from redis_pal.exceptions import SerializationError, DeserializationError


class RedisSR(redis.Redis):

    def __repr__(self) -> str:
        """
        Representation of this object
        """

        return "<RedisSR <pool={}>>".format(self.connection_pool)

    def __str__(self) -> str:
        """
        Representation of this object
        """

        return self.__repr__()

    @classmethod
    def _serialize(cls, o: object) -> bytes:
        try:
            return pickle.dumps(o)
        except Exception:
            try:
                return dill.dumps(o)
            except:
                raise SerializationError(
                    "Failed to serialize object {} of type {}".format(o, type(o)))

    @classmethod
    def _deserialize(cls, e: Union[str, int, float, bytes]) -> object:
        if e is None:
            return None
        try:
            return pickle.loads(e)
        except:
            try:
                return dill.loads(e)
            except:
                raise DeserializationError(
                    "Failed to deserialize {}".format(e))

    def set(self, key, value, *args, **kwargs) -> bool:
        _ser = self._serialize(value)
        _b = super(RedisSR, self).set(
            name=key, value=_ser, *args, **kwargs
        )
        return all([_b])

    def get(self, key, *args, **kwargs) -> object:
        return self._deserialize(super(RedisSR, self).get(name=key, *args, **kwargs))
    
    def keys(self):
        keys = super(RedisSR, self).keys()
        return list(map(lambda x : x.decode('utf8'), keys))