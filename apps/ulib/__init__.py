import json
from flask import request


def obj_to_dict(item, force=False):
    """[Convert from class to Dict(). The idea is the same as bean in java]

    Arguments:
        item {[class]} -- [any type of class have __dict__ readable]

    Returns:
        [dict] -- [contain all method self.* of class]
    """
    obj = dict()
    for key in item.__dict__.keys():
        try:
            if isinstance(item.__getattribute__(key), str) or isinstance(item.__getattribute__(key), int) or isinstance(item.__getattribute__(key), float) or isinstance(item.__getattribute__(key), dict) or isinstance(item.__getattribute__(key), list):
                obj[key] = item.__getattribute__(key)
            elif item.__getattribute__(key) is None:
                obj[key] = None
            elif force:
                obj[key] = str(item.__getattribute__(key))
        except:
            continue
    return obj


def dict_to_obj(obj, your_dict: dict, list_key: list):
    for key in list_key:
        try:
            type(obj).__dict__[key].__set__(obj, your_dict[key])
        except:
            continue
    return obj
