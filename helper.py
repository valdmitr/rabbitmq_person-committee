import json


def pack_dict_to_json(json_str, my_dict):
    a = json.loads(json_str)
    a.update(my_dict)
    return json.dumps(a)


def simple_pack(my_dict):
    return json.dumps(my_dict)