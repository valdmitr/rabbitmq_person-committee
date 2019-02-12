import json


def unpack_json(json_str):
    return json.loads(json_str)




def pack_dict_to_json(json_str, my_dict):
    a = unpack_json(json_str)
    a.update(my_dict)
    return simple_pack(a)


def simple_pack(my_dict):
    """
    :param my_dict: словарь, который мы хотим превратить в json-строку
    :return: возвращает json-строку
    """
    return json.dumps(my_dict)
#
# def write_file():
#     write_file_json();
#
#
# def write_file_xml():
#     pass

def write_file_json(file_name, my_dict, json_str=''):
    a = json.loads(json_str)
    a.update(my_dict)
    with open(file_name, "w") as write_file:
        json.dump(a, write_file)

#
# def send_message()
