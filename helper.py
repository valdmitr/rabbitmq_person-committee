import json

def unpack_str(string):
    """
    распаковываем данные из нужного формата
    :param string: строка, которую нужно преобразовать
    :return: результат выполнения функции unpack_json
    """
    return unpack_json(string)


def unpack_json(json_str):
    """
    :param json_str: json-строка, которую мы хотим преобразовать в словарь
    :return: словарь
    """
    return json.loads(json_str)


def pack_to_str(my_dict):
    """
    пакуем данные в нужный формат
    :param my_dict: аргумент, который нужно преобразовать в строку
    :return: результат выполнения функции pack_json
    """
    return pack_json(my_dict)


def pack_json(my_dict):
    """
    :param my_dict: словарь, который мы хотим превратить в json-строку
    :return: json-строку
    """
    return json.dumps(my_dict)


def append_smth(string, smth):
    """
    добавляем что-нибудь в нашу строку
    :param string: строка, в которую нужно добавить данные
    :param smth: что нужно добавить
    :return: результат выполнения функции append_dict_to_json
    """
    return append_dict_to_json(string, smth)


def append_dict_to_json(json_str, my_dict):
    """
    добавляем данные словаря в уже существующий json
    :param json_str: json-строка, в которую добавляем/обновляем данные
    :param my_dict: словарь, который нужно добавить
    :return: новая json-строка
    """
    a = unpack_str(json_str)
    a.update(my_dict)
    return pack_to_str(a)


def write_file_json(file_name, my_dict, json_str=''):
    a = json.loads(json_str)
    a.update(my_dict)
    with open(file_name, "w") as write_file:
        json.dump(a, write_file)

#
# def send_message()
