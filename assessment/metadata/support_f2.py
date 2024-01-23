import json

from console_print import c_print, MyColors as Color
from file_manager import file_manager, FileManagerError


def calc_point(value):
    match value:
        case 'M':
            return 1
        case 'R':
            return 0.5
        case 'O':
            return 0.2


def load_point(sup, method="merged"):
    c_print.myprint("LOG: creating property-weight dictionary")
    try:
        properties = file_manager.open_file('properties_settings_ckan', 'json', True, 'set')
        point = {}
        for property_ in properties[sup]:
            if method in property_:
                point[property_["property"]] = calc_point(property_[method])
            else:
                point[property_["property"]] = 0
        return point

    except FileManagerError:
        raise


def load_max_point(sup, method):
    try:
        properties = file_manager.open_file('properties_settings_ckan', 'json', True, 'set')

        point = 0

        for property_ in properties[sup]:
            if method in property_:
                point += calc_point(property_[method])

        return point

    except FileManagerError:
        raise


def load_relation():
    properties = file_manager.open_file('properties_settings_ckan', 'json', True, 'set')

    linked = []
    relation = {}
    for property_ in properties:
        for each in properties[property_]:
            linked.append(each["property"])
        relation[property_] = linked
        linked = []

    return relation


def load_sup_link():
    c_print.myprint("LOG: loading list of properties with sub properties")
    try:
        properties = file_manager.open_file('properties_settings_ckan', 'json', True, 'set')

        linked = []
        for property_ in properties:
            if property_ != "result":
                linked.append(property_)

        return linked

    except FileManagerError:
        raise


def load_metadata_properties_schema(method="merged", m_property="result"):
    c_print.myprint("Loading schema property/weight and sub property")
    try:
        properties = file_manager.open_file('properties_settings_ckan', 'json', True, 'set')

        schema = {}
        for property_ in properties[m_property]:
            if method in property_:
                if property_["property"] not in list(properties.keys()):
                    schema[property_["property"]] = {"point": property_[method], "sub": None}
                else:
                    sub_properties = {}
                    for sub_property in properties[property_["property"]]:
                        if method in sub_property:
                            if sub_property["property"] not in list(properties.keys()):
                                sub_properties[sub_property["property"]] = {"point": sub_property[method], "sub": None}
                            else:
                                sub_sub_properties = {}
                                for sub_sub_property in properties[sub_property["property"]]:
                                    if method in sub_sub_property:
                                        sub_sub_properties[sub_sub_property["property"]] = {
                                            "point": sub_sub_property[method], "sub": None}
                                if sub_sub_properties != {}:
                                    sub_properties[sub_property["property"]] = {"point": sub_property[method],
                                                                                "sub": sub_sub_properties}

                    schema[property_["property"]] = {"point": property_[method],
                                                     "sub": sub_properties}
                    if sub_properties == {}:
                        schema[property_["property"]]["sub"] = None
        return schema

    except FileManagerError:
        raise

def normalizer(my_obj):
    my_obj2 = json.dumps(my_obj)
    my_obj2 = json.loads(my_obj2)
    while type(my_obj2) is list:
        if my_obj2:
            my_obj2 = my_obj2[0]
        else:
            my_obj2 = None

    if type(my_obj2) is not list and type(my_obj2) is not dict:
        return my_obj2

    for _property in my_obj2:
        my_obj2[_property] = normalizer(my_obj2[_property])

    return my_obj2


def find_sub_weight(payload, property_, sub_property, method):
    try:
        print("--> calculating weight of " + sub_property)
        prop_points = load_point(sub_property, method)
        link_in_prop = load_relation()
        metadata_properties = payload["result"]

        max_point = load_max_point(sub_property, method)
        if max_point == 0:
            return 1

        act_point = 0
        for sub_sub_property in link_in_prop[sub_property]:
            if sub_sub_property in metadata_properties[property_][sub_property] \
                    and metadata_properties[property_][sub_property] != {} \
                    and metadata_properties[property_][sub_property] is not None:
                act_point += prop_points[sub_sub_property]

        weight = act_point / max_point

        print("-->LOG: sub_weight = " + str(weight))

        return weight

    except FileManagerError:
        raise


def find_weight(payload, _property, method):
    try:
        print("->LOG: calculating weight of " + str(_property))
        prop_with_sub = load_sup_link()
        prop_points = load_point(_property, method)
        link_in_prop = load_relation()
        if 'result' in payload:
            metadata_properties = payload["result"]
        else:
            metadata_properties = payload

        max_point = round(load_max_point(_property, method), 2)
        if max_point == 0:
            return 1

        act_point = 0

        sub_weight = 1
        for sub_property in link_in_prop[_property]:
            if sub_property in metadata_properties[_property] \
                    and metadata_properties[_property][sub_property] != {} \
                    and metadata_properties[_property][sub_property] is not None:

                if sub_property in prop_with_sub:
                    print("--> LOG: " + sub_property + " seems to have sub -> ")
                    sub_weight = find_sub_weight(payload, _property, sub_property, method)

                act_point += round(sub_weight * prop_points[sub_property], 2)

            else:
                print("-->LOG: " + sub_property + " not found or empty")

        weight = round(sub_weight * (act_point / max_point), 2)

        print("->LOG: weight = " + str(weight))

        return weight

    except FileManagerError:
        raise
