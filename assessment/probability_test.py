import json
import re


def date_time_format(payload):
    text_payload = json.dumps(payload)
    iso_format = re.compile(r"([0-3][0-9]{3}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9](?:[+-][0-1]["
                            r"0-9](?::[0-9]{2})?|Z)?)")

    time_format = re.compile(r"(?:^|\s)((?:[0-1][0-9]|2[1-3]):[0-5][0-9]:[0-5][0-9])(?:$|\s)", re.IGNORECASE)
    date_format = re.compile(r"(?:^|\s)([0-2][0-9]{3}-[01][0-9]-[0-3][0-9])(?:$|\s)", re.IGNORECASE)

    err_time_format = re.compile(r'(?:^|\s)([0-5]?[0-9]:[0-5][0-9])(?:$|\s)')
    err_date_format = re.compile(r"(?:^|\s)((?:[0-3][0-9])?[0-9]{2}[./][01]?[0-9][./][0-3]?[0-9]|"
                                 r"[0-9]{2}[-./][01]?[0-9][-./][0-3]?[0-9]|"
                                 r"[0-3]?[0-9][-./][0-1]?[0-9][-./](?:[0-3]?[0-9])?[0-9]{2})(?:$|\s)",
                                 re.IGNORECASE)

    correct = 0
    wrong = 0

    iso_dates = re.findall(iso_format, text_payload)

    correct_dates = re.findall(date_format, text_payload)

    correct_times = re.findall(time_format, text_payload)

    wrong_dates = re.findall(err_date_format, text_payload)

    wrong_times = re.findall(err_time_format, text_payload)

    correct += len(iso_dates) + len(correct_dates) + len(correct_times)
    wrong += len(wrong_times) + len(wrong_dates)

    return correct + wrong, correct


#TODO: number format
#TODO: utf8 compliance
