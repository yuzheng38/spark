#!/usr/bin/env python3

def default_parser(line, unique_params_indices, numeric_params_indices, DEFAULTS):
    key = line[-2:]
    values = line[:-2]

    for i in unique_params_indices:
        if values[i] == DEFAULTS[i]:
            values[i] = None
    for i in numeric_params_indices:
        # try parsing
        float_val = 0

        try:
            float_val = float(values[i])
        except ValueError:
            float_val = float('nan')

        if values[i] == DEFAULTS[i]:
            try:
                float_val = float(DEFAULTS[i])
            except ValueError:
                pass

        values[i] = float_val

    return key + values

def categorical_parser(line, unique_params_indices):
    key = line[:2]
    values = line[2:]

    values_uniq = [values[ui] for ui in unique_params_indices]
    return key + values_uniq

def unique_fn(values):
    s = set(values)
    return len(s)

def numeric_parser(line, numeric_params_indices):
    key = line[:2]
    values = line[2:]

    values_num = [values[ni] for ni in numeric_params_indices]
    return key + values_num

def count_parser(line, cnt_index):
    key = line[:2]
    values = line[2:]

    cnt = values[cnt_index]
    return key + [cnt]
