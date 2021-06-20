import os
import pandas as pd
from progressbar import progressbar
import os

base_folder = 'dataset/iot_23_datasets_full/opt/Malware-Project/BigDataset/IoTScenarios/'
destination_folder = 'dataset/parsed/'
folders = [base_folder + i for i in os.listdir(base_folder)]
extra_file = 'dataset/iot_23_datasets_full/opt/Malware-Project/BigDataset/IoTScenarios/CTU-Honeypot-Capture-7-1/Somfy-01/bro/conn.log.labeled'
files = [i + '/bro/' + os.listdir(i + '/bro')[0] for i in folders if 'Honeypot-Capture-7-1' not in i] + [extra_file]


def count_parsed_files(destination_folder):
    return len(os.listdir(destination_folder))


def get_num_lines(file):
    r = 0
    with open(file, 'r') as file:
        for l in file:
            r += 1
            
    return r


def _parse_line(line):
    return line.replace('\t', ' ').split()


def parse_fields(fields_line):
    return _parse_line(fields_line)[1:]


def parse_types(types_line):
    return _parse_line(types_line)[1:]


def save_part(lines, columns):
    df = pd.DataFrame(lines, columns=columns)
    part_name = destination_folder + f'part_{count_parsed_files(destination_folder) + 1}'
    print('Rotating df; file saved =', part_name)
    df.to_parquet(part_name)


def process_file(file):
    num_lines = get_num_lines(file)
    lines = []
    columns = []
    parts_created = 0
    print('starting processing of file:', file)
    with open(file, 'r') as f:
        for line in progressbar(f, max_value=num_lines, redirect_stdout=True):
            if line.startswith('#fields'):
                columns = parse_fields(line)
            elif not line.startswith('#'):
                line = _parse_line(line)
                lines.append(line)
                if len(lines) > 1000000:
                    save_part(lines, columns)
                    parts_created += 1
                    lines = []
    
    if len(lines) > 0:
        save_part(lines, columns)
        parts_created += 1

    return parts_created


for file in files:
    r = process_file(file)
    print(f'parts created for file: {r}')
