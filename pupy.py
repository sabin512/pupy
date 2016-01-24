#!/usr/bin/python3
import sys
import os
import pathlib
import lzma
import hashlib
import json

OUTPUT_PATH = 'store'
REGISTRY_FILE = os.path.join(OUTPUT_PATH, 'registry.json')
STORED_FILE_PATTERN = '%s.xz'

def read_file_chunks(file_reader, chunksize=8192):
    with file_reader as f:
        chunk = f.read(chunksize)
        while chunk:
            yield chunk
            chunk = f.read(chunksize)

def write_file(file_writer, bytes_generator):
    with file_writer as f:
        for chunkbytes in bytes_generator:
            f.write(chunkbytes)
            print('#',end='',flush=True)
        print()

def decompress(input_file, output_file):
    print('Extracting %s ' % output_file, end='')
    bytes_generator = read_file_chunks(lzma.open(input_file, 'rb'))
    file_writer = open(output_file, 'wb')
    write_file(file_writer, bytes_generator) 

def compress(input_file, output_file):
    print('Compressing %s ' % input_file, end='')
    bytes_generator = read_file_chunks(open(input_file, 'rb'))
    file_writer = lzma.open(output_file, 'wb')
    write_file(file_writer, bytes_generator)

def compute_sha1(input_file):
    print('Computing SHA1 for %s...' % input_file)
    hasher = hashlib.sha1()
    bytes_generator = read_file_chunks(open(input_file, 'rb'), chunksize=65536)
    for chunkbytes in bytes_generator:
        hasher.update(chunkbytes)
    return hasher.hexdigest()

def get_registry():
    if not pathlib.Path(REGISTRY_FILE).exists():
        return list()

    with open(REGISTRY_FILE, 'r') as f:
        registry = json.loads(f.read())
    return registry

def register_file(registry, orig_name, orig_sha1, stored_name, stored_sha1):
    print('Registering %s...' % orig_name)
    file_entry = dict()
    file_entry['orig_name'] = orig_name
    file_entry['orig_sha1'] = orig_sha1
    file_entry['stored_name'] = stored_name
    file_entry['stored_sha1'] = stored_sha1
    registry.append(file_entry)

def flush_registry(registry):
    with open(REGISTRY_FILE, 'w') as f:
        f.write(json.dumps(registry))

def get_file_entry(registry, orig_name):
    for file_entry in registry:
        if file_entry['orig_name'] == orig_name:
            return file_entry
    return None

def add_to_store(input_file):
    orig_sha1 = compute_sha1(input_file)
    print('Original SHA1 %s' %  orig_sha1)
    registry = get_registry()

    existing_entry = get_file_entry(registry, input_file)
    if existing_entry:
        existing_sha1 = existing_entry['orig_sha1']
        if orig_sha1 == existing_sha1:
            print('File %s is already stored' % input_file)
            return
        else:
            print('Something is wrong, file is already stored with SHA1: %s' % existing_sha1)
            return

    print('Storing file %s' % input_file)
    stored_filename = STORED_FILE_PATTERN % input_file
    stored_file = os.path.join(OUTPUT_PATH, stored_filename)
    compress(input_file, stored_file)
    stored_sha1 = compute_sha1(stored_file)
    print('Stored SHA1 %s' % stored_sha1)
    register_file(registry, input_file, orig_sha1, stored_filename, stored_sha1) 
    flush_registry(registry)

def main(argv):
    if len(argv) == 1:
        print('Usage %s <input file>' % argv[0])
        sys.exit(1)

    add_to_store(argv[1])

if __name__ == '__main__':
    main(sys.argv)
