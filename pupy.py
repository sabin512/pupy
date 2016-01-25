#!/usr/bin/python3
import sys
import os
import lzma
import hashlib
import registry

OUTPUT_PATH = 'store'
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

def add_to_store(input_file):
    orig_sha1 = compute_sha1(input_file)
    print('Original SHA1 %s' %  orig_sha1)
    reg = registry.Registry(OUTPUT_PATH) 
    reg.load()
    existing_sha1 = reg.get_orig_sha1(input_file)

    if orig_sha1 == existing_sha1:
        print('File %s is already stored' % input_file)
        return
    elif existing_sha1:
        print('Something is wrong, file is already stored with SHA1: %s' % existing_sha1)
        return

    print('Storing file %s' % input_file)
    stored_filename = STORED_FILE_PATTERN % input_file
    stored_file = os.path.join(OUTPUT_PATH, stored_filename)
    compress(input_file, stored_file)
    stored_sha1 = compute_sha1(stored_file)
    print('Stored SHA1 %s' % stored_sha1)
    reg.add_file_entry(input_file, orig_sha1, stored_filename, stored_sha1) 
    reg.save()

def main(argv):
    if len(argv) == 1:
        print('Usage %s <input file>' % argv[0])
        sys.exit(1)

    add_to_store(argv[1])

if __name__ == '__main__':
    main(sys.argv)
