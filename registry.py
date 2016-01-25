import os
import pathlib
import json

REGISTRY_FILENAME = 'registry.json' 

class Registry:
    def __init__(self, store_path):
        self.store_path = store_path
        self.registry_file = os.path.join(store_path, REGISTRY_FILENAME)
    
    def load(self):
        if not pathlib.Path(self.registry_file).exists():
            print('%s not found, creating new registry...' % self.registry_file)
            self.data = list()
            return
        with open(self.registry_file, 'r') as f:
            self.data = json.loads(f.read())

    def save(self):
        print('Saving %s...' % self.registry_file)
        with open(self.registry_file, 'w') as f:
            f.write(json.dumps(self.data))

    def get_file_entry(self, orig_name):
        for file_entry in self.data:
            if file_entry['orig_name'] == orig_name:
                return file_entry
        return None 

    def get_orig_sha1(self, orig_name):
        existing_entry = self.get_file_entry(orig_name)
        if not existing_entry:
            return None
        return existing_entry['orig_sha1']

    def add_file_entry(self, orig_name, orig_sha1, stored_name, stored_sha1):
        print('Registering %s...' % orig_name)
        file_entry = dict()
        file_entry['orig_name'] = orig_name
        file_entry['orig_sha1'] = orig_sha1
        file_entry['stored_name'] = stored_name
        file_entry['stored_sha1'] = stored_sha1
        self.data.append(file_entry)
