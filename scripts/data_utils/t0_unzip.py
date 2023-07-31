import zipfile
import os


rootdir = '/home/xuanlong/dataclean/data'

for root, dirs, files in os.walk(rootdir):
    for file in files:
        if root.split(r'/')[-1] in ['ccaligned', 'ccmatrix', 'wikimatrix', 'wikimedia', 'wikimedia_v1', 'wikimedia_v20210402']:
            if os.path.splitext(file)[1] == '.zip':
                zip_file = os.path.join(root, file)
                with zipfile.ZipFile(zip_file, 'r') as zip_fin:
                    zip_fin.extractall(root)
                print(zip_file)
