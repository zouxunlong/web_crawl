import os
import time
import plac
from pathlib import Path


def split_file_by_lang(file, output_file1, output_file2):
    with open(file, encoding='utf8') as f_in, \
            open(output_file1, 'w', encoding='utf8') as f_out1, \
            open(output_file2, 'w', encoding='utf8') as f_out2:
        for i, line in enumerate(f_in):
            sentences = line.split('|||')
            if len(sentences) != 2:
                return
            f_out1.write(sentences[0].strip()+'\n')
            f_out2.write(sentences[1].strip()+'\n')


@plac.opt('rootdir', "Src Input File", type=Path)
def main(rootdir):

    file_splited = 0

    for root, dirs, files in os.walk(rootdir):
        for file in files:
            if file.endswith('.en-ta') or file.endswith('.EN-TA'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.ta')
                file_splited += 1

            if file.endswith('.ta-en') or file.endswith('.TA-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.ta', file+'.en')
                file_splited += 1

            if file.endswith('.en-ms') or file.endswith('.EN-MS'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.ms')
                file_splited += 1

            if file.endswith('.ms-en') or file.endswith('.MS-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.ms', file+'.en')
                file_splited += 1

            if file.endswith('.en-zh') or file.endswith('.EN-ZH'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.zh')
                file_splited += 1

            if file.endswith('.zh-en') or file.endswith('.ZH-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.zh', file+'.en')
                file_splited += 1

            if file.endswith('.en-id') or file.endswith('.EN-ID'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.id')
                file_splited += 1

            if file.endswith('.id-en') or file.endswith('.ID-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.id', file+'.en')
                file_splited += 1

            if file.endswith('.en-vi') or file.endswith('.EN-VI'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.vi')
                file_splited += 1
                
            if file.endswith('.vi-en') or file.endswith('.VI-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.vi', file+'.en')
                file_splited += 1

            if file.endswith('.en-th') or file.endswith('.EN-TH'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.th')
                file_splited += 1
                
            if file.endswith('.th-en') or file.endswith('.TH-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.th', file+'.en')
                file_splited += 1

    print("Done. {} file splited".format(file_splited))


if __name__ == '__main__':
    main('/home/xuanlong/dataclean/cleaning/data')
