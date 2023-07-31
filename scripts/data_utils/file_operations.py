import os


def inject_from_file(file, output_filepath):
    with open(file, encoding='utf8') as fIN, open(output_filepath, 'a', encoding='utf8') as fOUT:
        for i, sentence in enumerate(fIN):
            if sentence.strip():
                fOUT.write(sentence.strip()+'\n')


def files_combine(rootdir):

    file_combined=0

    for root, dirs, files in os.walk(rootdir):
        for file in files:
            if file.endswith('.en-ta') or file.endswith('.EN-TA'):
                inject_from_file(os.path.join(root, file), str(rootdir)+'_combined.en-ta')
                file_combined+=1
            if file.endswith('.en-ms') or file.endswith('.EN-MS'):
                inject_from_file(os.path.join(root, file), str(rootdir)+'_combined.en-ms')
                file_combined+=1
            if file.endswith('.en-zh') or file.endswith('.EN-ZH'):
                inject_from_file(os.path.join(root, file), str(rootdir)+'_combined.en-zh')
                file_combined+=1

    print("Done. {} file combined".format(file_combined),flush=True)

# -------------------------------------------------------------

def split_file_by_lang(file, output_file1, output_file2):
    with open(file, encoding='utf8') as f_in, \
            open(output_file1, 'w', encoding='utf8') as f_out1, \
            open(output_file2, 'w', encoding='utf8') as f_out2:
        for line in f_in:
            sentences = line.split('|||')
            if len(sentences) != 2:
                return
            f_out1.write(sentences[0].strip()+'\n')
            f_out2.write(sentences[1].strip()+'\n')


def files_split(rootdir):

    file_splited = 0

    for root, dirs, files in os.walk(rootdir):
        for file in files:
            if file.endswith('.en-ta') or file.endswith('.EN-TA'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.ta')
                file_splited += 1
            if file.endswith('.en-ms') or file.endswith('.EN-MS'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.ms')
                file_splited += 1
            if file.endswith('.en-zh') or file.endswith('.EN-ZH'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.en', file+'.zh')
                file_splited += 1
            if file.endswith('.ta-en') or file.endswith('.TA-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.ta', file+'.en')
                file_splited += 1
            if file.endswith('.ms-en') or file.endswith('.MS-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.ms', file+'.en')
                file_splited += 1
            if file.endswith('.zh-en') or file.endswith('.ZH-EN'):
                file = os.path.join(root, file)
                split_file_by_lang(file, file+'.zh', file+'.en')
                file_splited += 1
        break
    print("Done. {} file splited".format(file_splited))


def count(file):
    n=0
    with open(file,'r',encoding='utf8') as f_in:
        for line in f_in:
            n+=1
    print(n)



def count_files(rootdir):

    file_counts = 0

    for root, dirs, files in os.walk(rootdir):
        for file in files:
            file_counts+=1
    print("total {} files.".format(file_counts))


if __name__=="__main__":
    count_files('/home/xuanlong/dataclean/data/stage5')