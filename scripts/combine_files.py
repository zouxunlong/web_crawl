import os
import datetime


def inject_from_file(file, output_path):

    output_file_dir = os.path.dirname(output_path)
    if not os.path.exists(output_file_dir):
        os.makedirs(output_file_dir)

    with open(file, encoding='utf8') as f_in, open(output_path, 'a', encoding='utf8') as f_out:
        for i, sentence in enumerate(f_in):
            f_out.write(sentence)


def combine_files_in_dir(rootdir):

    file_combined = 0

    for root, dirs, files in os.walk(rootdir):
        files.sort()
        for file in files:
            m_time = os.path.getmtime(os.path.join(root, file))
            dt_m = datetime.datetime.fromtimestamp(m_time)
            s_time = datetime.datetime.strptime("2023-05-18", "%Y-%m-%d")
            if dt_m > s_time and file.endswith('.vi2en'):
            #     print('Modified on:', dt_m)
                inject_from_file(os.path.join(root, file),
                                os.path.join(rootdir, 'combined.vi2en'))
                file_combined += 1

    print("Done. {} file combined".format(file_combined))


def count(file):
    n=0
    with open(file,'r',encoding='utf8') as f_in:
        for line in f_in:
            n+=1
    print(n)

if __name__=="__main__":
    # count('/home/xuancong/airflow/data/stage4/combined.en2vi')

    rootdir = '/home/xuancong/airflow/data/stage4'
    combine_files_in_dir(rootdir)
