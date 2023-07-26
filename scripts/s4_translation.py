import os
import threading
import plac
from pathlib import Path
import socket


def batch_translation(sentences_src, src, tgt, output_file):

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        host = '10.2.56.190'
        if (src, tgt) == ('en', 'zh'):
            port = 29016
        elif (src, tgt) == ('zh', 'en'):
            port = 10177
        elif (src, tgt) == ('en', 'ms'):
            port = 28018
        elif (src, tgt) == ('ms', 'en'):
            port = 28017
        elif (src, tgt) == ('en', 'ta'):
            port = 28020
        elif (src, tgt) == ('ta', 'en'):
            port = 28019
        elif (src, tgt) == ('en', 'vi'):
            port = 28124
        elif (src, tgt) == ('vi', 'en'):
            port = 28123
        else:
            return

        s.connect((host, port))

        batch_size = 30
        sentences_tgt = []

        with open(output_file, "a", encoding="utf8") as f_out:
            for i in range(0, len(sentences_src), batch_size):

                batch_sentences_src = sentences_src[i:i+batch_size]
                batch_sentences_tgt = ""

                s.sendall(bytes('\n'.join(batch_sentences_src)+'\n', encoding='utf8'))
                while batch_sentences_tgt.count("\n") != len(batch_sentences_src):
                    batch_sentences_tgt += s.recv(4096).decode('UTF-8')
                batch_sentences_tgt = batch_sentences_tgt.strip().split('\n')
                
                sentences_tgt.extend(batch_sentences_tgt)
                print('{}/{}'.format(i, len(sentences_src)), flush=True)

                assert len(batch_sentences_src) == len(
                    batch_sentences_tgt), 'length of src and target do not match'

                batch_ssentence_pairs = zip(batch_sentences_src, batch_sentences_tgt)

                for sentence_pair in batch_ssentence_pairs:
                    f_out.write("{} ||| {}\n".format(sentence_pair[0].replace("|", " "),
                                                    sentence_pair[1].replace("|", " ")))
                    
    print('finished {}, {} sentences in total.'.format(
        output_file, len(sentences_tgt)), flush=True)
    return sentences_tgt


def translation(input_file, output_path, src, dest):

    output_path_dir = os.path.dirname(output_path)
    if not os.path.exists(output_path_dir):
        os.makedirs(output_path_dir)

    sentences_src = [line.strip() for line in open(
        input_file, encoding="utf8").readlines()]

    for tgt in dest:

        output_file = os.path.splitext(output_path)[0] + '.' + src+'2'+tgt
        if not os.path.exists(output_file):
            threads = []
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[:25000], src, tgt, output_file+'1')))
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[25000+15120:50000], src, tgt, output_file+'2')))
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[50000+6540:75000], src, tgt, output_file+'3')))
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[75000:100000], src, tgt, output_file+'4')))
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[100000:125000], src, tgt, output_file+'5')))
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[125000+3390:150000], src, tgt, output_file+'6')))
            threads.append(threading.Thread(target=batch_translation, args=(sentences_src[150000+22050:175000], src, tgt, output_file+'7')))
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[175000+14640:200000], src, tgt, output_file+'8')))
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[200000:225000], src, tgt, output_file+'88')))
            # threads.append(threading.Thread(target=batch_translation, args=(sentences_src[225000:], src, tgt, output_file+'9')))

            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            # sentences_tgt = batch_translation(sentences_src[580:], src, tgt, output_file)

        else:
            continue


@ plac.pos('input_path', "Src File/dir", type=Path)
@ plac.pos('output_path', "Tgt File/dir", type=Path)
@ plac.pos('src', "source language", type=str)
def main(input_path="/home/xuancong/airflow/data/zh_newsmarket/zh_newsmarket.sent",
         output_path="/home/xuancong/airflow/data/zh_newsmarket/zh_newsmarket.sent",
         src='zh'):

    os.chdir(os.path.dirname(__file__))

    if os.path.isfile(input_path):
        input_file = str(input_path)
        output_path = str(output_path)
        if src in ['en']:
            translation(input_file, output_path, src, ['zh', 'ms', 'ta', 'vi'])
        if src in ['zh', 'ms', 'ta', 'vi']:
            translation(input_file, output_path, src, ['en'])

    elif os.path.isdir(input_path):
        # threads = []
        for rootdir, dirs, files in os.walk(input_path):
            src = rootdir[-2:]
            files.sort(reverse=True)
            for file in files:
                if file.endswith('.sent'):
                    input_file = os.path.join(rootdir, file)
                    output_file = os.path.join(rootdir.replace(
                        str(input_path), str(output_path)), file)

                    if src in ['en']:
                        translation(input_file, output_path, src,
                                    ['zh', 'ms', 'ta', 'vi'])
                        # threads.append(threading.Thread(target=translation, args=(
                        #     input_file, output_file, src, ['zh', 'ms', 'ta', 'vi'])))

                    elif src in ['zh', 'ms', 'ta', 'vi']:
                        translation(input_file, output_path, src, ['en'])
                        # threads.append(threading.Thread(target=translation, args=(
                        #     input_file, output_file, src, ['en'])))

        # for thread in threads:
        #     thread.start()
        # for thread in threads:
        #     thread.join()

        print('finished all threads', flush=True)

    else:
        print("invalid input_file", flush=True)


if __name__ == "__main__":
    plac.call(main)
