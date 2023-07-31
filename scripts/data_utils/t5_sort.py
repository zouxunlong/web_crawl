import time


def sort(file_path):
    start_time = time.time()
    with open(file_path) as f_in:
        list = f_in.readlines()
        list.sort(reverse=True)
    with open(file_path, 'w', encoding='utf8') as f_out:
        for sentence in list:
            f_out.write(sentence)
    list.clear()
    print("finished " + file_path)
    print("--- {} seconds ---".format(time.time() - start_time))


if __name__ == '__main__':

    sort('/home/xuanlong/dataclean/data.t4.en-id')
