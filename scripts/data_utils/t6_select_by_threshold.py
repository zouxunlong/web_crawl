import time


def select(file_in, file_out1, file_out2, thre):
    start_time = time.time()
    n = 0
    k = 0
    with open(file_in, encoding='utf8') as f_in, \
        open(file_out1, 'w', encoding='utf8') as f_out1, \
            open(file_out2, 'w', encoding='utf8') as f_out2:
        for i, line in enumerate(f_in):
            sentences = line.split('|||')
            if len(sentences) != 3:
                continue
            if float(sentences[0].strip()) > thre:
                f_out1.write(line)
                n += 1
            else:
                f_out2.write(line)
                k += 1

    print("total {}".format(i))
    print("select {}".format(n))
    print("filter {}".format(k))
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':

    select('/home/xuanlong/dataclean/cleaning/data/V4.en-th',
           '/home/xuanlong/dataclean/cleaning/data/V4_selected.en-th',
           '/home/xuanlong/dataclean/cleaning/data/V4_filtered.en-th',
           0.625)
