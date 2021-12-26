


def usage_demo_loop(path_to_dir):
    for path, subdirs, files in os.walk(path_to_dir):
        for name in files:
            try:
                short_name, extention = os.path.splitext(name)
                if extention == '.wav':
                    fullWav = path + '/' + name
                    fullMp3name = path + '/' + short_name + '.mp3'
                    AudioSegment.from_wav(fullWav).export(fullMp3name, format="mp3")
                    do_aws_transcription(file_name=fullMp3name)
                else :
                    if extention == '.mp3':
                        do_aws_transcription(path + '/' + name)
            except Exception as e:
                print("error: {}".format(e))
                continue


if __name__ == '__main__':
    import argparse

    try:
        parser = argparse.ArgumentParser(description=
                                         "This script transcribes all mp3 files in given working dir (wd)")

        parser.add_argument('-wd', help='specify root directory from which to run script', required=True)

        args = vars(parser.parse_args())
        usage_demo_loop(args['wd'])

    except Exception as e:
        logging.error("error occured: {}".format(e))