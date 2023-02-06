import os
import subprocess


if __name__ == "__main__":

    filelist = os.listdir('blocks')
    for file in filelist:
        path = 'blocks' + '/' + file
        subprocess.run(["python", path])