import os
import subprocess


if __name__ == "__main__":

    filelist = os.listdir('flows')

    for file in filelist:
        path = 'flows' + '/' + file
        subprocess.run(["python", path])