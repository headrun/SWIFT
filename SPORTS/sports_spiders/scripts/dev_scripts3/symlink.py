import os
import ssh_utils
import sys
from subprocess import Popen, PIPE
import glob
from vtv_utils import execute_shell_cmd

USERNAME = 'veveo'
PASSWORD = 'veveo123'

DEFAULT_PATH = '/home/veveo/release/server/cronjobs/*.py'



class Symlink:

    def __init__(self):
        self.source_ip      = '10.4.18.34'
        self.destination_ip = '10.4.2.197'
        self.symlink_path    = '/home/veveo/headrun/jhansi/symlink_files'
        self.symlink_cmd = 'ln -s /home/veveo/release/server/cronjob_runner.py /home/veveo/release/server/cronjobs/%s'

    def main(self):
        #status = ssh_utils.scp('veveo123', 'veveo@%s:%s' % (self.destination_ip, DEFAULT_PATH), self.symlink_path)
        os.chdir(self.symlink_path)
        files = glob.iglob('*.py')

        for new_file in files:
            cmd = self.symlink_cmd % new_file
            execute_shell_cmd(cmd)


if __name__ == '__main__':
    OBJ = Symlink()
    OBJ.main()
