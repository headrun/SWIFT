#!/usr/bin/python

import os, sys, time, optparse

sys.path.append('..')

from vtv_utils import VTV_CONFIG_DIR, VTV_REBOOT_TMP_DIR, VTV_STARTUP_CFG_FILE_NAME, VTV_ALL_RW_PERMISSION, \
                      copy_file, get_compact_traceback, get_startup_cfg_attributes, initialize_logger, \
                      remove_files_based_on_filter, VTV_COMMAND_HISTORY_FILE, VTV_CMD_FILE, VTV_APACHE_LOG

from vtv_xml import   PROCESS_TAG, SERVER_TAG

from vtv_user import vigenere_crypt

import ssh_utils

from globals import Option, get_xml_body, \
                    LOCAL_HOST, \
                    VTV_ARGS_KEY, VTV_CMD_KEY, VTV_SUB_CMD_KEY


MAX_FILES_TO_KEEP = 5

COMMAND_SUCCESS, COMMAND_EXCEPTION, LOCAL_SSH_ERROR, SCP_COMMAND_ERROR, INVALID_COMMAND = range(5)



def update_history(command, status):
    status_str = "Successful"
    if status != 0:
        status_str = "Failed"
    
    fp = open(VTV_COMMAND_HISTORY_FILE, "a")
    try:
        os.chmod(VTV_COMMAND_HISTORY_FILE, VTV_ALL_RW_PERMISSION)
    except:
        pass
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = "%s::%s::%s\n" % (time_str, command, status_str)
    fp.write(line)
    fp.close()

    
def print_command_status(system, command, status, status_str):
    if status == COMMAND_SUCCESS:
        status_str = 'Succeeded'
        str = '<font color="green"><h2> %s %s <h2></font>' % (command, status_str)
    else:
        str = '<font color="red"><h2> Status: %s - %s <h2></font>' % (status, status_str)

    str = str.replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;').replace("'", '&apos;')
    print get_xml_body(str, system)


def run_command(args_table, my_logger):
    system  = args_table['system']
    cmd = args_table.get(VTV_SUB_CMD_KEY, '')
    args_list = args_table.get(VTV_ARGS_KEY, [])

    cur_dir = os.getcwd()

    status = COMMAND_SUCCESS
    status_str = ''

    src = ''
    dst = ''
    password = ''

    new_cmd = cmd
    if cmd == 'commit_config':
        command_str = args_list[0]
        command_str = command_str.replace("::","\n")
        command_str = "config\n%s\ncommit\nquit\n" % command_str
    elif cmd == 'revert_config':
        config_no = args_list[0]
        command_str = '%s %s ;' % (cmd, config_no )
    elif cmd in ["upgrade_image", "upgrade_data", "upgrade_profile", 
                "upgrade_seed", "push_data", "load_config", "save_config"]:
        if len(args_list) <= 0:
            status = INVALID_COMMAND
            status_str =  "%s : invalid arguments" % (cmd)
            return cmd, status, status_str

        command = args_list[0]
        command = vigenere_crypt(command, False)
        command = command.strip()
    
        command_args = command.split()
        if len(command_args) <= 1:
            status = INVALID_COMMAND
            status_str =  "%s : invalid arguments" % (cmd)
            return cmd, status, status_str

        command, src = command_args[:2]
        if len(command_args) > 2:
            password = vigenere_crypt(command_args[2], False)

        time_stamp = time.strftime("%Y%m%dT%H%M%S", time.localtime())

        dst = os.path.join(VTV_REBOOT_TMP_DIR, "%s_%s_%s" % (cmd, time_stamp, os.path.basename(src)))

        new_cmd = '%s %s - %s' % (cmd, src, dst)
        if cmd != 'save_config':
            if src.find("@") != -1:
                status = ssh_utils.scp(password, src, dst)
            else:
                status = copy_file(src, dst)

            if status != COMMAND_SUCCESS:
                status_str = "%s : scp %s to %s failed with error: %d. " % (cmd, src, dst, status)
                status = SCP_COMMAND_ERROR
                return new_cmd, status, status_str

        remove_files_based_on_filter(VTV_REBOOT_TMP_DIR, "%s_*" % cmd, MAX_FILES_TO_KEEP, my_logger)
        command_str = '%s %s' % (cmd, dst)
    elif cmd == 'backout_data':
        if len(args_list) <= 0:
            status = INVALID_COMMAND
            status_str =  "%s : invalid arguments" % (cmd)
            return cmd, status, status_str
        data_id = args_list[0]
        command_str = '%s %s' % (cmd, data_id)
    else:
        command_str = cmd
        
    options = Option()
    os.chdir('..')
    
    server_dir = os.path.join(cur_dir, "..")
    if cmd == "reload_config":
        command = './vtvclid -s'
    else:
        open(VTV_CMD_FILE, 'w').write(command_str)
        command = './vtvclid -r %s' % VTV_CMD_FILE

    command = "cd %s; %s" % (server_dir, command) 
    status, process = ssh_utils.ssh_cmd_output(LOCAL_HOST, options.vtv_user_name, options.vtv_password, command)
    if status:
        status_str = "SSH %s returned error: %s output: %s %s" % (command, status, process.before, process.after)
        status = LOCAL_SSH_ERROR

    os.chdir(cur_dir)

    if status == COMMAND_SUCCESS and cmd == 'save_config':
        if src.find("@") != -1:
            status = ssh_utils.scp(password, dst, src)
        else:
            status = copy_file(dst, src)

        if status != COMMAND_SUCCESS:
            status_str = "%s : scp %s to %s failed with error: %d. " % (new_cmd, dst, src, status)
            status = SCP_COMMAND_ERROR
            return new_cmd, status, status_str

    return new_cmd, status, status_str


def main(args_table):
    cur_dir = os.getcwd()
    my_logger = initialize_logger(VTV_APACHE_LOG)

    attribute_table = get_startup_cfg_attributes(my_logger, VTV_STARTUP_CFG_FILE_NAME)
    system = attribute_table.get('system_type', '')

    try:
        cmd, status, status_str = run_command(args_table, my_logger)
    except:
        cmd = args_table[VTV_CMD_KEY]
        status = COMMAND_EXCEPTION
        e = sys.exc_info()[2]
        status_str = 'Command Exception: %s %s' % (cmd, get_compact_traceback(e))

    os.chdir(cur_dir)

    update_history(cmd, status)
    print_command_status(system, cmd, status, status_str)


if __name__ == '__main__':
    try:
        parser = optparse.OptionParser()
        parser.add_option('', '--command', default='home',  metavar='command', help='command')
        parser.add_option('', '--server', default='127.0.0.1',  metavar='server', help='list of server')
        parser.add_option('', '--process', default='vtvsysd',  metavar='process', help='list of process')

        options, args = parser.parse_args(sys.argv[1:])

        args_table = {}
        args_table[SERVER_TAG] = options.server
        args_table[PROCESS_TAG] = options.process
        args_table[VTV_ARGS_KEY] = args
        print args_table

        main(args_table)
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
    except Exception, e:
        print "exception in main: %s" % get_compact_traceback(e)
        sys.exit(1)
