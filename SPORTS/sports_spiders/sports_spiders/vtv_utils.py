#!/usr/bin/env python

################################################################################
#$Id: vtv_utils.py,v 1.1 2016/03/23 12:50:40 headrun Exp $
# Copyright(c) 2005 Veveo.tv
################################################################################

import sys
import os
import os.path
import string
import re
import time
import logging
import logging.handlers
import errno
import asyncore
import gc
from pathlib import Path
import zipfile
import tarfile
import shutil
import signal
import glob
import stat
import traceback
import types
import pickle
import struct
import datetime
import fcntl
from threading import Thread

# Version dependent code
from sports_spiders.vtv_py import get_md5_hash, MIMEMultipart, MIMEImage, MIMEText

PASSWD_UID_INDEX = 2
PASSWD_GID_INDEX = 3

VTV_OS_NICE_PRIORITY = -5

NUMBER_OF_ATTEMPTS_TO_GET_SCP_CORRECT = 3

VTV_RELEASE_DIR_NAME = "release"
VTV_STARTUP_DIR_NAME = "startup"
VTV_STARTUP_CFG = "startup.cfg"


def guess_startup_dir(cur_path):
    startup_dir = None
    release_dir_index = cur_path.rfind('/%s/' % VTV_RELEASE_DIR_NAME)
    if release_dir_index != -1:
        release_dir = cur_path[:release_dir_index]
        startup_dir = os.path.join(release_dir, VTV_STARTUP_DIR_NAME)
    return startup_dir


def get_startup_cfg_attributes_from_lines(logger, lines):
    attribute_table = {}

    for line in lines:
        if not line or line.startswith('#') or line.find("=") == -1:
            continue
        attribute, value = line.split("=", 1)
        attribute = attribute.strip()
        value = value.strip()
        attribute_table[attribute] = value

    return attribute_table


def get_startup_cfg_attributes_base(logger, file_name):
    try:
        fp = open(file_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        return {}

    lines = fp.readlines()
    attribute_table = get_startup_cfg_attributes_from_lines(logger, lines)
    fp.close()

    return attribute_table


def get_vtv_root_dir_from_startup_cfg():
    vtv_root_dir = None
    cur_path = os.getcwd()
    startup_dir = guess_startup_dir(cur_path)
    if startup_dir:
        attribute_table = get_startup_cfg_attributes_base(
            None, os.path.join(startup_dir, VTV_STARTUP_CFG))
        vtv_root_dir = attribute_table.get('VTV_ROOT_DIR', '')
    return vtv_root_dir


if 'VTV_ROOT_DIR' in os.environ:
    VTV_ROOT_DIR = os.environ['VTV_ROOT_DIR']
else:
    vtv_root_dir_from_startup_cfg = get_vtv_root_dir_from_startup_cfg()
    if vtv_root_dir_from_startup_cfg:
        VTV_ROOT_DIR = vtv_root_dir_from_startup_cfg
    else:
        VTV_ROOT_DIR = str(Path.home())
    os.environ['VTV_ROOT_DIR'] = VTV_ROOT_DIR

VAR_TMP_DIR = "/var/tmp"

VTV_STARTUP_DIR = "%s/%s" % (VTV_ROOT_DIR, VTV_STARTUP_DIR_NAME)
VTV_STARTUP_CFG_FILE_NAME = "%s/%s" % (VTV_STARTUP_DIR, VTV_STARTUP_CFG)
VTV_IMAGE_UPDATE_FILE_NAME = "%s/image.cfg" % VTV_STARTUP_DIR
VTV_RESTART_FILE_NAME = "%s/restart.txt" % VTV_STARTUP_DIR
VTV_UPGRADE_FILE_NAME = "%s/upgrade.txt" % VTV_STARTUP_DIR
VTV_UPGRADE_FILE_NAME_WITH_DATA_ID = "%s/upgrade_%s.txt"
VTV_STARTUP_SH_FILE_NAME = "%s/vtv_startup.sh" % VTV_STARTUP_DIR


def get_startup_cfg_attributes(logger, file_name=VTV_STARTUP_CFG_FILE_NAME):
    return get_startup_cfg_attributes_base(logger, file_name)


VTV_PRODUCT = ""
MYSQL_CLIENT_LIB_PATH = ""
VTV_SUDO_COMMAND = ""
try:
    startup_attribs = get_startup_cfg_attributes(None)
    VTV_PRODUCT = startup_attribs.get('product', "")
    MYSQL_CLIENT_LIB_PATH = startup_attribs.get('mysql_client_lib_path', "")
    VTV_SUDO_COMMAND = startup_attribs.get('sudo_command', "")
except (KeyboardInterrupt, SystemExit):
    raise
except Exception as e:
    pass

if VTV_PRODUCT:
    VTV_PY_EXT = "pyc"
else:
    VTV_PY_EXT = "py"


def _add_path_to_environ(var_name, path):
    current_value = os.environ.get(var_name, '')
    current_value_dirs = current_value.split(':')
    if path not in current_value_dirs:
        if current_value:
            os.environ[var_name] = '%s:%s' % (path, current_value)
        else:
            os.environ[var_name] = path

    return


if MYSQL_CLIENT_LIB_PATH:
    mysql_client_pythonpath = os.path.join(MYSQL_CLIENT_LIB_PATH, 'lib/python')
    if mysql_client_pythonpath not in sys.path:
        # TODO: Check if I need to 'replace' the existing mysqldb path
        sys.path.insert(1, mysql_client_pythonpath)
    for x in os.listdir(mysql_client_pythonpath):
        if x.endswith('.egg'):
            sys.path.insert(1, os.path.join(mysql_client_pythonpath, x))

    _add_path_to_environ('PYTHONPATH', mysql_client_pythonpath)
    _add_path_to_environ('LD_LIBRARY_PATH', os.path.join(
        MYSQL_CLIENT_LIB_PATH, 'lib'))


VTV_ALL_RWX_PERMISSION = stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO
VTV_ALL_RW_PERMISSION = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH
VTV_RESTRICTED_W_PERMISSION = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
VTV_RESTRICTED_X_PERMISSION = stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH

VTV_IMAGE_DIR = "%s/image" % VTV_ROOT_DIR
VTV_IMAGE_TMP_DIR = "%s/image/tmp" % VTV_ROOT_DIR
VTV_PROFILE_DIR = "%s/profile" % VTV_ROOT_DIR
VTV_STATS_DATA_DIR = "%s/stats" % VTV_ROOT_DIR
VTV_STATS_REPORT_DIR = "%s/report" % VTV_STATS_DATA_DIR
VTV_ALARMS_DATA_DIR = "%s/alarms" % VTV_ROOT_DIR

VTV_REPORTS_DIR = "%s/reports" % VTV_ROOT_DIR

VTV_DATAGEN_DIR = "%s/datagen" % VTV_ROOT_DIR
VTV_DATAGEN_CURRENT_DIR = "%s/current" % VTV_DATAGEN_DIR
VTV_DATAGEN_STABLE_DIR = "%s/stable" % VTV_DATAGEN_DIR
VTV_DATAGEN_TGZS_DIR = "%s/tgzs" % VTV_DATAGEN_DIR
VTV_DATAGEN_DAILY_DIR = "%s/daily" % VTV_DATAGEN_DIR
VTV_DATAGEN_WEEKLY_DIR = "%s/weekly" % VTV_DATAGEN_DIR

# This is in upper case because it exists physically in the local machine
VTV_DATAGEN_LOCAL_DIR = "%s/LOCAL" % VTV_DATAGEN_DIR

VTV_ALL_DATA_DIR = "%s/all_data" % VTV_DATAGEN_DIR
VTV_ALL_DATA_CURRENT_DIR = "%s/CURRENT" % VTV_ALL_DATA_DIR
VTV_ALL_DATA_STABLE_DIR = "%s/STABLE" % VTV_ALL_DATA_DIR
VTV_ALL_DATA_TGZS_DIR = "%s/TGZS" % VTV_ALL_DATA_DIR
VTV_ALL_DATA_DAILY_DIR = "%s/DAILY" % VTV_ALL_DATA_DIR
VTV_ALL_DATA_WEEKLY_DIR = "%s/WEEKLY" % VTV_ALL_DATA_DIR

VTV_RUNDATA_DIR = "%s/rundata" % VTV_DATAGEN_DIR

VTV_CONFIG_DIR = "%s/config" % VTV_ROOT_DIR
VTV_CONFIG_FILE_NAME = "%s/vtv.cfg" % VTV_CONFIG_DIR
VTV_CONFIG_XML_FILE_NAME = "%s/vtv.xml" % VTV_CONFIG_DIR

VTV_RELEASE_DIR = "%s/%s" % (VTV_ROOT_DIR, VTV_RELEASE_DIR_NAME)
VTV_ROOT_DATA_DIR = "%s/data" % VTV_RELEASE_DIR
VTV_ROOT_AUX_DATA_DIR = "%s/aux_data" % VTV_ROOT_DATA_DIR
VTV_ROOT_DATAMOVIES_DIR = "%s/dataMovies" % VTV_ROOT_DATA_DIR
VTV_ETC_DIR = "%s/etc" % VTV_RELEASE_DIR
VTV_TEST_DIR = "%s/test" % VTV_RELEASE_DIR
VTV_PID_DIR = "%s/pids" % VTV_RELEASE_DIR
VTV_LOG_DIR = "%s/logs" % VTV_RELEASE_DIR
VTV_CRASH_DIR = "%s/crashes" % VTV_RELEASE_DIR

VTV_SERVER_DIR = "%s/server" % VTV_RELEASE_DIR
VTV_STATS_DIR = "%s/stats" % VTV_SERVER_DIR
VTV_ALARMS_DIR = "%s/alarms" % VTV_SERVER_DIR
VTV_MERGE_DIR = "%s/merge" % VTV_SERVER_DIR
VTV_PUBLISH_DIR = "%s/publish" % VTV_SERVER_DIR
VTV_FOLD_DIR = "%s/fold" % VTV_SERVER_DIR
VTV_WEBAPPS_DIR = "%s/webapps" % VTV_SERVER_DIR
VTV_CGIBIN_DIR = "%s/cgi-bin" % VTV_SERVER_DIR
VTV_HTML_DIR = "%s/html" % VTV_SERVER_DIR
VTV_BACKUP_DIR = "%s/backup" % VTV_ROOT_DIR
VTV_RECO_BIN_DIR = "%s/v360" % VTV_SERVER_DIR
VTV_SPLUNK_DIR = "%s/splunk" % VTV_ROOT_DIR
VTV_SPLUNK_LOG_FILE = "vtv_splunk.log"
VTV_APACHE_LOG = "%s/%s" % (VTV_LOG_DIR, 'vtv_apache.log')

VTV_KRAMER_RELEASE_DATA_DIR = "/data/RELEASE/KRAMER/"

VTV_TRASH_DIR = "%s/.trash" % VTV_ROOT_DIR

VTV_SHELL = "./vtvclid"
VTV_SHELL_NAME = "vtvclid"
VTV_CLID_EXE = "%s/%s" % (VTV_SERVER_DIR, VTV_SHELL_NAME)

VTV_DBUNIVERSE_DIR = "%s/dbuniverse" % VTV_SERVER_DIR
VTV_DBSERVER_DIR = "%s/dbserver" % VTV_DBUNIVERSE_DIR
VTV_DBUNIVERSE_IMPLEMENTATION_DIR = "%s/implementations" % VTV_DBUNIVERSE_DIR
VTV_DBUNIVERSE_SCHEMA_DIR = "%s/schemas" % VTV_DBUNIVERSE_DIR
VTV_DBUNIVERSE_SEED_DIR = "%s/data_files" % VTV_DBUNIVERSE_IMPLEMENTATION_DIR

VTV_CONTENT_DIR = "%s/content" % VTV_ROOT_DIR
VTV_CONTENT_VDB_DIR = "%s/vdb" % VTV_CONTENT_DIR
VTV_CONTENT_VDB_URLS_DIR = "%s/urls" % VTV_CONTENT_VDB_DIR
VTV_CONTENT_VDB_URLS_FILES_DIR = "%s/source_files" % VTV_CONTENT_VDB_URLS_DIR
VTV_CONTENT_UPDATE_DIR = "%s/update" % VTV_CONTENT_DIR
VTV_CONTENT_ARCHIVE_DIR = "%s/archive" % VTV_CONTENT_DIR
VTV_CONTENT_ARCHIVE_VDB_DIR = "%s/vdb" % VTV_CONTENT_ARCHIVE_DIR
VTV_CONTENT_ARCHIVE_VDB_URLS_DIR = "%s/vdb" % VTV_CONTENT_ARCHIVE_VDB_DIR
VTV_CONTENT_ARCHIVE_CONTENTDB_DIR = "%s/contentDb" % VTV_CONTENT_ARCHIVE_DIR
VTV_CONTENT_ARCHIVE_TEST_DIR = "%s/test" % VTV_CONTENT_ARCHIVE_DIR
VTV_CONTENT_ARCHIVE_CONTENTDB_RECO_DIR = "%s/dataReco" % VTV_CONTENT_ARCHIVE_CONTENTDB_DIR
VTV_CONTENT_ARCHIVE_TUNE_LOG_DIR = "%s/tune_logs" % VTV_CONTENT_ARCHIVE_CONTENTDB_DIR
VTV_CONTENT_CLIP_DIR = "%s/dataClips" % VTV_CONTENT_ARCHIVE_CONTENTDB_DIR
VTV_CONTENT_ARCHIVE_TUNE_OUTPUT_DIR = "%s/tune_outputs" % VTV_CONTENT_ARCHIVE_CONTENTDB_DIR
VTV_CONTENT_CLIP_TAG_DIR = "%s/clipTags" % VTV_CONTENT_CLIP_DIR
VTV_CONTENT_BACKUP_DIR = "%s/backup" % VTV_CONTENT_DIR
VTV_CONTENT_GMRF_DIR = "%s/dataGmrf" % VTV_CONTENT_ARCHIVE_CONTENTDB_DIR
VTV_CONTENT_GMRF_DATA_DIR = "%s/data" % VTV_CONTENT_GMRF_DIR
VTV_CONTENT_GMRF_LOGS_DIR = "%s/logs" % VTV_CONTENT_GMRF_DIR
VTV_CONTENT_GMRF_STATUS_DIR = "%s/status" % VTV_CONTENT_GMRF_DIR
VTV_DVR_DUMP_DIR = "dvr_dump"

CONTENT_UPDATE_ZIP_DIR_NAME = "update_zips"
EXTENSION_TGZ_NAME = "tgz"
NO_OF_TEST_TGZ_TO_BE_RETAINED = 10

# files for pull stats
SEARCH_METRICS_FILE = "search_metrics.txt"
VEVEO_METRICS_FILE = "veveo_search_metrics.txt"

VTV_EXCEPTION_FILE = "%s/exception.log" % VTV_CONFIG_DIR
VTV_LOG_FILE = "vtv.log"
VTV_ROTATED_LOG_FILE = "%s.1" % VTV_LOG_FILE
RECENT_LOG_FILE = 'vtv_recent.log'
VTV_SERVER_LOG_FILE = "%s/vtv.log" % VTV_SERVER_DIR
VTV_SERVER_DEBUG_LOG_FILE = "%s/vtv_debug.log" % VTV_SERVER_DIR
VTV_LOG_TMP_DIR = "%s/tmp" % VTV_LOG_DIR

VTV_LATEST_IMAGE_DIR = "%s/latest_image" % VTV_IMAGE_DIR
VTV_PREVIOUS_IMAGE_DIR = "%s/previous_image" % VTV_IMAGE_DIR
VTV_ARCHIVE_IMAGE_DIR = "%s/archive_image" % VTV_IMAGE_DIR
VTV_LATEST_DATA_DIR = "%s/latest_data" % VTV_IMAGE_DIR
VTV_PREVIOUS_DATA_DIR = "%s/previous_data" % VTV_IMAGE_DIR
VTV_ARCHIVE_DATA_DIR = "%s/archive_data" % VTV_IMAGE_DIR
VTV_LATEST_SEED_DIR = "%s/latest_seed" % VTV_IMAGE_DIR
VTV_PREVIOUS_SEED_DIR = "%s/previous_seed" % VTV_IMAGE_DIR
VTV_ARCHIVE_SEED_DIR = "%s/archive_seed" % VTV_IMAGE_DIR

VTV_SERVER_IMAGE = "server.tgz"
VTV_SERVER_DATA = "data.tgz"
VTV_SERVER_DATA_WITH_ID = "data_%s.tgz"
VTV_SERVER_SEED = "seed.tgz"
VTV_SERVER_PREVIOUS_IMAGE = "server_previous.tgz"
VTV_SERVER_PREVIOUS_DATA = "data_previous.tgz"
VTV_SERVER_PREVIOUS_DATA_WITH_ID = "data_previous_%s.tgz"
VTV_SERVER_PREVIOUS_SEED = "seed_previous.tgz"
VTV_SERVER_ARCHIVE_IMAGE = "server_archive.tgz"
VTV_SERVER_ARCHIVE_DATA = "data_archive.tgz"
VTV_SERVER_ARCHIVE_DATA_WITH_ID = "data_archive_%s.tgz"
VTV_SERVER_ARCHIVE_SEED = "seed_archive.tgz"
VTV_SERVER_METADATA = "metadata_%s"
VTV_SERVER_PUSH_DATA = "pushdata.tgz"

DIGEST_FILE_NAME = 'digest_file.txt'

VTV_DATA_VERSION_LOG_FILE_NAME = "contentSanity.log"
VTV_DATA_MANIFEST_FILE_NAME = "data_manifest.txt"

VTV_PLAIN_COMMAND_FILE_NAME = 'vtvclid_commands.txt'
VTV_VTVCLID_COMMAND_FILE_NAME = "%s/%s" % (
    VTV_CONFIG_DIR, VTV_PLAIN_COMMAND_FILE_NAME)
VTV_COMMAND_HISTORY_FILE = "%s/%s" % (VTV_LOG_DIR, 'command_history.txt')
VTV_CMD_FILE = "%s/%s" % (VTV_LOG_DIR, 'command.txt')
VTV_SQL_QUERIES_FILE = "%s/%s" % (VTV_LOG_DIR, 'queries.xml')
VTV_LOGIN_HISTORY_FILE = "%s/%s" % (VTV_LOG_DIR, 'login_history.txt')

VTV_REBOOT_TMP_DIR = '%s/tmp' % VTV_ROOT_DIR
VTV_REBOOT_TMP_SERVER_DIR = '%s/server' % VTV_REBOOT_TMP_DIR
VTV_REBOOT_TMP_DATA_DIR = '%s/data' % VTV_REBOOT_TMP_DIR
VTV_REBOOT_TMP_ETC_DIR = '%s/etc' % VTV_REBOOT_TMP_DIR
VTV_REBOOT_TMP_TEST_DIR = '%s/test' % VTV_REBOOT_TMP_DIR
VTV_PID_TMP_DIR = "%s/pids" % VTV_REBOOT_TMP_DIR

VTV_RELOAD_TMP_DIR = '%s/tmp' % VTV_ROOT_DIR

VTV_TEST_DIR = "%s/test" % VTV_RELEASE_DIR

VTV_DEFAULT_DIR_LIST = [
    VTV_CONFIG_DIR, VTV_PROFILE_DIR, VTV_IMAGE_DIR,
    VTV_ETC_DIR, VTV_LOG_DIR, VTV_PID_DIR, VTV_CRASH_DIR,
    VTV_TEST_DIR, VTV_BACKUP_DIR, VTV_STATS_DATA_DIR,
    VTV_IMAGE_TMP_DIR, VTV_ALARMS_DATA_DIR, VTV_ROOT_AUX_DATA_DIR
]

VTV_EXTERNAL_SERVERS_FILE_LIST = [
    VTV_COMMAND_HISTORY_FILE, VTV_CMD_FILE, VTV_SQL_QUERIES_FILE,
    VTV_LOGIN_HISTORY_FILE, VTV_APACHE_LOG
]

VTV_CV_PACK_DATA_PID_FILE_NAME = "vtv_cv_pack_data.pid"
VTV_GENERATEDATA_PID_FILE_NAME = "generateData.pid"
VTV_RUNDATA_PID_FILE_NAME = "run_data.pid"
RUNDATA_STATUS_FILE_NAME = "%s/run_data_status.txt" % VTV_CONFIG_DIR
VKC_DUMMY_FILE_NAME = "%s/dummy_file.txt" % VTV_CONFIG_DIR
VTV_LAST_PULL_STATS_FILE = "%s/last_pull_stats.txt" % VTV_CONFIG_DIR

PUSH_DATA_TMP_DIR = '%s/pushtmp' % VTV_ROOT_DIR
PUSH_DATA_TMP_RELEASE_DIR = '%s/release' % PUSH_DATA_TMP_DIR
PUSH_DATA_TMP_SERVER_DIR = '%s/server' % PUSH_DATA_TMP_RELEASE_DIR
PUSH_DATA_TMP_DATA_DIR = '%s/data/dataMovies' % PUSH_DATA_TMP_RELEASE_DIR
PUSH_DATA_TMP_ETC_DIR = '%s/etc' % PUSH_DATA_TMP_RELEASE_DIR
PUSH_DATA_DATA_DIR = "data"

DEAD_PUSH_TMP_DIR = '%s/deadtmp' % VTV_ROOT_DIR
PUSH_DATA_DEAD_INCR_DATA_ID = 'deadincr'

PUSH_DATA_PENDING_DEFAULT = "default_no_tag"

VTV_SYSD_STATE_FILE_NAME = "sysd.state"
VTV_NODED_STATE_FILE_NAME = "noded.state"

VTV_PROCESS_OS, VTV_PROCESS_C, VTV_PROCESS_PY = list(range(3))
VTV_PROCESS_EXT_LIST = ["", "", VTV_PY_EXT]

VTV_PROCESS_TYPE_ONE, VTV_PROCESS_TYPE_MIN_ONE, VTV_PROCESS_TYPE_ANY = list(
    range(3))

VTV_PROCESS_SYSTEM_PRIORITY = -2

VTV_DEFAULT_IP = "127.0.0.1"
VTV_LOCAL_IP = "127.0.0.1"
VTV_DEFAULT_NETMASK = "255.255.255.0"
VTV_DEFAULT_PORT = "lo"

# 15 min
SERVER_SNAPSHOT_STATS_TIME_OUT = 900.0

# Daemons, which can run long with not sending heartbeats (aliveness)
NOKEEPALIVE_DAEMON_LIST = ['vtvepgd', 'vtvdbd']

CHECK_DAEMONS_TIME = 3.0
CHECK_DAEMON_COUNT = 21   # restart when no alive received after this count exceeded
# restart nokeepalive processes when no alive received after this count exceeded
CHECK_NOKEEPALIVE_DAEMON_COUNT = 1200
CHECK_PID_TIME = 3.0

ALL_NODES_CHECK_COUNT = 21
ALL_NODES_CHECK_TIME = 5
ALL_NODES_ALIVE_TIME = ALL_NODES_CHECK_COUNT * CHECK_DAEMONS_TIME

# 30 sec
SERVER_CURRENT_STATS_TIME_OUT = 30.0

LOGD_SERVER_PORT = 80
AUTHD_SERVER_PORT = 80
SEARCH_SERVER_PORT = 80
ACTD_SERVER_PORT = 80
AUTHD_NOTIFICATION_PORT = 8300
VTV_AUTHD_CACHE_TIMOUT = 600

SEARCHD_RTU_SERVER_PORT = 8400
RTUD_CRAWL_SERVER_PORT = 80

VTV_DEBUG_SERVER_PORT = 9000

ELECT_MASTER_SERVER_PORT = 8500
SYSD_CLID_SERVER_PORT = 8600
SYSD_NODED_SERVER_PORT = 8700
NODED_PROC_SERVER_PORT = 8800
NODED_HEALTHMON_PORT = 8900

VTVDB_DB_PORT = 9500

DEFAULT_HTTP_LISTENER_PORT = 8777
HTTP_LISTENER_LOG_FILE = 'http_listener.log'
LATEST_VOD_XML_DIR_NAME = "veveoxml"
OBSOLETE_LOG_FILE_NAME = "obsolete.log"
OBSOLETE_VOD_DIR_NAME = "obsoleteVOD"
INCR_VOD_XML_DIR_NAME = "incr_vod"
HTTP_INCR_VOD_XML_DIR_NAME = "http_incr_vod"
VOD_FILE_PREFIX_NAME = "vod_inc"
TUNE_LOGS_DIR_NAME = "tune_logs"
VOD_LOGS_DIR_NAME = "vod_logs"
LOG_PROCESS_PIPELINE_CONFIG = "tune_log_processing_config.txt"
LOG_PROCESS_PIPELINE_INPUT_DIR = "log_processing_input"
TUNE_OUTPUTS_DIR_NAME = "tune_outputs"

ROGERS_DUMP_FILE_AT_SYSD = os.path.join(VTV_ROOT_DIR, 'playlist.txt')

CLEAR_EOL = ''

MAX_MSG_SIZE = 8192

SLEEP_TIME = 3
X_MSG_TYPE = 1

VTV_DATETIME_MAX_TYPE = 4
VTV_MONTH, VTV_DAY, VTV_HOUR, VTV_MINUTE = list(range(VTV_DATETIME_MAX_TYPE))
VTV_DATETIME_MAX_RANGE = 3
VTV_TIME_FROM, VTV_TIME_TO, VTV_TIME_STEP = list(range(VTV_DATETIME_MAX_RANGE))

# upgrade timeouts in min
DEFAULT_UPGRADE_SYSTEM_TIMEOUT = 60
DEFAULT_UPGRADE_PROCESS_TIMEOUT = 10
MIN_UPGRADE_PROCESS_TIMEOUT = 1
MAX_UPGRADE_PROCESS_TIMEOUT = 60
MIN_UPGRADE_SYSTEM_TIMEOUT = 5
MAX_UPGRADE_SYSTEM_TIMEOUT = 240

CHECK_PID_EXISTS_TIME = 10

TIME_UNIT_INVALID = -1

RUNNING, NOT_RUNNING = list(range(2))

VTV_MAX_STATUS_CODE = 2
SUCCESS, FAILURE = list(range(VTV_MAX_STATUS_CODE))
VTV_STATUS_STRINGS = {SUCCESS: "SUCCESS", FAILURE: "FAILURE"}

DATA_VALUE_UNKNOWN_STR = "-"

CRITICAL_STR = "critical"
MAJOR_STR = "major"
MINOR_STR = "minor"
INFO_STR = "info"

ENABLE_IPTABLES_COMMAND = "/sbin/iptables -A INPUT -i %s -p tcp --dport %s -j ACCEPT &> /dev/null"
DISABLE_IPTABLES_COMMAND = "/sbin/iptables -D INPUT -i %s -p tcp --dport %s -j ACCEPT &> /dev/null"
RESTART_IPTABLES = "service iptables restart &> /dev/null"
SAVE_IPTABLES = "service iptables save &> /dev/null"
VTV_IPTABLES_FILE = VTV_CONFIG_DIR + "/iptables.conf"

DATA_DVR_ADDITION_FILE = "DATA_DVR_ADDITIONS_FILE"
DATA_DVR_DELETION_FILE = "DATA_DVR_DELETIONS_FILE"
FIVE_JSON_DIRS = ['asset', 'assetImage', 'category', 'colorScheme', 'package']

CUR8_EVENT = 'cur8'
COLLECTION_EVENT = 'collection'
WATCH_EVENT = 'watch'

IFCONFIG_CMD = '/sbin/ifconfig'

TIMEOUT_OCCURED = 'Timeout Occured'

VTV_SUDO_COMMANDS_FILE = "%s/vtv_sudo_commands.txt" % VTV_CONFIG_DIR
VTV_SUDO_COMMANDS = set()
try:
    if os.access(VTV_SUDO_COMMANDS_FILE, os.F_OK):
        fp = open(VTV_SUDO_COMMANDS_FILE, "r")
        for line in fp:
            VTV_SUDO_COMMANDS.add(line.strip())
        fp.close()
except (KeyboardInterrupt, SystemExit):
    raise
except Exception as e:
    pass

RC_LOCAL_STRING = """
# spawn System and Node Daemons
/bin/bash %s/startup/vtv_startup.sh %s
"""

VTV_STARTUP_SH_STRING = """#!/bin/bash

cur_dir=`pwd`

type=$1
export VTV_ROOT_DIR=%s
cd %s/startup
python vtv_startup.%s  --$type &

cd $cur_dir

exit 0
"""


def get_compact_traceback(e=""):
    except_list = [asyncore.compact_traceback()]
    return "error: %s traceback: %s" % (str(e), str(except_list))


def execute_shell_cmd(cmd, process_logger=None, print_error=1):
    if not process_logger:
        print_error = 0

    try:
        cmd_name = cmd.split(' ')[0]
        if cmd_name in VTV_SUDO_COMMANDS and VTV_SUDO_COMMAND:
            cmd = "%s %s" % (VTV_SUDO_COMMAND, cmd)
        popen_file = os.popen(cmd)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        if print_error:
            process_logger.error("Cannot run %s : %s" %
                                 (cmd, get_compact_traceback(e)))
        return 1, ""

    text = ""
    while True:
        try:
            text += popen_file.read()
            status = popen_file.close()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            if e[0] == errno.EINTR:  # if error is INTR try again reading
                continue
            if print_error:
                process_logger.error(
                    "Cannot read/close %s error: %s" % (cmd, get_compact_traceback(e)))
            return 2, ""

        break

    if print_error and status:
        process_logger.error("%s: failed with %d text: %s stack: %s " % (
            cmd, int(status), text, traceback.format_stack()))

    return status, text


def renice(nice_value, pid, logger):
    execute_shell_cmd("renice -n %s -p %s" % (nice_value, pid), logger)


def run_os_command_with_normal_priority(cmd, logger):
    pid = os.getpid()
    old_nice = os.nice(0)
    renice(0, pid, logger)
    status = os.system(cmd)
    renice(old_nice, pid, logger)
    return status


def timestamped_format():
    return str(time.time()).replace('.', '')


def timestamped_metadata_file(file_name):
    timestamp = str(time.time()).replace('.', '')
    return VTV_SERVER_METADATA % ("%s_%s" % (timestamp, os.path.basename(file_name)))


def save_data_id(data_id, logger):
    if os.path.exists(VTV_STARTUP_CFG_FILE_NAME):
        fp = open(VTV_STARTUP_CFG_FILE_NAME)
        lines = fp.readlines()
        fp.close()
        fw = open(VTV_STARTUP_CFG_FILE_NAME, 'w')
        for line in lines:
            linesplit = line.split('=')
            if len(linesplit) != 2:
                logger.error('Invalid entry %s in %s removing it' %
                             (line, VTV_STARTUP_CFG_FILE_NAME))
                continue
            if linesplit[0] != 'data_id':
                fw.write(line)
            elif linesplit[1].split('\n')[0] != data_id:
                fw.write(line)
        fw.write('data_id=%s\n' % data_id)
        fw.close()
    else:
        logger.error('%s does not exist!' % VTV_STARTUP_CFG_FILE_NAME)


def get_data_id_list(logger):
    data_id_list = []
    if os.path.exists(VTV_STARTUP_CFG_FILE_NAME):
        fp = open(VTV_STARTUP_CFG_FILE_NAME)
        lines = fp.readlines()
        fp.close()
        for line in lines:
            spl = line.split('=')
            if len(spl) != 2:
                continue
            if spl[0] == 'data_id':
                data_id_list.append(spl[1].split('\n')[0])
    else:
        logger.error('%s does not exist' % VTV_STARTUP_CFG_FILE_NAME)
    return data_id_list


def get_sysd_ip_in_monitor(logger=None):
    attribute_table = get_startup_cfg_attributes(logger)
    server_server_interface = attribute_table.get(
        'server_server_interface', '')

    try:
        if_ip_list = get_if_ip_list(string.join(get_dhcpd_if_list()))
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        if_ip_list = []

    if not if_ip_list and server_server_interface:
        if_ip_list = get_if_ip_list(server_server_interface)

    if logger:
        logger.info("In monitor: server_server_interface : %s if ip list: %s" % (
            server_server_interface, if_ip_list))

    if if_ip_list:
        return if_ip_list[0]
    else:
        return VTV_LOCAL_IP


def get_sysd_ip_info_in_server(logger=None):
    attribute_table = get_startup_cfg_attributes(logger)
    server_server_interface = attribute_table.get(
        'server_server_interface', '')
    master_sysd = attribute_table.get('master_sysd', '')
    slave_sysd = attribute_table.get('slave_sysd', '')
    self_ip_list = get_if_ip_list(server_server_interface)
    if self_ip_list:
        self_ip = self_ip_list[0]
    else:
        self_ip = VTV_LOCAL_IP

    return self_ip, master_sysd, slave_sysd


def get_dhcpd_if_list():
    dhcpd_config_file = "/etc/sysconfig/dhcpd"
    text = open(dhcpd_config_file).read()
    dhcpd_regex = re.compile(r"DHCPDARGS=(?P<port>\w+)")

    if_list = []
    for tag_match in dhcpd_regex.finditer(text):
        if_list.append(tag_match.group('port'))

    return if_list


def get_if_ip_list(if_name):
    if not if_name:
        return []
    status, text = execute_shell_cmd('%s %s' % (IFCONFIG_CMD, if_name))
    return get_ip_list(text)


def get_all_ip_list():
    status, text = execute_shell_cmd(IFCONFIG_CMD)
    return get_ip_list(text)


def am_i_a_monitor(logger=None):
    attribute_table = get_startup_cfg_attributes(logger)
    install_type = attribute_table.get('install_type', 'monitor')
    if install_type == 'monitor':
        return True
    return False


def set_logger_log_level(logger, log_level_list):
    if not log_level_list:
        logger.setLevel(logging.INFO)
        return

    log_str = string.join(log_level_list, ":")

    if log_str.find("DEBUG_") != -1:
        logger.setLevel(logging.DEBUG)
        return

    if "INFO" in log_level_list:
        logger.setLevel(logging.INFO)
        return


def set_close_on_exec(fd):
    st = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, st | fcntl.FD_CLOEXEC)


def add_logger_handler(logger, file_name, log_level_list=[]):
    success_cnt = 3
    handler = None
    for i in range(success_cnt):
        try:
            handler = logging.FileHandler(file_name)
            break
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            pass

    if not handler:
        return

    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)d: %(filename)s: %(lineno)d: %(levelname)s: %(message)s', "%Y%m%dT%H%M%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    set_logger_log_level(logger, log_level_list)
    try:
        os.chmod(file_name, VTV_ALL_RW_PERMISSION)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        pass
    if handler.stream:
        set_close_on_exec(handler.stream)


def remove_logger_handler(logger, file_name, log_level_list=[]):
    for handler in logger.handlers:
        if handler.baseFilename == file_name:
            handler.close()
            logger.removeHandler(handler)


def reinitialize_logger(logger, file_name=VTV_LOG_FILE, log_level_list=[]):
    file_name = os.path.abspath(file_name)
    try:
        remove_logger_handler(logger, file_name, log_level_list)
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        e = sys.exc_info()[2]
        time_str = time.strftime("%Y%m%dT%H%M%S", time.localtime())
        exception_str = "%s: %s: exception: %s" % (
            time_str, sys.argv, get_compact_traceback(e))
        open(VTV_EXCEPTION_FILE, "a").write(exception_str)
        print(exception_str)
    try:
        add_logger_handler(logger, file_name, log_level_list)
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        e = sys.exc_info()[2]
        time_str = time.strftime("%Y%m%dT%H%M%S", time.localtime())
        exception_str = "%s: %s: exception: %s" % (
            time_str, sys.argv, get_compact_traceback(e))
        open(VTV_EXCEPTION_FILE, "a").write(exception_str)
        print(exception_str)


def logger_exception(message, logger):
    e = sys.exc_info()[2]
    logger.error("exception: %s %s" % (message, get_compact_traceback(e)))


def initialize_logger(file_name=VTV_LOG_FILE, log_level_list=[]):
    logger = logging.getLogger()
    try:
        add_logger_handler(logger, file_name, log_level_list)
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        e = sys.exc_info()[2]
        time_str = time.strftime("%Y%m%dT%H%M%S", time.localtime())
        exception_str = "%s: %s: exception: %s" % (
            time_str, sys.argv, get_compact_traceback(e))
        open(VTV_EXCEPTION_FILE, "a").write(exception_str)
        print(exception_str)
    return logger


def get_named_logger(name, file_name, log_level_list=[]):
    logger = logging.getLogger(name)
    reinitialize_logger(logger, file_name, log_level_list)
    return logger


def initialize_timed_rotating_logger(file_name, name='', log_level_list=[], format='', when='W0', interval=1, backupCount=5):
    if name:
        logger = logging.getLogger(name)
    else:
        logger = logging.getLogger()
    try:
        for i in range(3):
            try:
                handler = logging.handlers.TimedRotatingFileHandler(
                    file_name, when, interval, backupCount)
                break
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as e:
                print(e)
                pass
        if not format:
            format = '%(asctime)s.%(msecs)d: %(filename)s: %(lineno)d: %(levelname)s: %(message)s'
        formatter = logging.Formatter(format, "%Y%m%dT%H%M%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        set_logger_log_level(logger, log_level_list)
        try:
            os.chmod(file_name, VTV_ALL_RW_PERMISSION)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print(e)
            pass
        if handler.stream:
            set_close_on_exec(handler.stream)
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        e = sys.exc_info()[2]
        time_str = time.strftime("%Y%m%dT%H%M%S", time.localtime())
        exception_str = "%s: %s: exception: %s" % (
            time_str, sys.argv, get_compact_traceback(e))
        open(VTV_EXCEPTION_FILE, "a").write(exception_str)
        print(exception_str)
    return logger


def get_process_count(proc_name, logger, print_error=0):
    status, output = execute_shell_cmd(
        'ps -ef | grep -v grep | grep -c %s' % proc_name, logger, print_error)
    if status == None:
        return int(output)
    else:
        return 0


def check_process_status(proc_name, logger, print_error=0):
    status, output = execute_shell_cmd(
        'pgrep -fl "%s"' % proc_name, logger, print_error)
    if status == None:
        return 1  # process is running
    else:
        return 0  # process is not running


def check_status_given_pid(pid, signum=0):
    if pid <= 0:
        return NOT_RUNNING
    try:
        status, text = execute_shell_cmd("kill -%s %s" % (signum, pid))
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        return NOT_RUNNING

    if status == None:
        return RUNNING
    else:
        return NOT_RUNNING


def kill_process_given_pid_file(pid_file):
    if not os.path.exists(pid_file):
        return False
    try:
        status, text = execute_shell_cmd("pkill -F %s" % pid_file)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        return False
    return True


def check_and_kill_process(pid, proc_name, pid_file, logger, is_stop=False):
    try:
        status = check_status_given_pid(pid, signal.SIGINT)
        if status == RUNNING:
            time.sleep(1)
            status = check_status_given_pid(pid, signal.SIGKILL)

        if is_stop:
            find_and_kill_process(proc_name, logger)

        os.remove(pid_file)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        pass


def find_and_kill_process(proc_name, logger, print_error=0, sleep_time=1):
    status, output = execute_shell_cmd(
        "pgrep -fl %s" % proc_name, logger, print_error)
    if status == None:
        status, output = execute_shell_cmd(
            "pkill -%d -f %s" % (signal.SIGINT, proc_name), logger, print_error)
        time.sleep(sleep_time)
        status, output = execute_shell_cmd(
            "pkill -%d -f %s" % (signal.SIGKILL, proc_name), logger, print_error)
        if status == None:
            logger.info("%s stoped sucessfully\n" % proc_name)
        else:
            if print_error:
                logger.info("Cannot stop %s: status: %d, output: %s\n" %
                            (proc_name, status, output))
        pid_file_name = get_pid_file_for_process(proc_name)
        if os.access(pid_file_name, os.F_OK):
            os.remove(pid_file_name)


def find_and_kill_processes(proc_name_list, logger, print_error=0):
    for proc_name in proc_name_list:
        find_and_kill_process(proc_name, logger)


def find_and_kill_daemon(daemon_name, logger, print_error=0):
    status, output = execute_shell_cmd(
        "service %s status 2>1 " % daemon_name, logger, print_error)
    if status == None:
        status, output = execute_shell_cmd(
            "service %s stop 2>1 " % daemon_name, logger, print_error)
        if status == None:
            logger.info("%s stoped sucessfully\n" % daemon_name)
        else:
            if print_error:
                logger.info("Cannot stop %s: status: %d, output: %s\n" %
                            (daemon_name, status, output))


def find_and_start_daemon(daemon_name, logger, print_error=0, options=''):
    status, output = execute_shell_cmd(
        "service %s status 2>1 " % daemon_name, logger, print_error)
    if status != None:
        old_nice = os.nice(0)
        pid = os.getpid()
        renice(0, pid, logger)
        status, output = execute_shell_cmd("service %s start %s 2>1 " % (
            daemon_name, options), logger, print_error)
        renice(old_nice, pid, logger)
        if status == None:
            logger.info("%s started sucessfully\n" % daemon_name)
        else:
            if print_error:
                logger.info("Cannot stop %s: status: %d, output: %s\n" %
                            (daemon_name, status, output))


def start_these_daemons(listDaemons, logger):
    for daemon in listDaemons:
        find_and_start_daemon(daemon, logger)


def stop_these_daemons(listDaemons, logger):
    for daemon in listDaemons:
        find_and_kill_daemon(daemon, logger)
        find_and_kill_process(daemon, logger)


def restart_these_daemons(listDaemons, logger):
    for daemon in listDaemons:
        status, output = execute_shell_cmd(
            "service %s stop 2>1 " % daemon, logger)
        find_and_kill_process(daemon, logger)
        old_nice = os.nice(0)
        pid = os.getpid()
        renice(0, pid, logger)
        status, output = execute_shell_cmd(
            "service %s start 2>1 " % daemon, logger)
        renice(old_nice, pid, logger)
        if status == None:
            logger.info("%s restarted sucessfully\n" % daemon)
        else:
            logger.info("Cannot restart %s: status: %d, output: %s\n" %
                        (daemon, status, output))


def signal_process_group_given_id(pgid, signal_id, logger):
    logger.debug("given args: signal_process_group_given_id:  pgid = %s signal_id = %s \n" % (
        str(pgid), str(signal_id)))
    if pgid == None:
        return
    try:
        os.killpg(pgid, signal_id)
        logger.debug("Process Group %d has been issued signal: %d successfully\n" % (
            signal_id, pgid))
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.info(
            "Unable to send signal: %d to the Process Group: %d \n" % (signal_id, pgid))


def chmod_dirs(root_dir, perm, logger):
    status = os.system("chmod -R %s %s" % (oct(perm), root_dir))
    if status:
        logger.error("chmod %s perm %s failed with status: %s" %
                     (root_dir, perm, status))


def create_and_chmod_files(files, perm, logger):
    for file in files:
        open(file, 'a').close()
        chmod_file(file, perm, logger)


def chmod_file(file, perm, logger):
    status = os.chmod(file, perm)
    if status:
        logger.error("chmod %s perm %s failed with status: %s" %
                     (file, perm, status))


def make_dir(dir_name, logger=None, perm=VTV_RESTRICTED_W_PERMISSION):
    if logger:
        logger.info("mkdir %s" % dir_name)

    try:
        os.makedirs(dir_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        if e[0] != errno.EEXIST:
            if logger:
                logger.error("Cannot mkdir %s: %s" %
                             (dir_name, get_compact_traceback(e)))
            return False

    try:
        os.chmod(dir_name, perm)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        if logger:
            logger.error("Cannot chmod %s: %s" %
                         (dir_name, get_compact_traceback(e)))
        return False

    return True


def make_dir_list(dir_list, logger):
    for dir_name in dir_list:
        if not make_dir(dir_name, logger):
            return False


def vtv_create_dirs(logger):
    return make_dir_list(VTV_DEFAULT_DIR_LIST, logger)


def copy_dir_files(src_dir, dest_dir, logger):
    cmd = '/bin/cp -rf %s/* %s' % (src_dir, dest_dir)
    status, output = execute_shell_cmd(cmd, logger)
    if status == None:
        logger.info("Copy of %s to %s sucessful\n" % (src_dir, dest_dir))
        return 0
    else:
        return 1


def copy_dir_files_based_on_pattern(src_dir, dest_dir, pattern, logger):
    cmd = '/bin/cp -rf %s/%s %s' % (src_dir, pattern, dest_dir)
    status, output = execute_shell_cmd(cmd, logger)
    if status == None:
        logger.info("Copy of %s/%s to %s sucessful\n" %
                    (src_dir, pattern, dest_dir))
        return 0
    else:
        return 1


def copy_dir(src_dir, dest_dir, logger):
    cmd = '/bin/cp -rf %s %s' % (src_dir, dest_dir)
    status, output = execute_shell_cmd(cmd, logger)
    if status == None:
        logger.info("Copy of %s to %s sucessful\n" % (src_dir, dest_dir))
        return 0
    else:
        return 1


def copy_file_list(file_list, dest, logger=None):
    for file_name in file_list:
        try:
            copy_file(file_name, dest, logger)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            if logger:
                logger.info("cannot copy file: %s" % file_name)


def copy_file(src, dest, logger=None):
    status = 0
    if os.path.exists(src):
        try:
            shutil.copy2(src, dest)
        except (KeyboardInterrupt, SystemExit):
            if logger:
                logger.error("Copy of %s to %s failed\n" % (src, dest))
            raise
        except:
            status = 1
    else:
        status = -1

    return status


def move_dir_files(src_dir, dest_dir, logger):
    try:
        cmd = '/bin/mv -f %s/* %s' % (src_dir, dest_dir)
        status, output = execute_shell_cmd(cmd, logger)
        if status == None:
            logger.info("Move of %s to %s sucessful\n" % (src_dir, dest_dir))
            return 0
        else:
            return 1
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("Move of dir files failed: %s" % (src_dir))


def move_dir_files_based_on_pattern(src_dir, dest_dir, pattern, logger):
    try:
        cmd = '/bin/mv -f %s/%s %s' % (src_dir, pattern, dest_dir)
        status, output = execute_shell_cmd(cmd, logger)
        if status == None:
            logger.info("Move of %s/%s to %s sucessful\n" %
                        (src_dir, pattern, dest_dir))
            return 0
        else:
            return 1
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("Move of dir files failed: %s" % (src_dir))


def move_file_list(file_list, dest, logger=None):
    for file_name in file_list:
        try:
            move_file(file_name, dest, logger)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            if logger:
                logger.info("cannot move file: %s" % file_name)


def move_file(src, dest, logger=None):
    if os.path.exists(src):
        try:
            shutil.move(src, dest)
        except (KeyboardInterrupt, SystemExit):
            if logger:
                logger.error("Move of %s to %s failed\n" % (src, dest))
            raise
        else:
            pass


def move_file_forcefully(src, dest, logger=None):
    if os.path.exists(src):
        try:
            if os.path.isfile(dest) or os.path.islink(dest):
                remove_file(dest, logger)
            else:
                filename = os.path.basename(src)
                filename = os.path.join(dest, filename)
                remove_file(filename, logger)
            shutil.move(src, dest)
        except (KeyboardInterrupt, SystemExit):
            if logger:
                logger.error("Move of %s to %s failed\n" % (src, dest))
            raise
        else:
            pass


def remove_file(file_name, logger):
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("cannot remove file: %s. Exception: %s" % (file_name, e))


def remove_file_list(file_list, logger):
    for file_name in file_list:
        try:
            remove_file(file_name, logger)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            logger.error("cannot remove file: %s. Exception: %s" %
                         (file_name, e))


def remove_dir(dir_name, logger):
    if not os.path.isdir(dir_name):
        return 1

    cmd = '/bin/rm -rf %s' % dir_name
    status, output = execute_shell_cmd(cmd, logger)
    if status == None:
        logger.info("removal of %s sucessful\n" % dir_name)
        return 0
    else:
        return 1


def remove_dir_files(dir_name, logger):
    cmd = '/bin/rm -rf %s/*' % dir_name
    status, output = execute_shell_cmd(cmd, logger)
    if status == None:
        logger.info("removal of %s sucessful\n" % dir_name)
        return 0
    else:
        return 1


def remove_dir_list(dir_list, logger):
    for dir_name in dir_list:
        try:
            remove_dir(dir_name, logger)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            logger.info("cannot remove dir: %s" % dir_name)


def remove_files_based_on_filter(src_dir, filter_pattern, max_files, logger, is_dir=False):
    absolute_filter_pattern = os.path.join(src_dir, filter_pattern)
    lmod_file_list = order_files_by_last_mod_time_desc(
        absolute_filter_pattern, latest_first=True)
    file_extension = '.%s' % VTV_PY_EXT
    file_list = [file_name for (
        lmod_time, file_name) in lmod_file_list if not file_name.endswith(file_extension)]
    files_to_remove = file_list[max_files:]
    remove_function_to_call = (is_dir and remove_dir_list) or remove_file_list
    if files_to_remove:
        logger.info('trying to remove: %s' % files_to_remove)
        remove_function_to_call(files_to_remove, logger)


def remove_files_based_on_filter_per_day(src_dir, filter_pattern, max_no_of_days, max_files, logger, is_dir=False):
    def get_date_str_from_file(filename):
        filename = os.path.basename(filename)
        return os.path.splitext(filename)[0].rsplit('_')[-1].split('T')[0]

    absolute_filter_pattern = os.path.join(src_dir, filter_pattern)
    lmod_file_list = order_files_by_last_mod_time_desc(
        absolute_filter_pattern, latest_first=True)
    file_extension = '.%s' % VTV_PY_EXT
    file_list = [file_name for (
        lmod_time, file_name) in lmod_file_list if not file_name.endswith(file_extension)]

    files_to_remove = []

    if file_list:
        cur_dt = get_date_str_from_file(file_list[0])
        date_count = per_day_tgz_count = 0
        for tgz_name in file_list:
            dt = get_date_str_from_file(tgz_name)
            if dt == cur_dt:
                per_day_tgz_count += 1
            else:
                cur_dt = dt
                per_day_tgz_count = 1
                date_count += 1

            if per_day_tgz_count > max_files or (date_count+1) > max_no_of_days:
                files_to_remove.append(tgz_name)

        remove_function_to_call = (
            is_dir and remove_dir_list) or remove_file_list
        if files_to_remove:
            logger.info('trying to remove: %s' % files_to_remove)
            remove_function_to_call(files_to_remove, logger)


def sort_file(file_name, sorted_file_name, logger):
    cmd = "export LC_ALL=C;sort %s > %s" % (file_name, sorted_file_name)
    status, output = execute_shell_cmd(cmd, logger)
    if status == None:
        if logger:
            logger.info("sorting file %s sucessful\n" % file_name)
        return 0
    else:
        return 1


def append_file(src, dst, logger):
    d = open(dst, 'ab')
    shutil.copyfileobj(open(src, 'rb'), d)
    d.close()


def cat_file_list(src_list, dst, logger):
    for src in src_list:
        append_file(src, dst, logger)


def delete_sym_link(link_name, logger):
    try:
        os.remove(link_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        pass

    try:
        os.unlink(link_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        pass


def create_sym_link(real_name, link_name, logger):
    try:
        os.remove(link_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        pass
    try:
        os.unlink(link_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        pass

    try:
        os.symlink(real_name, link_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("sym link of %s to %s failed\n" % (real_name, link_name))


def create_update_image_file(logger):
    create_timestamp_file(VTV_IMAGE_UPDATE_FILE_NAME, logger)


def create_restart_file(logger):
    create_timestamp_file(VTV_RESTART_FILE_NAME, logger)


def create_upgrade_file(logger, file_name=VTV_UPGRADE_FILE_NAME):
    create_timestamp_file(file_name, logger)


def create_timestamp_file(filename, logger):
    cwd = os.getcwd()
    os.chdir(VTV_STARTUP_DIR)
    try:
        image_file = open(filename, "w")
        image_file.write("%f" % time.time())
        image_file.close()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("exception in main: %s" % get_compact_traceback(e))
    os.chdir(cwd)


def reset_upgrade_file(logger, file_name=VTV_UPGRADE_FILE_NAME):
    if os.path.isfile(file_name):
        logger.info("Resetting upgrade file")
        os.remove(file_name)


def check_for_upgrade(logger, file_name=VTV_UPGRADE_FILE_NAME):
    upgrade_time = ''
    if os.path.isfile(file_name):
        logger.info("Upgrade in progress")
        upgrade_time = open(file_name).read()

    return upgrade_time


def check_for_restart(logger):
    restart_pending = False
    if os.path.isfile(VTV_RESTART_FILE_NAME):
        logger.info("Restart all in progress")
        restart_pending = True
        os.remove(VTV_RESTART_FILE_NAME)

    return restart_pending


def cat_vtv_log_files(dst_dir, src_dir):
    dst = os.path.join(dst_dir, VTV_LOG_FILE)
    src = os.path.join(src_dir, VTV_LOG_FILE)

    if not os.access(src, os.F_OK) or not os.access(dst, os.F_OK):
        return 1

    try:
        src_log_file = open(src, "r")
        text = src_log_file.read()
        src_log_file.close()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        return 2

    try:
        dst_log_file = open(dst, "a")
        dst_log_file.write(text)
        dst_log_file.close()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        return 3

    try:
        os.remove(src)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        return 4

    return 0


def get_log_dir_list(log_root_dir, logger):
    log_dir_list = []
    cur_dir = os.getcwd()
    os.chdir(log_root_dir)
    dir_list = glob.glob("[0-9]*.[0-9]*.[0-9]*.[0-9]*")
    os.chdir(cur_dir)
    for dir_name in dir_list:
        try:
            pathname = os.path.join(log_root_dir, dir_name)
            mode = os.stat(pathname)[stat.ST_MODE]
            if stat.S_ISDIR(mode):
                log_dir_list.append(pathname)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            logger.error("get log dir failed : %s " %
                         (get_compact_traceback(e)))

    return log_dir_list


def get_all_interface_list():
    cmd = '%s -a' % IFCONFIG_CMD
    status, text = execute_shell_cmd(cmd)
    if_regex = re.compile(r"(?P<interface>\S+)\s+Link encap:")

    if_list = []
    for tag_match in if_regex.finditer(text):
        if_list.append(tag_match.group('interface'))

    return if_list


def parse_mac_address(port_name, my_logger):
    cmd = '%s %s' % (IFCONFIG_CMD, port_name)
    status, text = execute_shell_cmd(cmd, my_logger)

    mac_regex = re.compile(r"\s+HWaddr\s+(?P<mac_address>\w+:\w+:\w+:\w+:\w+:\w+)")

    mac_address_list = []
    for tag_match in mac_regex.finditer(text):
        mac_address_list.append(tag_match.group('mac_address'))

    return mac_address_list.pop(0)


def get_ip_list(text):
    ip_regex = re.compile(r"\s+inet addr:(?P<ip_addr>\w+.\w+.\w+.\w+)")

    ip_addr_list = []
    for tag_match in ip_regex.finditer(text):
        ip_addr_list.append(tag_match.group('ip_addr'))

    return ip_addr_list


def zip_files(file_list, zip_file_name, logger):
    status = 0
    if not file_list:
        logger.warning('File list to zip is empty')
        status = 1
    else:
        zfobj = zipfile.ZipFile(zip_file_name, 'w')
        for file in file_list:
            zfobj.write(file)
        zfobj.close()
    return status


def unzip_file_into_dir(file, dir, create_dir=True):
    if create_dir:
        os.mkdir(dir, 0o777)
    zfobj = zipfile.ZipFile(file)
    for name in zfobj.namelist():
        if name.endswith('/'):
            os.mkdir(os.path.join(dir, name))
        else:
            outfile = open(os.path.join(dir, name), 'wb')
            outfile.write(zfobj.read(name))
            outfile.close()


def tar_zip_file(file_name, file_list, my_logger, is_tar=False):
    my_logger.info("Compressing specified files:%s to %s ..." %
                   (str(file_list), file_name))

    status = 0
    try:
        attribs = 'cvzf'
        if is_tar:
            attribs = 'cvf'
        status = os.system("tar %s %s %s &> /dev/null" %
                           (attribs, file_name, ' '.join(file_list)))
        if status:
            my_logger.error("Cannot tar %s : %s %s" %
                            (file_name, file_list, status))
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        my_logger.error("Cannot tar %s to %s : %s" %
                        (str(file_list), file_name, get_compact_traceback(e)))
        status = 1

    return status


def untar_zip_file(file_name, my_logger, file_to_extract="", is_tar=False):
    if file_to_extract:
        my_logger.info("extracting specified file:%s from %s ..." %
                       (file_to_extract, file_name))
    else:
        my_logger.info("Untarring %s in %s ..." % (file_name, os.getcwd()))

    try:
        attribs = 'xvfz'
        if is_tar:
            attribs = 'xvf'
        status = os.system("tar %s %s %s > /dev/null" %
                           (attribs, file_name, file_to_extract))
        if status:
            my_logger.error("Cannot untar %s : %s %s" %
                            (file_name, file_to_extract, status))
            return 1

    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        if file_to_extract:
            my_logger.info("File %s not found in %s : %s" % (
                file_to_extract, file_name, get_compact_traceback(e)))
        else:
            my_logger.error("Cannot untar %s : %s" %
                            (file_name, get_compact_traceback(e)))
        return 1

    return 0


def untar_zip_to_dir(file_name, dir_name, my_logger, is_tar=False):
    dir_name = os.path.abspath(dir_name)
    if not os.path.exists(dir_name):
        make_dir(dir_name, my_logger)
    cur_dir = os.getcwd()
    os.chdir(dir_name)
    status = untar_zip_file(file_name, my_logger, is_tar=is_tar)
    os.chdir(cur_dir)
    return status


def tar_dir_files(file_name, dir_name, my_logger):
    cur_dir = os.getcwd()
    os.chdir(dir_name)
    files = os.listdir('.')
    ret_code = tar_zip_file(file_name, files, my_logger)
    os.chdir(cur_dir)
    return ret_code


def remove_from_tarzip_file(file_name, my_logger, files_to_remove, retain=True):
    root_dir = os.getcwd()
    tmp_dir_name = 'tar_tmp'
    if os.path.exists(tmp_dir_name):
        remove_dir(tmp_dir_name, my_logger)
    os.makedirs(tmp_dir_name)
    os.chdir(tmp_dir_name)
    new_file_name = '%s/%s' % (root_dir, file_name)
    new_file_list = []
    try:
        tar = tarfile.open(new_file_name, "r:gz")
        for tarinfo in tar.getmembers():
            tar.extract(tarinfo.name)
            new_file_list.append(tarinfo.name)
        tar.close()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        my_logger.error("Cannot untar %s : %s" %
                        (new_file_name, get_compact_traceback(e)))
        return -1

    file_found = 0
    for file in files_to_remove:
        if file in new_file_list:
            new_file_list.remove(file)
            file_found = 1
            if retain:
                shutil.move(file, root_dir)

    if file_found == 1:
        newtar = tarfile.open(new_file_name, 'w:gz')
        name = "."
        newtar.add(name)
        newtar.close()

    os.chdir(root_dir)
    remove_dir(tmp_dir_name, my_logger)

    return file_found


def progrss_bar_init():
    global CLEAR_EOL
    try:
        import curses
    except ImportError:
        CLEAR_EOL = ''
    else:
        curses.setupterm()
        CLEAR_EOL = curses.tigetstr("el") or ''


def progress_bar_str(s):
    sys.stderr.write(s + CLEAR_EOL + "\r")
    sys.stderr.flush()


def progress_bar(start, count):
    progress_bar_str("Start: %s Time Taken: %s Count: %d" % (time.ctime(
        start), str(datetime.timedelta(0, time.time() - start)), count))


def get_md5_hexdigest_for_file(file_name, m=None, block_size=1024*1024):
    digest = None
    if not os.access(file_name, os.F_OK):
        return digest
    if not m:
        m = get_md5_hash()
    f = open(file_name, 'rb')
    while True:
        data = f.read(block_size)
        if not data:
            digest = m.hexdigest()
            break
        try:
            m.update(data)
        except:
            break
    f.close()
    return digest


def get_md5_hexdigest_for_files(files, logger=None):
    digest_files = []
    m = get_md5_hash()
    m_copy = m.copy()
    for file in files:
        if get_md5_hexdigest_for_file(file, m_copy):
            m = m_copy.copy()
            digest_files.append(file)
        else:
            m_copy = m.copy()
            if logger:
                logger.error('Error while computing the digest for %s' % file)
    return m.hexdigest(), digest_files


def generate_md5_digest(data):
    m = get_md5_hash()
    m.update(data)
    return m.digest()


def hexify(s):
    return ("%02x"*len(s)) % tuple(map(ord, s))


"""Python utility to print MD5 checksums of argument files."""


def get_file_to_md5_list(file_list):
    md5_list = []
    for file in file_list:
        m = get_md5_hash()
        if get_md5_hexdigest_for_file(file, m):
            val = hexify(m.digest())
        else:
            val = 'DigestError'
        md5_list.append((file, val))

    return md5_list


def create_digest_in_dir(dir_name, logger):
    digest_file_name = os.path.join(dir_name, DIGEST_FILE_NAME)
    if not os.access(digest_file_name, os.F_OK):
        get_the_digest_file_for_file_in_dir(dir_name, logger)


def get_the_digest_file_for_file_in_dir(dir_name, logger):
    digest_file_name = os.path.join(dir_name, DIGEST_FILE_NAME)
    # assumes only one tgz in archive,previous img/data dirs
    tgz_names = glob.glob('%s/*.tgz' % dir_name)
    if len(tgz_names) <= 0:
        if logger:
            logger.error('no tgzs found in dir: %s' % dir_name)
    else:
        get_the_digest(tgz_names[0], digest_file_name, logger)


def get_the_digest(file_name, digest_file, logger):
    if logger:
        logger.info("in get_the_finger_print: file_name: %s" % file_name)

    if not os.access(file_name, os.F_OK):
        if logger:
            logger.error('Cant access file: %s' % file_name)
        return 0

    if os.access(digest_file, os.F_OK):
        remove_file(digest_file, logger)

    digest = get_md5_hexdigest_for_file(file_name)
    if digest:
        file(digest_file, 'w').write(digest)
        if logger:
            logger.info('Finger print for :%s is :%s cur_dir: %s' %
                        (file_name, digest, os.getcwd()))
    else:
        if logger:
            logger.error('Error while computing the digest for %s in %s' %
                         (file_name, os.getcwd()))
        return 0
    return 1


def get_type_refcounts():
    obs = gc.get_objects()
    type2count = {}
    type2all = {}
    for o in obs:
        all = sys.getrefcount(o)
        t = type(o)
        if t in type2count:
            type2count[t] += 1
            type2all[t] += all
        else:
            type2count[t] = 1
            type2all[t] = all

    ct = [(type2count[t], type2all[t], t) for t in type2count.keys()]
    ct.sort()
    ct.reverse()

    return ct


class TrackRefs:
    """Object to track reference counts across test runs."""

    def __init__(self):
        self.type2count = {}
        self.type2all = {}

    def update(self):
        obs = gc.get_objects()
        type2count = {}
        type2all = {}
        for o in obs:
            all = sys.getrefcount(o)
            t = type(o)
            if t in type2count:
                type2count[t] += 1
                type2all[t] += all
            else:
                type2count[t] = 1
                type2all[t] = all

        ct = get_type_refcounts()
        ct.sort()
        ct.reverse()

        for type2count, type2all, t in ct:
            delta1 = type2count - self.type2count[t]
            delta2 = type2all - self.type2all[t]
            if delta1 or delta2:
                print("%-55s %8d %8d" % (t, delta1, delta2))

        self.type2count = type2count
        self.type2all = type2all


def get_refcounts():
    d = {}
    # collect all classes
    for m in list(sys.modules.values()):
        for sym in dir(m):
            o = getattr(m, sym)
            if type(o) is type:
                d[o] = sys.getrefcount(o)
    # sort by refcount
    pairs = [(x[1], x[0]) for x in list(d.items())]
    pairs.sort()
    pairs.reverse()
    return pairs


def print_gc_top_100_ref_counts(logger):
    logger.info("----------------------------->")
    for n, c in get_refcounts()[:100]:
        logger.info("Ref Count, Name: %10d %s" % (n, c.__name__))
    logger.info("<-----------------------------")


def vtv_pickle(file_name, object, logger):
    try:
        out_fh = open(file_name, "wb")
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("Cannot open file %s %s : %s" %
                     (file_name, str(e), get_compact_traceback(e)))
        return 1

    try:
        pickle.dump(object, out_fh)
    except pickle.PickleError:
        logger.error(
            "Object pickling failed - unable to write to file: %s" % file_name)

    out_fh.close()

    return 0


def vtv_unpickle(file_name, logger):
    try:
        unpickle_fh = open(file_name)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.debug("Unable to open file %s for unpickling" % file_name)
        return None

    object = None

    try:
        object = pickle.load(unpickle_fh)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("file '%s' unpickling failed" % file_name)

    unpickle_fh.close()

    return object


def get_python_process_name(proc_name):
    return "%s.%s" % (proc_name, VTV_PROCESS_EXT_LIST[VTV_PROCESS_PY])


def get_python_process_name_with_prefix(proc_name):
    if proc_name.startswith("/"):
        return "python %s.%s" % (proc_name, VTV_PROCESS_EXT_LIST[VTV_PROCESS_PY])
    return "python ./%s.%s" % (proc_name, VTV_PROCESS_EXT_LIST[VTV_PROCESS_PY])


def get_no_python_process_name(proc_name):
    parts = proc_name.split(".")
    if parts[-1] in ["py", "pyc", "pyo"]:
        return string.join(parts[0:-1], ".")
    return proc_name


def get_process_name_from_sysarg():
    return os.path.basename(sys.argv[0])


def get_crawler_name_with_prefix(config_name):
    return "crawler_%s" % config_name


def get_crawler_name_from_sysarg():
    my_name = get_process_name_from_sysarg()
    my_name = get_no_python_process_name(my_name)
    my_name = my_name.replace("crawler_", "")
    return my_name


def get_pid_file_for_process(process_name, pid_dir=VTV_PID_DIR):
    pid_file_name = os.path.join(pid_dir, "%s.pid" % process_name)
    return pid_file_name


def get_pid_file(pid_dir=VTV_PID_DIR):
    process_name = get_no_python_process_name(os.path.basename(sys.argv[0]))
    return get_pid_file_for_process(process_name, pid_dir)


def create_pid_file(pid_dir=VTV_PID_DIR):
    pid_file_name = get_pid_file(pid_dir)
    open(pid_file_name, "w").write("%s" % os.getpid())
    return pid_file_name


def get_pid_using_name(proc_name):
    cmd = "pgrep -f %s" % proc_name
    status, pid_list_str = execute_shell_cmd(cmd, None)
    if not pid_list_str:
        return []
    pid_list = pid_list_str.split('\n')
    pid_list = [x for x in pid_list if x]

    return pid_list


def get_pid_from_pid_file(pid_file, logger=None):
    try:
        f = open(pid_file)
        pid = int(f.read().replace('\n', ''))
        f.close()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        pid = -1
    return pid


def get_pid_for_process(process_name):
    pid_file_name = get_pid_file_for_process(process_name)
    return get_pid_from_pid_file(pid_file_name)


def get_vtv_root_dir(dir_name=""):
    return VTV_ROOT_DIR


def set_vtv_root_dir(root_dir):
    os.environ['VTV_ROOT_DIR'] = root_dir


def get_release_dir(dir_name=""):
    return VTV_RELEASE_DIR


def get_pid_dir(dir_name):
    release_dir = get_release_dir(dir_name)
    pid_dir = os.path.join(release_dir, "pids")
    if not pid_dir:
        return None
    return pid_dir


def get_child_pids(process_name, dir_name=""):
    pid_dir = get_pid_dir(dir_name)
    child_pid_file_name = os.path.join(pid_dir, "%s.child_pid" % process_name)
    if os.path.isfile(child_pid_file_name):
        try:
            pids = open(child_pid_file_name).read().replace(
                '\n', '').split(",")
            pids = [int(pid) for pid in pids]
        except:
            pids = []
    else:
        pids = []

    return pids


def create_pid_file_in_release_dir(process_name, dir_name=""):
    pid_dir = get_pid_dir(dir_name)
    if not pid_dir:
        return

    try:
        if os.access(pid_dir, os.F_OK):
            pid_file_name = os.path.join(pid_dir, "%s.pid" % process_name)
            open(pid_file_name, "w").write("%s" % os.getpid())
    except Exception as e:
        pass


def create_child_pid_file_in_release_dir(process_name, child_pids, dir_name=""):
    pid_dir = get_pid_dir(dir_name)
    try:
        if os.access(pid_dir, os.F_OK):
            child_pid_file_name = os.path.join(
                pid_dir, "%s.child_pid" % process_name)
            pids = [str(pid) for pid in child_pids]
            pids = ",".join(pids)
            open(child_pid_file_name, "w").write("%s" % pids)
    except Exception as e:
        pass


def start_vtv_process_wait(process, my_logger, start_dir):
    wait_opt = os.P_WAIT
    status = -1
    if my_logger:
        my_logger.info("Spawning process [%s] in foreground in %s" % (
            process, os.getcwd()))

    cwd = os.getcwd()
    os.chdir(start_dir)

    try:
        status = os.spawnv(wait_opt, "/bin/bash", ['bash', '-c', process])
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        if my_logger:
            my_logger.error("Unable to spwn process: %s : error: %s\n" % (
                process, get_compact_traceback(e)))

    os.chdir(cwd)

    return status


def start_vtv_process(process, my_logger, start_dir, wait_opt=os.P_NOWAIT):
    status = -1
    old_nice = os.nice(0)
    pid = os.getpid()
    renice(0, pid, my_logger)
    if wait_opt == os.P_NOWAIT:
        process = '%s &' % process
        if my_logger:
            my_logger.info("Spawning process [%s] in background in %s" % (
                process, os.path.abspath(os.getcwd())))

        cwd = os.getcwd()
        os.chdir(start_dir)
        status = os.system(process)
        os.chdir(cwd)
        if status and my_logger:
            my_logger.error(
                "spawning system process: %s failed with status: %s" % (process, status))
    else:
        status = start_vtv_process_wait(process, my_logger, start_dir)
    renice(old_nice, pid, my_logger)
    return status


def daemonize(module_globals):
    # do the UNIX double-fork magic, see Stevens' "Advanced
    # Programming in the UNIX Environment" for details (ISBN 0201563177)
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            os._exit(0)
    except OSError as e:
        module_globals.logger.error(
            "fork #1 failed: %s" % (get_compact_traceback(e)))
        sys.exit(1)

    pid_file_name = get_pid_file()

    # decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent, print eventual PID before
            os.system("echo %s > %s" % (pid, pid_file_name))
            os._exit(0)

        else:  # child
            if module_globals.set_pgid:
                os.setpgid(pid, 0)

    except OSError as e:
        module_globals.logger.error(
            "fork #2 failed: %s" % (get_compact_traceback(e)))
        sys.exit(1)

    # Redirect standard file descriptors.
    stdin = "/dev/null"
    stdout = "/dev/null"
    stderr = "/dev/null"
    si = file(stdin, 'r')
    so = file(stdout, 'a+')
    se = file(stderr, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())


def get_pid_given_pid_file_name(pid_file_name, logger):
    logger.debug("get_pid_given_pid_file_name: %s \n" % pid_file_name)
    try:
        i = 0
        pid = -1
        while i < CHECK_PID_EXISTS_TIME:
            pid = get_pid_from_pid_file(pid_file_name, logger)
            if pid == -1:
                time.sleep(1)
                i += 1
                continue
            break
        logger.debug("Got pid for file = %s and pid = %d\n" %
                     (pid_file_name, pid))
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("set pgid failed: %s" % (get_compact_traceback(e)))

    return pid


def get_string_checksum(obj):
    checksum = 0
    obj_len = len(obj)
    if obj_len % 2:
        checksum = ord(obj[-1]) << 8
        obj_len -= 1

    for i in range(0, obj_len, 2):
        checksum = checksum + ((ord(obj[i]) << 8) | ord(obj[i+1]))

    return checksum


def generate_vtvclid_command_file(server_ip, process_port_hash, set_or_delete):
    clid_cmd_file_name = VTV_VTVCLID_COMMAND_FILE_NAME+"_"+str(server_ip)
    if os.access(clid_cmd_file_name, os.F_OK):
        os.remove(clid_cmd_file_name)

    fp = open(clid_cmd_file_name, "w")

    if set_or_delete not in ["set", "delete"]:
        fp.write("server %s \n\
                  {\n    \
                       vtvdatad\n    \
                           {\n        \
                                %s D;\n    \
                           }\n\
                  }\n" % (server_ip, set_or_delete))
    else:
        for process in process_port_hash:
            fp.write("server %s\n" % server_ip)
            fp.write("{\n")

            if set_or_delete == "delete":
                fp.write("    %s D;\n" % process)
            elif set_or_delete == "set":
                fp.write("    %s\n" % process)
                fp.write("    {\n")
                fp.write("        server_port %s;\n" %
                         process_port_hash[process])
                fp.write("        no_core ;\n")
                fp.write("    }\n")

            fp.write("}\n")
    fp.close()
    return clid_cmd_file_name


def validate_clid_command(command, showid, logger):
    ex_command = '''%s/vtvclid -i %s -c "%s" 2>&1 > /dev/null ''' % (
        VTV_SERVER_DIR, showid, command)
    status = start_vtv_process(ex_command, logger, os.curdir)
    exit_status = os.WEXITSTATUS(status)
    return exit_status


def validate_clid_file_command(file_name, showid, logger):
    ex_command = '''%s/vtvclid -i %s -r %s &> /dev/null ''' % (
        VTV_SERVER_DIR, showid, file_name)
    status = start_vtv_process(ex_command, logger, os.curdir)
    exit_status = os.WEXITSTATUS(status)
    return exit_status


def get_the_various_extn_formats_for_given_extn(extn_str, logger):
    pattern = ""
    for char in extn_str:
        pattern += "[%s%s]" % (char.upper(), char.lower())
    return pattern


def get_the_file_in_given_dir(dir_name, extension, latest_flag, logger):
    oldest_file_name = ""
    total_file_cnt = 0

    cur_dir = os.getcwd()
    if os.access(dir_name, os.F_OK):
        os.chdir(dir_name)
        extension_pattern = get_the_various_extn_formats_for_given_extn(
            extension, logger)

        if latest_flag:
            rslt = os.system(
                "ls -t *.%s 2>/dev/null > temp_dir_files" % extension_pattern)
        else:
            rslt = os.system(
                "ls -rt *.%s 2>/dev/null > temp_dir_files" % extension_pattern)

        if rslt:  # non zero
            logger.error("%s extension files aren't presenet " % extension)
            os.chdir(cur_dir)
            return [oldest_file_name, total_file_cnt]

        total_file_cnt = len(open("temp_dir_files", "r").readlines())
        # strip new line at the end
        oldest_file_name = "%s/%s" % (dir_name,
                                      open("temp_dir_files", "r").readlines()[0][:-1])
        os.system("rm -f temp_dir_files")
    else:
        logger.error("directory %s doesnt exists" % dir_name)

    os.chdir(cur_dir)
    return [oldest_file_name, total_file_cnt]


def remove_oldest_files_in_given_dir(dir_name, extension, logger, retain_files_cnt=NO_OF_TEST_TGZ_TO_BE_RETAINED):
    total_file_cnt = 0

    cur_dir = os.getcwd()
    extension_pattern = get_the_various_extn_formats_for_given_extn(
        extension, logger)
    if os.access(dir_name, os.F_OK):
        os.chdir(dir_name)
        rslt = os.system(
            "ls -rt *.%s 2>/dev/null > temp_dir_files" % extension_pattern)
        if rslt:  # non zero
            logger.info("%s extension files aren't presenet " %
                        extension_pattern)
            os.chdir(cur_dir)
            return 0

        all_files = open("temp_dir_files", "r").readlines()
        total_file_cnt = len(all_files)
        if total_file_cnt > retain_files_cnt:  # more no.of old files
            for file in all_files[:(total_file_cnt-retain_files_cnt)]:
                logger.info('removing file: %s' % file)
                os.system('rm -f %s' % file)

        os.system("rm -f temp_dir_files")
    else:
        logger.error("directory %s doesnt exists" % dir_name)

    os.chdir(cur_dir)
    return 1


def get_datetime(ref_day):
    return datetime.datetime(int(ref_day[0:4]), int(ref_day[4:6]), int(ref_day[6:8]))


def get_latest_dirs(src_dir, end_day, start_day, interval=0):
    ''' returns latest dirs from start day to end day having interval days in between '''
    dirs = glob.glob('%s/[0-9]*' % (src_dir))
    current_day = end_day
    dirs.sort(reverse=True)
    valid_dirs = [os.path.join(src_dir, end_day)]

    if current_day and start_day:
        for d in dirs:
            dr = os.path.basename(d)
            if end_day < dr:
                continue
            if dr <= start_day:
                break
            diff_time = get_datetime(current_day) - get_datetime(dr)
            if diff_time.days > interval-1:
                valid_dirs.append(d)
                current_day = dr
    else:
        valid_dirs = dirs
    return valid_dirs


def order_files_by_last_mod_time_desc(pattern, latest_first=True):
    files = []
    for fl in glob.glob(pattern):
        stats = os.stat(fl)
        lmod = time.localtime(stats[8])
        data = (lmod, fl)
        files.append(data)
    files.sort()
    if latest_first:
        files.reverse()
    return files


def get_latest_file(file_pattern, logger):
    try:
        latest_file = ''
        files = order_files_by_last_mod_time_desc(file_pattern)
        if files:
            latest_file = files[0][1]
    except Exception as e:
        logger.error('Error in get_latest_file.Exception %s' % str(e))
    return latest_file


def get_iptable_commands(info):
    state_port_regex = re.compile(r".*?<state>(?P<state>\w+)</state>.*?<server_port>(?P<server_port>\w+)</server_port>.*?")

    port, state = (None, None)
    for tag_match in state_port_regex.finditer(info):
        port = tag_match.group('server_port')
        state = tag_match.group('state')

    if not (port and state):
        return []

    if_list = get_all_interface_list()
    commands = []
    for interface in if_list:
        if state == 'down':
            commands.append(DISABLE_IPTABLES_COMMAND % (interface, port))
        elif state == 'up':
            commands.append(ENABLE_IPTABLES_COMMAND % (interface, port))
    return commands


def get_test_stats_xml_str(server, process_name, vho_id, region_id, run_count, success_count, failure_count, error_count, test_status, time_stamp=""):
    if time_stamp == "":
        time_stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    xml_str = '<pr name=""><TS><VH>%s</VH><RG>%s</RG><TI>%s</TI><SN>%s</SN><PN>%s</PN><RC>%s</RC><SC>%s</SC><FC>%s</FC><EC>%s</EC><ST>%s</ST></TS></pr>' % (
        vho_id, region_id, time_stamp, server, process_name, run_count, success_count, failure_count, error_count, test_status)
    return xml_str


def get_alarm_xml_str(alarm_type, server, process_name, event_type, event_value, event_level, snmp_trap_type, time_stamp=""):
    if time_stamp == "":
        time_stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    xml_str = '<pr name=""><AL><SN>%s</SN><PN>%s</PN><ET>%s</ET><EV><![CDATA[%s]]></EV><TY>%s</TY><STY>%s</STY><TI>%s</TI><EL>%s</EL></AL></pr>' % (
        server, process_name, event_type, event_value.replace('\n', '<br>'), alarm_type, snmp_trap_type, time_stamp, event_level)

    return xml_str


def send_alarm_to_sysd(server, process_name, alarm_type, event_type, event_value, event_level, snmp_trap_type, sysd_ip, sysd_clid_port, send_func, logger):
    alarm_xml = get_alarm_xml_str(
        alarm_type, server, process_name, event_type, event_value, event_level, snmp_trap_type)
    logger.info("sending alarm:%s to sysd:%s" % (alarm_xml, sysd_ip))
    if send_func(alarm_xml, sysd_ip, sysd_clid_port, logger) == -1:
        logger.error("sending alarm msg to sysd failed!")
        return 1

    return 0


def get_param_from_file(file_name, param, default_value=''):
    if not os.access(file_name, os.F_OK):
        return default_value

    lines = open(file_name).readlines()
    for line in lines:
        if line[0] == '#' or line.find('=') == -1:
            continue
        name, value = line.split('=')
        value = value.split('\n')[0]
        if name == param:
            return value

    return default_value


def get_update_zip_dir_name(provider_name):
    provider_dir = os.path.join(VTV_CONTENT_DIR, provider_name)
    update_dir = os.path.join(provider_dir, CONTENT_UPDATE_ZIP_DIR_NAME)
    return update_dir


def get_correct_path_to_concepts_file(concept_file):
    print(os.getcwd())
    if os.path.exists(concept_file):
        return concept_file
    else:
        return os.path.join(VTV_CONFIG_DIR, concept_file)


MAX_DIGEST_CONSIDERED_GID_LEN = 12


def copy_concepts_header(input_concepts_file, output_concepts_file):
    first = True
    for line in open(input_concepts_file):
        if first:
            first = False
            if not line.startswith('@'):
                break
        output_concepts_file.write(line)
        if line.strip() == '@#@':
            break


def dumpConcVdbHeader(out_f, version):
    out_f.write("@version=%s\n" % version)
    out_f.write("@guidinfo=ENCODEDSTRING:{'CONC': '500000'}\n")
    CONC_FORMAT_FIELD_LIST = (('Gi', 'guid'), ('Ti', 'Title'), ('Ty', 'type'),
                              ('Rl', 'Rules'), ('De', 'Description'), ('Ke', 'Keywords'))
    # Dump all the field info
    for field, field_desc in CONC_FORMAT_FIELD_LIST:
        out_f.write("$%s:%s\n" % (field, field_desc))
    out_f.write('@#@\n')


def get_digest_attached_gid(gid):
    gid_str = gid[:MAX_DIGEST_CONSIDERED_GID_LEN].ljust(
        MAX_DIGEST_CONSIDERED_GID_LEN, '\0')
    gid_parts = struct.unpack('III', gid_str)

    shift_bits = 17
    X = 0
    for i in range(3):
        X = X ^ ((gid_parts[i] << shift_bits) | (
            gid_parts[(i+1) % 3] >> (32 - shift_bits)))

    X = X & (0x00000000ffffffff)
    aI = 2718281821
    mI = 0x7ffffffff
    digest = (aI * X) & mI

    return "%s_%lx" % (gid, digest)


def files_size_and_presence_check(file_list, min_file_size):
    missing_files = []
    empty_files = []
    for file_name in file_list:
        if not os.path.exists(file_name):
            missing_files.append(file_name)
        elif os.path.getsize(file_name) <= min_file_size:  # checks in byte
            empty_files.append(file_name)
    return missing_files, empty_files


def vtv_list_complement(list1=[], list2=[]):
    complement_list = []
    set_1 = set(list1)
    set_2 = set(list2)
    diff_set = (set_1 - set_2)
    for member in diff_set:
        complement_list.append(member)
    return complement_list


VTV_MAIL_ERROR_FORMAT_STR = """Could not delivery mail to: %s

    Server said: %s
    %s

    %s"""


def vtv_send_html_mail(logger, server, sender, receivers, subject, text, html_file, image_file, disposition="attachment"):
    import smtplib

    AUTHREQUIRED = 0  # if you need to use SMTP AUTH set to 1
    smtpuser = ''  # for SMTP AUTH, set SMTP username here
    smtppass = ''  # for SMTP AUTH, set SMTP password here

    """Create a mime-message that will render HTML in popular MUAs, text in better ones"""
    import MimeWriter
    import mimetools
    import io
    import base64

    out = io.StringIO()  # output buffer for our message

    writer = MimeWriter.MimeWriter(out)

    # set up some basic headers... we put subject here
    # because smtplib.sendmail expects it to be in the
    # message body
    writer.addheader("Subject", subject)
    writer.addheader("MIME-Version", "1.0")

    # start the multipart section of the message
    # multipart/alternative seems to work better
    # on some MUAs than multipart/mixed
    # writer.startmultipartbody("alternative")
    writer.startmultipartbody("mixed")
    writer.flushheaders()

    # the plain text section
    txtin = io.StringIO(text)
    subpart = writer.nextpart()
    subpart.addheader("Content-Transfer-Encoding", "quoted-printable")
    pout = subpart.startbody("text/plain", [("charset", 'us-ascii')])
    mimetools.encode(txtin, pout, 'quoted-printable')
    txtin.close()

    # start the html subpart of the message
    if html_file:
        htmlin = io.StringIO(open(html_file).read())
        subpart = writer.nextpart()
        subpart.addheader("Content-Transfer-Encoding", "quoted-printable")
        subpart.addheader("Content-Disposition", disposition)
        pout = subpart.startbody("text/html", [("charset", 'us-ascii')])
        mimetools.encode(htmlin, pout, 'quoted-printable')
        htmlin.close()

    if image_file:
        subpart = writer.nextpart()
        subpart.addheader("Content-Transfer-Encoding", "base64")
        pout = subpart.startbody("image/png")
        base64.encode(open(image_file, 'rb'), pout)

    writer.lastpart()
    msg = out.getvalue()
    out.close()

    session = smtplib.SMTP(server)
    if AUTHREQUIRED:
        session.login(smtpuser, smtppass)
    smtpresult = session.sendmail(sender, receivers, msg)

    if smtpresult:
        errstr = ""
        for recip in list(smtpresult.keys()):
            errstr = VTV_MAIL_ERROR_FORMAT_STR % (
                recip, smtpresult[recip][0], smtpresult[recip][1], errstr)
        str = "smtp error: %s" % errstr
        if logger:
            logger.error(str)
        #raise smtplib.SMTPException, errstr
        return


def vtv_send_html_mail_2(logger, server, sender, receivers, subject, text, html, image):
    # Send an HTML email with an embedded image and a plain text message for
    # email clients that don't want to display the HTML.

    # Create the root message and fill in the from, to, and subject headers
    msgRoot = MIMEMultipart('mixed')
    msgRoot['Subject'] = subject
    msgRoot['From'] = sender
    msgRoot['To'] = ', '.join(receivers)
    msgRoot.preamble = 'This is a multi-part message in MIME format.'

    # Encapsulate the plain and HTML versions of the message body in an
    # 'alternative' part, so message agents can decide which they want to display.
    msgAlternative = MIMEMultipart('alternative')
    msgRoot.attach(msgAlternative)

    msgText = MIMEText(text, 'plain', _charset='UTF-8')
    msgAlternative.attach(msgText)

    # We reference the image in the IMG SRC attribute by the ID we give it below
    msgText = MIMEText(html, 'html', _charset='UTF-8')
    msgAlternative.attach(msgText)

    # This example assumes the image is in the current directory
    if image:
        fp = open(image, 'rb')
        msgImage = MIMEImage(fp.read())
        fp.close()

        # Define the image's ID as referenced above
        msgImage.add_header('Content-ID', '<image1>')
        msgRoot.attach(msgImage)

    # Send the email (this example assumes SMTP authentication is required)
    import smtplib
    smtp = smtplib.SMTP()
    smtp.connect(server)
    #smtp.login('exampleuser', 'examplepass')
    smtp.sendmail(sender, receivers, msgRoot.as_string())
    smtp.quit()


def get_last_pull_stats():
    last_pull_stats_time = str(time.time() - 24 * 60 * 60)
    if os.path.exists(VTV_LAST_PULL_STATS_FILE):
        fd = open(VTV_LAST_PULL_STATS_FILE, 'r')
        data = fd.read()
        if data:
            last_pull_stats_time = data
        fd.close()
    return last_pull_stats_time


def update_last_pull_stats(last_stats_pulled):
    fd = open(VTV_LAST_PULL_STATS_FILE, 'w')
    fd.write(str(last_stats_pulled))
    fd.close()


def get_today_info():
    year = str(time.gmtime().tm_year)
    month = str(time.gmtime().tm_mon)
    day = str(time.gmtime().tm_mday)
    if len(month) < 2:
        month = '0'+month
    if len(day) < 2:
        day = '0'+day
    month = year+month
    day = month+day
    return year, month, day


def timer_function_for_self_destruct(timeout_in_seconds, logger):
    time.sleep(timeout_in_seconds)
    logger.info('ending at %s because of timeout' % datetime.datetime.now())
    os.kill(os.getpid(), signal.SIGINT)


def run_and_kill_after_timeout(classobj_or_function, timeout_in_seconds, logger, *params):
    # runs classobj.run() or func() with given parameters and kills the process after timeout
    t = Thread(target=timer_function_for_self_destruct,
               args=(timeout_in_seconds, logger))
    t.start()

    if isinstance(classobj_or_function, types.InstanceType):
        status = classobj_or_function.run(*params)
        return status
    if isinstance(classobj_or_function, types.FunctionType):
        status = classobj_or_function(*params)
        return status


def write_rc_local(logger, install_type, setup_username=''):
    logger.info("Setup rc.local to start startup daemon")

    rc_file_name = '/etc/rc.local'
    our_str = '# spawn System and Node Daemons'
    rc_str = RC_LOCAL_STRING % (VTV_ROOT_DIR, install_type)
    if setup_username and setup_username != 'root':
        rc_str = "su %s -c '%s'" % (setup_username, rc_str)

    try:
        text = open(rc_file_name).read()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error('Error: Reading file %s. Exception: %s' %
                     (rc_file_name, e))
        raise

    if our_str in text:
        text, our_str = text.split(our_str, 1)
    else:
        exit_str = 'exit 0'
        if exit_str in text:
            text = text.replace(exit_str, '')
    text = '%s\n%s' % (text, rc_str)

    try:
        open(rc_file_name, "w").write(text)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error('Error: Writing %s. Exception: %s' % (rc_file_name, e))
        raise


def write_vtv_startup_sh(logger, py_ext=VTV_PY_EXT):
    startup_sh_str = VTV_STARTUP_SH_STRING % (
        VTV_ROOT_DIR, VTV_ROOT_DIR, py_ext)
    try:
        open(VTV_STARTUP_SH_FILE_NAME, "w").write(startup_sh_str)
        os.chmod(VTV_STARTUP_SH_FILE_NAME, VTV_RESTRICTED_X_PERMISSION)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error('Error: Writing %s. Exception: %s' %
                     (VTV_STARTUP_SH_FILE_NAME, e))
        raise


def main():
    print(get_python_process_name("vtvnoded"))
    print(get_no_python_process_name("vtvnoded.py"))

    """
    logger = initialize_logger()
    print get_file_to_md5_list(sys.argv[1:])
    logger = initialize_logger()
    vtv_pickle("my.pickle", [1, {2:3, 4:5}], logger)
    print vtv_unpickle("my.pickle", logger)
    """

    """
    logger = initialize_logger()
    update_image_and_data_versions('imdb', logger)
    update_image_and_data_versions('wiki', logger)
    """

    return (0)


if __name__ == '__main__':
    main()
