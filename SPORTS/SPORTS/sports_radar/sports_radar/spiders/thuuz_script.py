import sys
import os
import datetime
import genericFileInterfaces
from vtv_utils import copy_file, make_dir_list, VTV_DATAGEN_CURRENT_DIR
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection
from data_schema import get_schema


schema = ['Gi', 'Sk', 'Ti', 'Ep', 'Vt', 'Ot', 'Ll', 'Du', 'Ge', 'De', 'Od', 'Tg']
schema = get_schema(schema)

GAME_DATE_LIST  =  []

class SportsThuuzScript(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.init_config(self.options.config_file)
        self.db_ip              = self.get_default_config_value('THUUZ_IP')
        self.db_name            = self.get_default_config_value('THUUZ_NAME')
        my_name                 = self.get_default_config_value('DATA_DIR')
        self.cursor, self.conn  = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = self.vtv_username,
                                                      passwd=self.vtv_password)
        self.OUT_DIR            = os.path.join(self.system_dirs.VTV_DATAGEN_CURRENT_DIR, my_name)
        make_dir_list([self.OUT_DIR], self.logger)
        self.programs_file      = open(os.path.join(self.OUT_DIR, "thuuz.data"), "w+")
        self.schedule_file      = open(os.path.join(self.OUT_DIR, "DATA_AVAILABILITY_FILE.thuuz"), "w+")

    def get_db_record(self):
        QRY = 'select  * from  sports_thuuz_games where Od > (curdate() - interval 14 day)'
        self.cursor.execute(QRY)
        tg = "Tg: LIVE"

        for data in self.get_fetchmany_results():
            Gi, Sk, Ti, Ep, Vt, Ot, Ll, Du, Ge, De, Od, Tg, ci, at, created_at, modified_at = data
            data_record = [Gi, Sk, Ti, Ep, Vt, Ot, Ll, Du, Ge, De, str(Od), Tg]
            genericFileInterfaces.writeSingleRecord(self.programs_file, data_record, schema, 'utf-8')
            record = "%s#<>#%s#<>#%s" % (ci, at, tg)
            self.schedule_file.write("%s\n" %record)
        self.schedule_file.close()
        self.programs_file.close()
        self.cursor.close()
        self.conn.close()

    def cleanup(self):
        self.move_logs(self.OUT_DIR, [ ('.', 'thuuz_script_*log')] )
        self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

    def set_options(self):
        config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, 'sports_thuuz.cfg')
        self.parser.add_option('-c', '--config-file', default=config_file, help='configuration file')

    def run_main(self):
        self.get_db_record()
        

if __name__ == '__main__':
    vtv_task_main(SportsThuuzScript)
    sys.exit(0)
