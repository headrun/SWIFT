import MySQLdb
import random
import optparse

class GettyImagesFlagUpdate():

    def __init__(self):

        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()


    def main(self):
	update_status='update sports_images_status set flag=9 where image_id in (select image_id from sports_getty_images_status where status=9)'
	self.cursor.execute(update_status) 
        self.conn.commit()
        up_al_flag = 'update sports_images_status set flag=0 where  image_id  in (select image_id from sports_getty_images_status where status=0)' 
        self.cursor.execute(up_al_flag)
        self.conn.commit()
        entity_ids_que = 'select distinct(entity_id) from sports_getty_images_status where status=1'
        self.cursor.execute(entity_ids_que)
        entity_ids = self.cursor.fetchall()
        for entity_id in entity_ids:
            entity_id = entity_id[0]
            if not entity_id:
                continue
            img_que = "select image_id from sports_getty_images_status where status = 1 and entity_id ='%s'"%(entity_id)
            self.cursor.execute(img_que)
            imgs = self.cursor.fetchall()
            imgs_data =[]
            for img_id in imgs:
                if not img_id :continue
                sport_que = "select image_type,weight from sports_images where id = '%s'"%(img_id)
                self.cursor.execute(sport_que)
                try:
                    img_type, weight = self.cursor.fetchone()
                    imgs_data.append((entity_id,img_id,img_type,weight))

                except:continue

            if imgs_data:
                weights = 0
                weights1 =0
                head_data,action_data = (),()
                img_id_ = ''
                for da in imgs_data:
                    if 'headshots'==da[2]:
                        if weights <= da[-1]:
                            head_data  = da
                            weights = da[-1]
                    else:
                        if weights1<=da[-1]:
                            weights1=da[-1]
                            action_data = da
                if head_data:
                    image_status = 'update sports_getty_images_status set status=1 where image_id="%s"'%(head_data[1])
		    flag_status = 'update sports_images_status set flag=1 where image_id="%s"'%(head_data[1])
                    self.cursor.execute(image_status)
                    self.cursor.execute(flag_status)
                    self.conn.commit()
                if action_data:
                    image_status1 = 'update sports_getty_images_status set status=1 where image_id="%s"'%(action_data[1])
                    flag_status1 = 'update sports_images_status set flag=1 where image_id="%s"'%(action_data[1])
                    self.cursor.execute(image_status1)
                    self.cursor.execute(flag_status1)
                    self.conn.commit()
                for da_ in imgs_data:
                    if head_data == da_ or action_data == da_:
                        continue
                    image_status = 'update sports_getty_images_status set status=0 where image_id="%s"'%(da_[1])
                    image_status1 = 'update sports_getty_images_status set status=0 where image_id="%s"'%(da_[1])
                    self.cursor.execute(image_status)
                    self.cursor.execute(image_status1)
                    self.conn.commit()
        self.conn.close() 

if __name__ == '__main__':
    GettyImagesFlagUpdate().main()

