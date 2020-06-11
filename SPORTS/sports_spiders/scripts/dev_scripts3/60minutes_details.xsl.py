PYXSL.2

<DATA>
import urllib
import os,re,asyncore
import StringIO
import util.smeagoline.mylxml
import urllib2

def get_compact_traceback(e = ""):
    except_list = [asyncore.compact_traceback()]
    return "error: %s traceback: %s" % (str(e), str(except_list))

def getDateUsingAMFGateway(video_url):
    # Using amfgateway to get the date info
    final_format = ''
    try:
        resp_obj = urllib2.urlopen(video_url)
        redirect_url = resp_obj.url
        rn_id = re.findall('rn=([0-9]+)', redirect_url)
        cl_id = re.findall('cl=([0-9]+)', redirect_url)
        if rn_id and cl_id:
            rn_id = rn_id[0]
            cl_id = cl_id[0]
            # Create a new post file from the generic post file
            post_generic = open('post', 'r')
            post_generic_lines = post_generic.readlines()
            post_generic.close()
            post_new = open('post1', 'w')
            for line in post_generic_lines:
                line = line.replace('VEVEOID1', cl_id)
                line = line.replace('VEVEOID2', rn_id)
                post_new.write(line)
            post_new.close()

            os.system('wget --header "Content-Type: application/x-amf" --post-file post1 "http://cosmos.bcst.yahoo.com/ver/260.0/amfphp/gateway.php" -O amfgateway')
            
            outfile = open('amfgateway', 'r')
            outdata = outfile.read()
            date_str = re.findall('TIMESTAMP(.*)CLIP_LENGTH', outdata)
            if date_str:
                date_str = date_str[0].strip()
                date_str = date_str.replace(',', '')
                date_str = date_str.replace(':', ' ')
                date_str_parts = date_str.split()
                
                month = date_str_parts[1]
                date = date_str_parts[2]
                hour = date_str_parts[3]
                minute = date_str_parts[4]
                am_or_pm = date_str_parts[5]
                if am_or_pm == 'PM':
                    hour = unicode(int(hour) + 12)
                    
                final_format = miscutils.GetXSDateTime(day = date, month = month, hour = hour, minute = minute)
    except:
        pass

    return final_format


def removeJunkChars(inputstring):
    outputval = []
    #returnval = ''.join([x for x in inputstring if ord(x)<128])
    for x in inputstring:
        if ord(x)<256:
            outputval.append(x)
    temp =  ''.join(outputval)
    templen = len(temp)
    outputval = []
    for x in temp:
        if ord(x)<128:
            outputval.append(x)
    if templen != 0:
        if (float(len(outputval))*100/templen) >50.0 :
            return ''.join(outputval)
        else:
            return temp
    else:
        return temp

def put_in_unicode(str):
    if not isinstance(str,unicode):
       try:
          str = str.decode('utf-8','replace')
       except:
          str = ''
    return str

url = args.url 
file_name = args.store_dir+"/top"
total_file_str = util.smeagoline.mylxml.etree.tostring(doc).encode('utf8','replace')
open(file_name+".html","w").write(total_file_str)
error_file = args.store_dir+"/error_top.html"
item_id_regex = re.compile("\d+")
#try:
    
nodes = doc.xpath("//li[@class='dtk-item']")
#print "len(nodes)",len(nodes)
all_itemids={}
group_title = removeJunkChars(textify(doc.xpath("//h1")).strip())
try:
   comment_cnt = item_id_regex.findall(doc.xpath("//div[@class='pagingnav clrfix']")[0].xpath(".//a")[1].xpath("@href")[0])[0]
except:
   comment_cnt = 0
try:
   votes_str = textify(doc.xpath("//span[@id='numvotes']")[0])
   votes = item_id_regex.findall(votes_str)
   if len(votes)==0:
      votes = 0
   else:
      votes = votes[0]
except:
   votes = 0

try:
   rating = float(doc.xpath("//div[@id='avgrating']")[1].xpath(".//img")[0].xpath("@alt")[0].split(" stars")[0])
except:
   rating = 0.0

#try:
nodes = doc.xpath("//div[@class='dtk-carousel uppercarousel']")[0].xpath(".//li[@class='dtk-item']")

for node in nodes:
    video_url = "http"+node.xpath(".//h3")[0].xpath(".//a")[0].xpath("@href")[0].split("http")[1].split("',")[0]
    #print "video_url",video_url
    video_id = video_url.split("cl=")[1].split("&")[0]
    title = put_in_unicode(removeJunkChars(textify(node.xpath(".//h3")[0]).strip()))
    desc = removeJunkChars(textify(node.xpath(".//p")[0]).split("Watch Clip")[0])
    img = node.xpath(".//img")[0].xpath("@src")[0]
    all_itemids[video_id]=1
    record = generate_record()
    record.title = title
    record.source = "60minutes"
    record.important_keyword = ['60 Minutes'] #to corelate with cbsnews #12590
    record.sourcekey = video_id
    record.popularity_indicator={}
    record.popularity_indicator['comments']= comment_cnt
    record.popularity_indicator['rating']= rating
    record.popularity_indicator['votes']= votes
    record.date_of_creation = getDateUsingAMFGateway(video_url)
    dt_type = 'VeveoDate'
    record.dt_type = dt_type
    record.description = desc
    record.keywords = [args.segment_name, group_title]
    # Url
    record.urls = [] 
    url = video_url
    record.reference = [url]
    avail = DotAccessDict()
    avail.url = url
    avail.mimetype = "text/html"
    avail.bitrate = "Unknown"
    avail.size = "Unknown"
    
    record.urls.append(avail)
    record.images = [img, args.thumb_img]

    submit_data(record)
    #except:       
    #print 'no upper carousel for segemnt:',args.segment_name
    #pass

try:
    nodes = []
    try:
        nodes = doc.xpath("//div[@class='dtk-carousel midcarousel clrfix']")[0].xpath(".//li[@class='dtk-item']")
    except:   
        #print "no middle carouesl"
        pass
    try:
        nodes += doc.xpath("//div[@class='dtk-carousel lowestcarousel']")[0].xpath(".//li[@class='dtk-item']")
    except:    
        #print "no lower carouesl"
        pass

    for node in nodes:
        video_url = "http"+node.xpath(".//a")[0].xpath("@href")[0].split("http")[1].split("',")[0]
        #print "video_url",video_url
        video_id = video_url.split("cl=")[1].split("&")[0]
        title = removeJunkChars(textify(node).strip().split("Watch Clip")[0]).strip() 
        img = node.xpath(".//img")[0].xpath("@src")[0]
        all_itemids[video_id]=1
        record = generate_record()
        record.title = title
        record.source = "60minutes"
        record.important_keyword = ['60 Minutes'] #to corelate with cbsnews #12590
        record.sourcekey = video_id
        record.popularity_indicator={}
        record.popularity_indicator['comments']= comment_cnt
        record.popularity_indicator['rating']= rating
        record.popularity_indicator['votes']= votes
        
        record.date_of_creation = getDateUsingAMFGateway(video_url)
        dt_type = 'VeveoDate' 
        record.dt_type = dt_type
        record.keywords = [args.segment_name, group_title]
        # Url
        record.urls = [] 
        url = video_url
        record.reference = [url]
        avail = DotAccessDict()
        avail.url = url
        avail.mimetype = "text/html"
        avail.bitrate = "Unknown"
        avail.size = "Unknown"
        
        record.urls.append(avail)
        record.images = [args.thumb_img, img]

        submit_data(record)
except:
       #print "no middle or lower carousel"
       pass

vf = open(args.postproc_file,'a+')
for id in all_itemids:
  vf.write("%s\n"%id)
vf.close()  

#except Exception,e:
#   open(error_file,"w").write("%s\n"%get_compact_traceback(e))

</DATA>
