PYXSL.2

<TOPDATA>
import urllib
import os,re,asyncore
import util.smeagoline.mylxml

def removeExtraSpace(str):
    str=str.strip()
    newstr=''
    flag=0
    for c in str:
            if c in [' ', '\t', '\n']:
                    if flag == 1:
                            continue
                    else:
                            flag=1
            else:
                    flag=0
            newstr+=c
    return newstr

def capitalize(str):
    str = str.replace("-"," ")
    newstr=[]
    str=removeExtraSpace(str).split(' ')
    for word in str:
            if word != '':
                    newstr.append(word[0].upper() + word[1:])
    return ' '.join(newstr)

def get_compact_traceback(e = ""):
    except_list = [asyncore.compact_traceback()]
    return "error: %s traceback: %s" % (str(e), str(except_list))

url = args.url 
file_name = args.store_dir+"top"
total_file_str = util.smeagoline.mylxml.etree.tostring(doc).encode('utf8','replace')
open(file_name+".html","w").write(total_file_str)
error_file = file_name+"error_top.html"
extract_num = re.compile('\d+')
try:
    nodes = doc.xpath('//div/a[contains(string(@href),"segment")]')
    #print "len(nodes)",len(nodes)
    #import pdb;pdb.set_trace()
    uniq_ids= {}
    for node in nodes:
       link =node.xpath("@href")[0]
       segment_name = link.split("/")[-1]
       if segment_name  in uniq_ids: continue
       img = node.xpath(".//img")[0].xpath("@src")[0]	 
       uniq_ids[segment_name]=1
       seg_dir = args.store_dir+segment_name
       if not os.access(seg_dir, os.F_OK):
       	 os.system('mkdir -p %s'%seg_dir)

       arguments = {'segment_name':segment_name, 'store_dir':seg_dir+"/", 'thumb_img':img ,'postproc_file':args.postproc_file}
       #print arguments
       expand(link, "details", arguments= arguments)
 
except Exception,e:
    open(error_file,"w").write("%s\n"%get_compact_traceback(e))

</TOPDATA>
